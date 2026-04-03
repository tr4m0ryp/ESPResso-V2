"""Evaluation metrics for the carbon footprint model.

All metrics computed in original kgCO2e scale after inverse transform.
Per-head (raw_materials, transport, processing, packaging) and total
(sum of 4 heads). Per-tier evaluation produces a 5x6 matrix (heads x tiers).
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader

from model.carbon_footprint.src.preprocessing.transforms import (
    HEAD_NAMES,
    Log1pZScoreTransform,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual metric functions
# ---------------------------------------------------------------------------

def mae(pred: np.ndarray, true: np.ndarray) -> float:
    """Mean absolute error."""
    return float(np.mean(np.abs(pred - true)))


def mape(pred: np.ndarray, true: np.ndarray, eps: float = 1e-8) -> float:
    """Mean absolute percentage error (0-100 scale).

    Avoids division by zero via epsilon floor on denominator.
    """
    return float(np.mean(np.abs((pred - true) / (np.abs(true) + eps))) * 100.0)


def smape(pred: np.ndarray, true: np.ndarray, eps: float = 1e-8) -> float:
    """Symmetric mean absolute percentage error (0-200 scale).

    Less sensitive to near-zero denominators than MAPE.
    """
    numer = np.abs(pred - true)
    denom = (np.abs(pred) + np.abs(true)) / 2.0 + eps
    return float(np.mean(numer / denom) * 100.0)


def r2_score(pred: np.ndarray, true: np.ndarray) -> float:
    """Coefficient of determination (R-squared).

    Returns 1 - SS_res / SS_tot. If SS_tot is zero (constant target),
    returns 0.0 to avoid division by zero.
    """
    ss_res = np.sum((true - pred) ** 2)
    ss_tot = np.sum((true - np.mean(true)) ** 2)
    if ss_tot < 1e-12:
        return 0.0
    return float(1.0 - ss_res / ss_tot)


# ---------------------------------------------------------------------------
# Per-head + total metric computation
# ---------------------------------------------------------------------------

def compute_metrics(
    preds_z: np.ndarray,
    targets_z: np.ndarray,
    transform: Log1pZScoreTransform,
) -> Dict[str, float]:
    """Compute MAE, MAPE, SMAPE, R2 per head and total in original kgCO2e.

    Args:
        preds_z: Array [N, 4] in z-score space (model output).
                 Column order: raw_materials, transport, processing, packaging.
        targets_z: Array [N, 4] in z-score space.
        transform: Fitted Log1pZScoreTransform for inverse.

    Returns:
        Dict with keys like raw_materials_mae, raw_materials_r2, ...
        total_mae, total_mape, total_r2, total_smape.
        MAPE is omitted for packaging (near-constant, MAPE is misleading).
    """
    results: Dict[str, float] = {}
    total_pred = np.zeros(preds_z.shape[0], dtype=np.float64)
    total_true = np.zeros(targets_z.shape[0], dtype=np.float64)

    for i, head in enumerate(HEAD_NAMES):
        pred_orig = transform.inverse(preds_z[:, i], head)
        true_orig = transform.inverse(targets_z[:, i], head)

        results[f"{head}_mae"] = mae(pred_orig, true_orig)
        results[f"{head}_r2"] = r2_score(pred_orig, true_orig)
        results[f"{head}_smape"] = smape(pred_orig, true_orig)

        # Skip MAPE for packaging -- values near zero cause huge percentages
        if head != "packaging":
            results[f"{head}_mape"] = mape(pred_orig, true_orig)

        total_pred += pred_orig
        total_true += true_orig

    results["total_mae"] = mae(total_pred, total_true)
    results["total_mape"] = mape(total_pred, total_true)
    results["total_r2"] = r2_score(total_pred, total_true)
    results["total_smape"] = smape(total_pred, total_true)

    return results


# ---------------------------------------------------------------------------
# Per-tier evaluation (5x6 matrix)
# ---------------------------------------------------------------------------

@torch.no_grad()
def per_tier_evaluation(
    model: torch.nn.Module,
    dataloader: DataLoader,
    transform: Log1pZScoreTransform,
    tiers: Optional[List[str]] = None,
    device: Optional[torch.device] = None,
) -> pd.DataFrame:
    """Evaluate model at each fixed tier, producing a 5x6 MAE matrix.

    For each tier A-F, runs the full dataloader with that tier forced,
    then computes MAE per head plus total. The resulting DataFrame has
    tiers as columns and metric rows.

    Args:
        model: CarbonModel with forward(batch, tier=...) interface.
        dataloader: Validation or test DataLoader.
        transform: Fitted inverse transform.
        tiers: List of tier labels (default A-F).
        device: Torch device. Inferred from model if not given.

    Returns:
        pandas DataFrame (5 rows x len(tiers) columns) logged to logger.
    """
    if tiers is None:
        tiers = ["A", "B", "C", "D", "E", "F"]
    if device is None:
        device = next(model.parameters()).device

    model.eval()
    tier_results: Dict[str, Dict[str, float]] = {}

    for tier in tiers:
        all_preds: List[np.ndarray] = []
        all_targets: List[np.ndarray] = []

        for batch in dataloader:
            batch_dev = {
                k: v.to(device) if isinstance(v, torch.Tensor) else v
                for k, v in batch.items()
            }
            out = model(batch_dev, tier=tier)
            all_preds.append(out["preds"].detach().cpu().numpy())

            # Transform raw targets to z-score space (same as val_epoch)
            target_cols = []
            for h in HEAD_NAMES:
                raw = batch[f"cf_{h}"].cpu().numpy()
                target_cols.append(transform.transform(raw, h))
            all_targets.append(np.column_stack(target_cols))

        preds_z = np.concatenate(all_preds, axis=0)
        targets_z = np.concatenate(all_targets, axis=0)
        tier_results[tier] = compute_metrics(preds_z, targets_z, transform)

    # Build 5x6 DataFrame (MAE only for the matrix)
    row_labels = [f"{h}_mae" for h in HEAD_NAMES] + ["total_mae"]
    display_labels = [
        "Raw Materials MAE",
        "Transport MAE",
        "Processing MAE",
        "Packaging MAE",
        "Total MAE",
    ]
    data = {}
    for tier in tiers:
        data[tier] = [tier_results[tier][k] for k in row_labels]

    df = pd.DataFrame(data, index=display_labels)
    logger.info("Per-tier MAE matrix (kgCO2e):\n%s", df.to_string())
    return df
