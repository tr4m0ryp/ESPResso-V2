"""Evaluation metrics for the WA1 water footprint model.

All metrics computed in original m3 world-eq scale after inverse transform.
Per-head (raw, processing, packaging) and total (sum of 3 heads).
Per-tier evaluation produces a 4x6 matrix (heads x tiers).

Reference: notes/water-model-implementation.md (D7).
"""

import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import torch
from torch.utils.data import DataLoader

from model.water_footprint.src.preprocessing.transforms import Log1pZScoreTransform

logger = logging.getLogger(__name__)

HEAD_NAMES = ["raw", "processing", "packaging"]


# ---------------------------------------------------------------------------
# Individual metric functions
# ---------------------------------------------------------------------------

def mae(pred: np.ndarray, true: np.ndarray) -> float:
    """Mean absolute error."""
    return float(np.mean(np.abs(pred - true)))


def mape(pred: np.ndarray, true: np.ndarray, epsilon: float = 1e-8) -> float:
    """Mean absolute percentage error (0-100 scale).

    Avoids division by zero via epsilon floor on denominator.
    """
    return float(np.mean(np.abs((pred - true) / (np.abs(true) + epsilon))) * 100.0)


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
    preds_z: Dict[str, np.ndarray],
    targets_z: Dict[str, np.ndarray],
    transform: Log1pZScoreTransform,
) -> Dict[str, float]:
    """Compute MAE, MAPE, R2 per head and total in original m3 scale.

    Args:
        preds_z: {"raw": array, "processing": array, "packaging": array}
                 in z-score space (model output).
        targets_z: Same structure, z-score space targets.
        transform: Fitted Log1pZScoreTransform for inverse.

    Returns:
        Dict with keys like raw_mae, raw_mape, raw_r2, ..., total_mae, etc.
        MAPE is omitted for packaging (values ~0.002 cause huge errors).
    """
    results: Dict[str, float] = {}
    total_pred = np.zeros_like(preds_z["raw"])
    total_true = np.zeros_like(targets_z["raw"])

    for head in HEAD_NAMES:
        pred_orig = transform.inverse(preds_z[head], head)
        true_orig = transform.inverse(targets_z[head], head)

        results[f"{head}_mae"] = mae(pred_orig, true_orig)
        results[f"{head}_r2"] = r2_score(pred_orig, true_orig)

        # Skip MAPE for packaging -- values near zero cause huge percentages
        if head != "packaging":
            results[f"{head}_mape"] = mape(pred_orig, true_orig)

        total_pred += pred_orig
        total_true += true_orig

    results["total_mae"] = mae(total_pred, total_true)
    results["total_mape"] = mape(total_pred, total_true)
    results["total_r2"] = r2_score(total_pred, total_true)

    return results


# ---------------------------------------------------------------------------
# Per-tier evaluation (4x6 matrix)
# ---------------------------------------------------------------------------

@torch.no_grad()
def per_tier_evaluation(
    model: torch.nn.Module,
    dataloader: DataLoader,
    transform: Log1pZScoreTransform,
    tiers: Optional[List[str]] = None,
    device: Optional[torch.device] = None,
) -> pd.DataFrame:
    """Evaluate model at each fixed tier, producing a 4x6 MAE matrix.

    For each tier A-F, runs the full dataloader with that tier forced,
    then computes MAE per head plus total. The resulting DataFrame has
    tiers as columns and metric rows [raw_mae, proc_mae, pkg_mae, total_mae].

    Args:
        model: WA1Model with forward(batch, tier=...) interface.
        dataloader: Validation or test DataLoader.
        transform: Fitted inverse transform.
        tiers: List of tier labels (default A-F).
        device: Torch device. Inferred from model if not given.

    Returns:
        pandas DataFrame (4 rows x len(tiers) columns) printed to logger.
    """
    if tiers is None:
        tiers = ["A", "B", "C", "D", "E", "F"]
    if device is None:
        device = next(model.parameters()).device

    model.eval()
    tier_results: Dict[str, Dict[str, float]] = {}

    for tier in tiers:
        all_preds = {h: [] for h in HEAD_NAMES}
        all_targets = {h: [] for h in HEAD_NAMES}

        for batch in dataloader:
            batch_dev = {k: v.to(device) if isinstance(v, torch.Tensor) else v
                         for k, v in batch.items()}
            out = model(batch_dev, tier=tier)
            pred_tensor = out["preds"].detach().cpu().numpy()  # [B, 3]
            for i, head in enumerate(HEAD_NAMES):
                all_preds[head].append(pred_tensor[:, i])
                target_key = f"wf_{head}"
                all_targets[head].append(
                    batch[target_key].cpu().numpy()
                )

        preds_z = {h: np.concatenate(all_preds[h]) for h in HEAD_NAMES}

        # Targets from dataloader are raw m3 values. Transform to z-score
        # so compute_metrics can inverse both consistently.
        targets_raw = {h: np.concatenate(all_targets[h]) for h in HEAD_NAMES}
        targets_z = {
            h: transform.transform(targets_raw[h], h) for h in HEAD_NAMES
        }

        metrics = compute_metrics(preds_z, targets_z, transform)
        tier_results[tier] = metrics

    # Build 4x6 DataFrame (MAE only for the matrix)
    row_labels = ["raw_mae", "proc_mae", "pkg_mae", "total_mae"]
    key_map = {
        "raw_mae": "raw_mae",
        "proc_mae": "processing_mae",
        "pkg_mae": "packaging_mae",
        "total_mae": "total_mae",
    }
    data = {}
    for tier in tiers:
        col = []
        for row_key in row_labels:
            col.append(tier_results[tier][key_map[row_key]])
        data[tier] = col

    df = pd.DataFrame(data, index=row_labels)
    logger.info("Per-tier MAE matrix (m3 world-eq):\n%s", df.to_string())
    return df

