"""Additional publication-quality plots for WA1 model evaluation.

Tier degradation analysis, heatmaps, and gate distribution visualizations.
Depends on plots.py for shared constants and style setup.
"""

from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
import torch
from torch.utils.data import DataLoader

from model.water_footprint.src.evaluation.plots import (
    HEAD_LABELS, HEAD_NAMES, _save_and_show,
)

# Row label to display name mapping for tier matrix
_ROW_DISPLAY = {
    "raw_mae": "Raw Material MAE",
    "proc_mae": "Processing MAE",
    "pkg_mae": "Packaging MAE",
    "total_mae": "Total MAE",
}


def plot_tier_degradation(
    tier_matrix: pd.DataFrame,
    save_path: Optional[str] = None,
) -> None:
    """Line plot of MAE degradation across tiers A-F per head.

    Args:
        tier_matrix: DataFrame from per_tier_evaluation (4 rows x 6 cols).
                     Index: [raw_mae, proc_mae, pkg_mae, total_mae].
                     Columns: tier labels (A-F).
    """
    fig, ax = plt.subplots(figsize=(5, 3.5))
    tiers = list(tier_matrix.columns)
    markers = ["o", "s", "^", "D"]
    row_keys = list(tier_matrix.index)

    for row_key, marker in zip(row_keys, markers):
        values = tier_matrix.loc[row_key].values
        label = _ROW_DISPLAY.get(row_key, row_key)
        ax.plot(tiers, values, marker=marker, markersize=5, label=label,
                linewidth=1.5)

    # Use log scale if values span more than one order of magnitude
    all_vals = tier_matrix.values.flatten()
    if all_vals.max() / max(all_vals.min(), 1e-12) > 10:
        ax.set_yscale("log")

    ax.set_xlabel("Tier")
    ax.set_ylabel("MAE (m$^3$ world-eq)")
    ax.set_title("Tier Degradation")
    ax.legend(fontsize=7, loc="best")

    _save_and_show(fig, save_path)


def plot_tier_heatmap(
    tier_matrix: pd.DataFrame,
    save_path: Optional[str] = None,
) -> None:
    """Annotated heatmap of MAE values across tiers and heads.

    Args:
        tier_matrix: Same format as plot_tier_degradation input.
    """
    display_index = [_ROW_DISPLAY.get(r, r) for r in tier_matrix.index]
    plot_df = tier_matrix.copy()
    plot_df.index = display_index

    fig, ax = plt.subplots(figsize=(6, 2.8))
    sns.heatmap(
        plot_df,
        annot=True,
        fmt=".4f",
        cmap="YlOrRd",
        linewidths=0.5,
        ax=ax,
        cbar_kws={"label": "MAE (m$^3$ world-eq)"},
    )
    ax.set_title("Per-Tier MAE Heatmap")
    ax.set_ylabel("")

    _save_and_show(fig, save_path)


@torch.no_grad()
def plot_gate_distributions(
    model: torch.nn.Module,
    dataloader: DataLoader,
    device: torch.device,
    tiers: Optional[List[str]] = None,
    save_path: Optional[str] = None,
) -> None:
    """Violin plots of gate values across tiers for material and step gates.

    Args:
        model: WA1Model instance.
        dataloader: Evaluation DataLoader.
        device: Torch device.
        tiers: Tier labels to evaluate (default A-F).
    """
    if tiers is None:
        tiers = ["A", "B", "C", "D", "E", "F"]

    model.eval()
    records: List[Dict] = []

    for tier in tiers:
        mat_gates: List[np.ndarray] = []
        step_gates: List[np.ndarray] = []

        for batch in dataloader:
            batch_dev = {
                k: v.to(device) if isinstance(v, torch.Tensor) else v
                for k, v in batch.items()
            }
            out = model(batch_dev, tier=tier)
            gv = out["gate_values"]
            mat_gates.append(gv["material"].cpu().numpy())
            step_gates.append(gv["step"].cpu().numpy())

        mat_all = np.concatenate(mat_gates)
        step_all = np.concatenate(step_gates)

        for val in mat_all:
            records.append({"tier": tier, "gate": "Material", "value": float(val)})
        for val in step_all:
            records.append({"tier": tier, "gate": "Step", "value": float(val)})

    import pandas as pd
    df = pd.DataFrame(records)

    fig, ax = plt.subplots(figsize=(6, 3.5))
    sns.violinplot(
        data=df, x="tier", y="value", hue="gate",
        split=True, inner="quart", linewidth=0.8, ax=ax,
    )
    ax.set_xlabel("Tier")
    ax.set_ylabel("Gate Value")
    ax.set_title("Cross-Attention Gate Distributions by Tier")
    ax.legend(title="Gate", fontsize=7, title_fontsize=8)

    _save_and_show(fig, save_path)
