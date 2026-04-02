"""Additional publication-quality plots for carbon footprint model evaluation.

Tier degradation analysis, heatmaps, and three-group loss visualization.
Depends on plots.py for shared constants and style setup.
"""

from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

from model.carbon_footprint.src.evaluation.plots import (
    _save_and_show,
    setup_style,
)

# Row label to display name mapping for tier matrix
_ROW_DISPLAY = {
    "Raw Materials MAE": "Raw Materials",
    "Transport MAE": "Transport",
    "Processing MAE": "Processing",
    "Packaging MAE": "Packaging",
    "Total MAE": "Total",
}


def plot_tier_degradation(
    tier_matrix: pd.DataFrame,
    save_path: Optional[str] = None,
) -> None:
    """Line plot of MAE degradation across tiers A-F per head.

    Args:
        tier_matrix: DataFrame from per_tier_evaluation (5 rows x 6 cols).
                     Index: display labels from per_tier_evaluation.
                     Columns: tier labels (A-F).
    """
    setup_style()
    fig, ax = plt.subplots(figsize=(5, 3.5))
    tiers = list(tier_matrix.columns)
    markers = ["o", "s", "^", "D", "v"]
    row_keys = list(tier_matrix.index)

    for row_key, marker in zip(row_keys, markers):
        values = tier_matrix.loc[row_key].values
        label = _ROW_DISPLAY.get(row_key, row_key)
        ax.plot(tiers, values, marker=marker, markersize=5, label=label,
                linewidth=1.5)

    # Use log scale if values span more than one order of magnitude
    all_vals = tier_matrix.values.flatten()
    val_range = all_vals.max() / max(all_vals.min(), 1e-12)
    if val_range > 10:
        ax.set_yscale("log")

    ax.set_xlabel("Tier")
    ax.set_ylabel("MAE (kgCO2e)")
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
    setup_style()
    display_index = [_ROW_DISPLAY.get(r, r) for r in tier_matrix.index]
    plot_df = tier_matrix.copy()
    plot_df.index = display_index

    fig, ax = plt.subplots(figsize=(6, 3.2))
    sns.heatmap(
        plot_df,
        annot=True,
        fmt=".4f",
        cmap="YlOrRd",
        linewidths=0.5,
        ax=ax,
        cbar_kws={"label": "MAE (kgCO2e)"},
    )
    ax.set_title("Per-Tier MAE Heatmap")
    ax.set_ylabel("")

    _save_and_show(fig, save_path)


def plot_loss_groups(
    history: List[Dict],
    save_path: Optional[str] = None,
) -> None:
    """3-panel visualization of three-group loss dynamics over epochs.

    Shows main_loss, aux_loss, and structural_loss (distillation + diversity)
    as separate subplots, making it easy to diagnose which loss group
    drives training behavior.

    Args:
        history: List of dicts with keys: epoch, loss_dict containing
                 main_loss, aux_loss, structural_loss.
    """
    setup_style()
    epochs = [h["epoch"] for h in history]
    fig, axes = plt.subplots(1, 3, figsize=(11, 3.3))

    # Panel 1: main head losses
    ax = axes[0]
    main = [h.get("loss_dict", {}).get("main_loss", h.get("main_loss", 0.0))
            for h in history]
    ax.plot(epochs, main, color="tab:blue", linewidth=1.5)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("Main Loss (4 heads)")

    # Panel 2: auxiliary losses (distance, mode, weight)
    ax = axes[1]
    aux = [h.get("loss_dict", {}).get("aux_loss", h.get("aux_loss", 0.0))
           for h in history]
    ax.plot(epochs, aux, color="tab:orange", linewidth=1.5)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("Auxiliary Loss")

    # Panel 3: structural losses (distillation + diversity)
    ax = axes[2]
    struct = [
        h.get("loss_dict", {}).get(
            "structural_loss", h.get("structural_loss", 0.0)
        )
        for h in history
    ]
    ax.plot(epochs, struct, color="tab:purple", linewidth=1.5)
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("Structural Loss (Distill + Div)")

    _save_and_show(fig, save_path)
