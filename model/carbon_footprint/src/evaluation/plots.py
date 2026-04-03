"""Publication-quality plots for carbon footprint model evaluation.

Core plots: training curves, prediction scatter, residual analysis.
Additional plots (tier degradation, heatmap, loss groups) in plots_extra.py.

All figures use 300 DPI, colorblind-friendly palette, and font sizes
suitable for two-column research paper figures.
"""

from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import torch
from torch.utils.data import DataLoader

from model.carbon_footprint.src.preprocessing.transforms import (
    HEAD_NAMES,
    Log1pZScoreTransform,
)

HEAD_LABELS = {
    "raw_materials": "Raw Materials",
    "transport": "Transport",
    "processing": "Processing",
    "packaging": "Packaging",
}


def setup_style() -> None:
    """Set matplotlib/seaborn defaults for publication-quality figures."""
    sns.set_palette("colorblind")
    plt.rcParams.update({
        "font.size": 10,
        "axes.labelsize": 10,
        "axes.titlesize": 10,
        "xtick.labelsize": 8,
        "ytick.labelsize": 8,
        "legend.fontsize": 8,
        "figure.dpi": 300,
        "savefig.dpi": 300,
        "axes.grid": False,
        "figure.constrained_layout.use": True,
    })


def _save_and_show(fig: plt.Figure, save_path: Optional[str]) -> None:
    """Save figure if path given, then show and close."""
    if save_path is not None:
        fig.savefig(save_path, dpi=300, bbox_inches="tight")
    plt.show()
    plt.close(fig)


@torch.no_grad()
def _collect_predictions(
    model: torch.nn.Module,
    dataloader: DataLoader,
    transform: Log1pZScoreTransform,
    device: torch.device,
    tier: str = "F",
) -> tuple:
    """Run inference at given tier and collect predictions + actuals in kgCO2e.

    Returns:
        (preds, actuals) where each is {head_name: np.ndarray} in kgCO2e.
    """
    model.eval()
    preds: Dict[str, List[np.ndarray]] = {h: [] for h in HEAD_NAMES}
    actuals: Dict[str, List[np.ndarray]] = {h: [] for h in HEAD_NAMES}

    for batch in dataloader:
        batch_dev = {
            k: v.to(device) if isinstance(v, torch.Tensor) else v
            for k, v in batch.items()
        }
        out = model(batch_dev, tier=tier)
        pred_np = out["preds"].cpu().numpy()

        for i, head in enumerate(HEAD_NAMES):
            preds[head].append(transform.inverse(pred_np[:, i], head))
            actuals[head].append(
                transform.inverse(batch[f"cf_{head}"].numpy(), head)
            )

    preds_cat = {h: np.concatenate(v) for h, v in preds.items()}
    actuals_cat = {h: np.concatenate(v) for h, v in actuals.items()}
    return preds_cat, actuals_cat


def plot_training_curves(
    history: List[Dict],
    save_path: Optional[str] = None,
) -> None:
    """2x2 training diagnostics: loss, per-head loss, LR, distillation coeff.

    Args:
        history: List of dicts with keys: epoch, train_loss, val_loss,
                 L_raw, L_transport, L_processing, L_packaging, lr, distill_coeff.
    """
    setup_style()
    epochs = [h["epoch"] for h in history]
    fig, axes = plt.subplots(2, 2, figsize=(7, 5.5))

    # Top-left: train vs val loss
    ax = axes[0, 0]
    ax.plot(epochs, [h["train_loss"] for h in history], label="Train")
    ax.plot(epochs, [h["val_loss"] for h in history], label="Val")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Loss")
    ax.set_title("Train / Validation Loss")
    ax.legend()

    # Top-right: per-head losses (4 lines)
    ax = axes[0, 1]
    for head in HEAD_NAMES:
        vals = [h.get(f"L_{head}", 0.0) for h in history]
        ax.plot(epochs, vals, label=HEAD_LABELS[head])
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Head Loss")
    ax.set_title("Per-Head Losses")
    ax.legend(fontsize=7)

    # Bottom-left: learning rate schedule
    ax = axes[1, 0]
    ax.plot(epochs, [h["lr"] for h in history], color="tab:green")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Learning Rate")
    ax.set_title("LR Schedule")
    ax.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))

    # Bottom-right: distillation coefficient over epochs
    ax = axes[1, 1]
    distill = [h.get("distill_coeff", 0.0) for h in history]
    ax.plot(epochs, distill, color="tab:red")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Distillation Coefficient")
    ax.set_title("LUPI Distillation Schedule")

    _save_and_show(fig, save_path)


def plot_predictions(
    model: torch.nn.Module,
    dataloader: DataLoader,
    transform: Log1pZScoreTransform,
    device: torch.device,
    save_path: Optional[str] = None,
) -> None:
    """1x4 predicted-vs-actual scatter for each output head.

    Includes y=x reference line and R^2 annotation per subplot.
    """
    from model.carbon_footprint.src.evaluation.metrics import r2_score

    setup_style()
    preds, actuals = _collect_predictions(model, dataloader, transform, device)
    fig, axes = plt.subplots(1, 4, figsize=(13, 3.3))

    for ax, head in zip(axes, HEAD_NAMES):
        p, a = preds[head], actuals[head]
        ax.scatter(a, p, alpha=0.3, s=4, linewidths=0, rasterized=True)

        lo = min(a.min(), p.min())
        hi = max(a.max(), p.max())
        margin = (hi - lo) * 0.02
        ax.plot([lo - margin, hi + margin], [lo - margin, hi + margin],
                "k--", linewidth=0.8)

        r2 = r2_score(p, a)
        ax.text(0.05, 0.92, f"R$^2$ = {r2:.3f}",
                transform=ax.transAxes, fontsize=8,
                verticalalignment="top",
                bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.8))

        ax.set_xlabel("Actual (kgCO2e)")
        ax.set_ylabel("Predicted (kgCO2e)")
        ax.set_title(HEAD_LABELS[head])

    _save_and_show(fig, save_path)


def plot_residuals(
    model: torch.nn.Module,
    dataloader: DataLoader,
    transform: Log1pZScoreTransform,
    device: torch.device,
    save_path: Optional[str] = None,
) -> None:
    """2x4 residual analysis: scatter (top) and histogram (bottom) per head."""
    setup_style()
    preds, actuals = _collect_predictions(model, dataloader, transform, device)
    fig, axes = plt.subplots(2, 4, figsize=(13, 5.5))

    for col, head in enumerate(HEAD_NAMES):
        p, a = preds[head], actuals[head]
        residual = p - a

        # Top row: predicted vs residual
        ax = axes[0, col]
        ax.scatter(p, residual, alpha=0.3, s=4, linewidths=0, rasterized=True)
        ax.axhline(0, color="k", linewidth=0.8, linestyle="--")
        ax.set_xlabel("Predicted (kgCO2e)")
        ax.set_ylabel("Residual")
        ax.set_title(HEAD_LABELS[head])

        # Bottom row: error distribution
        ax = axes[1, col]
        ax.hist(residual, bins=80, edgecolor="none", alpha=0.85)
        mu, sigma = residual.mean(), residual.std()
        ax.axvline(mu, color="k", linewidth=0.8, linestyle="--")
        ax.text(0.95, 0.92,
                f"$\\mu$ = {mu:.4f}\n$\\sigma$ = {sigma:.4f}",
                transform=ax.transAxes, fontsize=7,
                verticalalignment="top", horizontalalignment="right",
                bbox=dict(boxstyle="round,pad=0.3", fc="white", alpha=0.8))
        ax.set_xlabel("Error (kgCO2e)")
        ax.set_ylabel("Count")

    _save_and_show(fig, save_path)
