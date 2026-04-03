"""Publication-quality plots for WA1 water footprint model evaluation.

Core plots: training curves, prediction scatter, residual analysis.
Additional plots (tier degradation, heatmap, gates) in plots_extra.py.

All figures use 300 DPI, colorblind-friendly palette, and font sizes
suitable for two-column research paper figures.
"""

from typing import Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import torch
from torch.utils.data import DataLoader

from model.water_footprint.src.preprocessing.transforms import Log1pZScoreTransform

HEAD_NAMES = ["raw", "processing", "packaging"]
HEAD_LABELS = {"raw": "Raw Material", "processing": "Processing",
               "packaging": "Packaging"}


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
) -> tuple:
    """Run inference at tier F and collect predictions + actuals in m3.

    Returns:
        (preds, actuals) where each is {head_name: np.ndarray}.
    """
    model.eval()
    preds = {h: [] for h in HEAD_NAMES}
    actuals = {h: [] for h in HEAD_NAMES}

    for batch in dataloader:
        batch_dev = {
            k: v.to(device) if isinstance(v, torch.Tensor) else v
            for k, v in batch.items()
        }
        out = model(batch_dev, tier="F")
        pred_np = out["preds"].cpu().numpy()

        for i, head in enumerate(HEAD_NAMES):
            preds[head].append(transform.inverse(pred_np[:, i], head))
            actuals[head].append(batch[f"wf_{head}"].numpy())

    preds = {h: np.concatenate(v) for h, v in preds.items()}
    actuals = {h: np.concatenate(v) for h, v in actuals.items()}
    return preds, actuals


def plot_training_curves(
    history: List[Dict],
    save_path: Optional[str] = None,
) -> None:
    """2x2 training diagnostics: loss, per-head loss, LR, aux weight.

    Args:
        history: List of dicts with keys epoch, train_loss, val_loss,
                 head_losses (dict with raw/processing/packaging/aux_weight),
                 lr.
    """
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

    # Top-right: per-head losses
    ax = axes[0, 1]
    for head in HEAD_NAMES:
        vals = [h.get("head_losses", {}).get(head, 0.0) for h in history]
        ax.plot(epochs, vals, label=HEAD_LABELS[head])
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Head Loss")
    ax.set_title("Per-Head Losses")
    ax.legend()

    # Bottom-left: learning rate
    ax = axes[1, 0]
    ax.plot(epochs, [h["lr"] for h in history], color="tab:green")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Learning Rate")
    ax.set_title("LR Schedule")
    ax.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))

    # Bottom-right: auxiliary weight loss
    ax = axes[1, 1]
    aux = [h.get("head_losses", {}).get("aux_weight", 0.0) for h in history]
    ax.plot(epochs, aux, color="tab:red")
    ax.set_xlabel("Epoch")
    ax.set_ylabel("Aux Weight Loss")
    ax.set_title("Auxiliary Weight Head")

    _save_and_show(fig, save_path)


def plot_predictions(
    model: torch.nn.Module,
    dataloader: DataLoader,
    transform: Log1pZScoreTransform,
    device: torch.device,
    save_path: Optional[str] = None,
) -> None:
    """1x3 predicted-vs-actual scatter for each output head.

    Includes y=x reference line and R^2 annotation per subplot.
    """
    from model.water_footprint.src.evaluation.metrics import r2_score

    preds, actuals = _collect_predictions(model, dataloader, transform, device)
    fig, axes = plt.subplots(1, 3, figsize=(10, 3.3))

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

        ax.set_xlabel("Actual (m$^3$ world-eq)")
        ax.set_ylabel("Predicted (m$^3$ world-eq)")
        ax.set_title(HEAD_LABELS[head])

    _save_and_show(fig, save_path)


def plot_residuals(
    model: torch.nn.Module,
    dataloader: DataLoader,
    transform: Log1pZScoreTransform,
    device: torch.device,
    save_path: Optional[str] = None,
) -> None:
    """2x3 residual analysis: scatter (top) and histogram (bottom) per head."""
    preds, actuals = _collect_predictions(model, dataloader, transform, device)
    fig, axes = plt.subplots(2, 3, figsize=(10, 5.5))

    for col, head in enumerate(HEAD_NAMES):
        p, a = preds[head], actuals[head]
        residual = p - a

        # Top row: predicted vs residual
        ax = axes[0, col]
        ax.scatter(p, residual, alpha=0.3, s=4, linewidths=0, rasterized=True)
        ax.axhline(0, color="k", linewidth=0.8, linestyle="--")
        ax.set_xlabel("Predicted (m$^3$ world-eq)")
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
        ax.set_xlabel("Error (m$^3$ world-eq)")
        ax.set_ylabel("Count")

    _save_and_show(fig, save_path)
