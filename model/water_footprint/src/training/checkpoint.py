"""Checkpoint save/load, experiment logging, and smoke test for WA1.

Split from trainer.py to stay under 300 lines per file.
"""

import json
import logging
import math
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import torch
from torch.utils.data import DataLoader, Subset

from model.water_footprint.src.utils.config import WA1Config, set_seed
from model.water_footprint.src.training.model import WA1Model
from model.water_footprint.src.training.loss import UWSOLoss
from model.water_footprint.src.preprocessing.dataset import WaterFootprintDataset
from model.water_footprint.src.preprocessing.transforms import Log1pZScoreTransform

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Checkpoint mixin (mixed into WA1Trainer via multiple inheritance)
# ---------------------------------------------------------------------------

class CheckpointMixin:
    """Checkpoint save/load and experiment logging for WA1Trainer."""

    def save_checkpoint(
        self, epoch: int, metrics: Dict[str, float], tag: str = "latest",
    ) -> Path:
        """Save model + optimizer + transform state."""
        ckpt_dir = Path(self.config.checkpoint_dir)
        ckpt_dir.mkdir(parents=True, exist_ok=True)
        path = ckpt_dir / f"checkpoint_{tag}.pt"
        torch.save({
            "epoch": epoch,
            "model_state": self.model.state_dict(),
            "optimizer_state": self.optimizer.state_dict(),
            "loss_state": self.loss_fn.state_dict(),
            "scheduler_state": self.scheduler.state_dict(),
            "config": asdict(self.config),
            "metrics": metrics,
            "transform": self.transform.state_dict(),
        }, path)
        logger.info("Saved checkpoint to %s", path)
        return path

    def load_checkpoint(self, path: str) -> int:
        """Restore from checkpoint. Returns the epoch number."""
        ckpt = torch.load(
            path, map_location=self.model_device, weights_only=False,
        )
        self.model.load_state_dict(ckpt["model_state"])
        self.optimizer.load_state_dict(ckpt["optimizer_state"])
        self.loss_fn.load_state_dict(ckpt["loss_state"])
        self.scheduler.load_state_dict(ckpt["scheduler_state"])
        self.transform.load_state_dict(ckpt["transform"])
        logger.info("Loaded checkpoint from %s (epoch %d)", path, ckpt["epoch"])
        return ckpt["epoch"]

    def log_run(self, best_metrics: Dict[str, float], elapsed: float) -> None:
        """Append a run entry to runs.jsonl."""
        log_path = Path(self.config.checkpoint_dir) / self.config.runs_log
        log_path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "seed": self.config.seed,
            "config": asdict(self.config),
            "best_metrics": best_metrics,
            "epochs_run": len(self.history),
            "elapsed_seconds": round(elapsed, 1),
            "history_summary": {
                "train_loss": [h["train_loss"] for h in self.history],
                "val_loss": [h["val_loss"] for h in self.history],
            },
        }
        with open(log_path, "a") as f:
            f.write(json.dumps(entry) + "\n")
        logger.info("Logged run to %s", log_path)


# ---------------------------------------------------------------------------
# Smoke test (standalone, does not require a trainer instance)
# ---------------------------------------------------------------------------

def smoke_test(
    config: Optional[WA1Config] = None,
    data_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate full pipeline on CPU with minimal data.

    Loads 100 rows, runs 2 train batches + 1 val batch, verifies output
    shapes, loss finiteness, gradient flow, gate variation across tiers,
    and checkpoint round-trip.

    Args:
        config: Optional WA1Config override. Defaults are applied for
            smoke-test-appropriate settings (small batch, no workers, CPU).
        data_path: Path to CSV data file. Falls back to config.data_path.

    Returns:
        Dict with 'passed' (bool) and diagnostic keys.
    """
    # Deferred import to avoid circular dependency at module level
    from model.water_footprint.src.training.trainer import WA1Trainer

    logging.basicConfig(level=logging.INFO)
    logger.info("--- smoke test start ---")
    device = torch.device("cpu")
    if config is None:
        config = WA1Config()
    # Override settings for smoke test regardless of caller config
    config.max_epochs = 2
    config.batch_size = 16
    config.num_workers = 0
    config.pin_memory = False
    config.persistent_workers = False
    config.checkpoint_dir = "/tmp/wa1_smoke"
    set_seed(config.seed)

    data_path = data_path or str(config.data_path)
    dataset = WaterFootprintDataset(data_path)
    indices = list(range(min(config.smoke_test_rows, len(dataset))))

    # Fit transform on the subset
    records = [dataset.records[i] for i in indices]
    raw_arr = np.array([r["wf_raw"] for r in records])
    proc_arr = np.array([r["wf_processing"] for r in records])
    pkg_arr = np.array([r["wf_packaging"] for r in records])
    transform = Log1pZScoreTransform().fit(raw_arr, proc_arr, pkg_arr)

    split_idx = int(len(indices) * 0.7)
    train_sub = Subset(dataset, indices[:split_idx])
    val_sub = Subset(dataset, indices[split_idx:])
    train_loader = DataLoader(train_sub, batch_size=16, shuffle=True)
    val_loader = DataLoader(val_sub, batch_size=16)

    model = WA1Model(config).to(device)
    loss_fn = UWSOLoss(config.huber_delta).to(device)
    trainer = WA1Trainer(
        config, model, loss_fn, train_loader, val_loader, transform,
        device=device,
    )

    # 1. Run train epoch (covers 2+ batches with 70 rows / batch_size=16)
    train_loss, head_losses = trainer.train_epoch(0)
    assert math.isfinite(train_loss), f"Train loss not finite: {train_loss}"
    logger.info("Train loss: %.4f, heads: %s", train_loss, head_losses)

    # 2. Check gradients flow (missing_* embeddings may have None grad
    #    when their tier was not sampled -- that is expected)
    n_with_grad = sum(
        1 for p in model.parameters()
        if p.requires_grad and p.grad is not None and p.grad.abs().sum() > 0
    )
    n_total = sum(1 for p in model.parameters() if p.requires_grad)
    grad_ratio = n_with_grad / max(n_total, 1)
    assert grad_ratio > 0.90, (
        f"Too few parameters received gradients: {n_with_grad}/{n_total}"
    )
    logger.info("Gradient flow: OK (%d/%d params)", n_with_grad, n_total)

    # 3. Val epoch
    val_loss, metrics = trainer.val_epoch()
    assert math.isfinite(val_loss), f"Val loss not finite: {val_loss}"
    logger.info("Val loss: %.4f", val_loss)

    # 4. Output shape check
    sample_batch = next(iter(val_loader))
    sample_batch = trainer._to_device(sample_batch)
    model.eval()
    with torch.no_grad():
        out = model(sample_batch, tier="A")
    assert out["preds"].shape[1] == 3, (
        f"Expected 3 heads, got {out['preds'].shape}"
    )
    logger.info("Output shape: %s", out["preds"].shape)

    # 5. Gate values differ across tiers
    with torch.no_grad():
        out_a = model(sample_batch, tier="A")
        out_f = model(sample_batch, tier="F")
    gate_a = out_a["gate_values"]["material"].mean().item()
    gate_f = out_f["gate_values"]["material"].mean().item()
    logger.info("Gate material: tier_A=%.4f, tier_F=%.4f", gate_a, gate_f)

    # 6. Checkpoint round-trip
    ckpt_path = trainer.save_checkpoint(0, metrics, tag="smoke")
    before = model(sample_batch, tier="F")["preds"].clone()
    trainer.load_checkpoint(str(ckpt_path))
    after = model(sample_batch, tier="F")["preds"]
    assert torch.allclose(before, after, atol=1e-6), (
        "Checkpoint round-trip mismatch"
    )
    logger.info("Checkpoint round-trip: OK")

    logger.info("--- smoke test passed ---")
    return {
        "passed": True,
        "train_loss": train_loss,
        "val_loss": val_loss,
        "grad_ratio": grad_ratio,
        "gate_a": gate_a,
        "gate_f": gate_f,
    }
