"""Checkpoint save/load, experiment logging, and smoke test for carbon model.

Split from trainer.py to stay under 300 lines per file.
"""

import json
import logging
import math
import time
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import torch
from torch.utils.data import DataLoader, Subset

from model.carbon_footprint.src.utils.config import CarbonConfig, set_seed
from model.carbon_footprint.src.training.model import CarbonModel
from model.carbon_footprint.src.training.loss import ThreeGroupLoss
from model.carbon_footprint.src.preprocessing.dataset import CarbonDataset
from model.carbon_footprint.src.preprocessing.transforms import (
    HEAD_NAMES,
    Log1pZScoreTransform,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Checkpoint mixin (mixed into CarbonTrainer)
# ---------------------------------------------------------------------------

class CheckpointMixin:
    """Checkpoint save/load and experiment logging for CarbonTrainer."""

    def save_checkpoint(
        self, epoch: int, metrics: Dict[str, float], tag: str = "latest",
    ) -> Path:
        """Save model + optimizer + loss + scheduler + transform state."""
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
    config: Optional[CarbonConfig] = None,
    data_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Validate full pipeline on CPU with minimal data.

    Loads 100 rows, runs 2 train batches + 1 val batch, verifies output
    shapes, loss finiteness, gradient flow, and checkpoint round-trip.

    Args:
        config: Optional CarbonConfig override. Defaults to CarbonConfig.smoke().
        data_path: Path to parquet data file. Falls back to config.data_path.

    Returns:
        Dict with 'passed' (bool) and diagnostic keys.
    """
    # Deferred import to avoid circular dependency at module level
    from model.carbon_footprint.src.training.trainer import CarbonTrainer

    logging.basicConfig(level=logging.INFO)
    logger.info("--- smoke test start ---")
    device = torch.device("cpu")
    if config is None:
        config = CarbonConfig.smoke()
    # Override settings for smoke test regardless of caller config
    config.max_epochs = 2
    config.batch_size = 16
    config.num_workers = 0
    config.pin_memory = False
    config.persistent_workers = False
    config.checkpoint_dir = "/tmp/carbon_smoke"
    set_seed(config.seed)

    data_path = data_path or str(config.data_path)
    dataset = CarbonDataset(data_path, config=config)

    # Override vocab sizes to match actual data (config defaults may be stale)
    config.vocab_materials = len(dataset.vocab["materials"]) + 1  # +1 for pad
    config.vocab_steps = len(dataset.vocab["steps"]) + 1
    config.vocab_categories = len(dataset.vocab["categories"]) + 1
    config.vocab_subcategories = len(dataset.vocab["subcategories"]) + 1

    n_rows = min(config.smoke_test_rows, len(dataset))
    indices = list(range(n_rows))

    # Fit transform on the subset
    records = [dataset.records[i] for i in indices]
    arrays = {
        h: np.array([r[f"cf_{h}"] for r in records]) for h in HEAD_NAMES
    }
    transform = Log1pZScoreTransform().fit(
        arrays["raw_materials"], arrays["transport"],
        arrays["processing"], arrays["packaging"],
    )

    split_idx = int(len(indices) * 0.7)
    train_sub = Subset(dataset, indices[:split_idx])
    val_sub = Subset(dataset, indices[split_idx:])
    train_loader = DataLoader(train_sub, batch_size=16, shuffle=True)
    val_loader = DataLoader(val_sub, batch_size=16)

    model = CarbonModel(config).to(device)
    loss_fn = ThreeGroupLoss(config).to(device)
    trainer = CarbonTrainer(
        config, model, loss_fn, train_loader, val_loader, transform,
        device=device,
    )

    # 1. Run train epoch
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
    assert grad_ratio > 0.50, (
        f"Too few parameters received gradients: {n_with_grad}/{n_total} "
        f"(ratio={grad_ratio:.2f})"
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
        out = model(sample_batch, tier="E")
    assert out["preds"].shape[1] == 4, (
        f"Expected 4 heads, got {out['preds'].shape}"
    )
    logger.info("Output shape: %s", out["preds"].shape)

    # 5. Verify auxiliary outputs
    expected_B = sample_batch["category_idx"].shape[0]
    assert out["aux_weight_pred"].shape == (expected_B,)
    assert out["aux_distance_pred"].shape == (expected_B,)
    assert out["aux_mode_pred"].shape == (expected_B, 2)
    logger.info("Auxiliary output shapes: OK")

    # 6. Checkpoint round-trip
    ckpt_path = trainer.save_checkpoint(0, metrics, tag="smoke")
    before = model(sample_batch, tier="E")["preds"].clone()
    trainer.load_checkpoint(str(ckpt_path))
    after = model(sample_batch, tier="E")["preds"]
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
        "n_params": n_total,
        "metrics": {k: v for k, v in metrics.items()
                    if isinstance(v, (int, float))},
    }
