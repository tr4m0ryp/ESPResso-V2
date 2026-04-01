"""WA1Trainer -- training loop with early stopping and viability check.

AdamW + linear warmup (5 epochs) + cosine decay. Early stopping with
patience=15. Checkpoint/logging/smoke_test in checkpoint.py.
Reference: notes/water-model-implementation.md (D6, D8).
"""

import logging
import math
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import torch
from torch.utils.data import DataLoader

from model.water_footprint.src.utils.config import WA1Config
from model.water_footprint.src.training.model import WA1Model
from model.water_footprint.src.training.loss import UWSOLoss
from model.water_footprint.src.preprocessing.transforms import Log1pZScoreTransform
from model.water_footprint.src.evaluation.metrics import compute_metrics
from model.water_footprint.src.training.checkpoint import CheckpointMixin, smoke_test

logger = logging.getLogger(__name__)

# Re-export smoke_test at module level for convenience
__all__ = ["WA1Trainer", "get_lr_scheduler", "smoke_test"]


# ---------------------------------------------------------------------------
# LR scheduler: linear warmup + cosine decay
# ---------------------------------------------------------------------------

def get_lr_scheduler(
    optimizer: torch.optim.Optimizer,
    warmup_epochs: int,
    max_epochs: int,
) -> torch.optim.lr_scheduler.LambdaLR:
    """Linear warmup for warmup_epochs, then cosine decay to zero."""
    def lr_lambda(epoch: int) -> float:
        if epoch < warmup_epochs:
            return epoch / warmup_epochs
        progress = (epoch - warmup_epochs) / max(max_epochs - warmup_epochs, 1)
        return 0.5 * (1.0 + math.cos(math.pi * progress))
    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)


# ---------------------------------------------------------------------------
# Trainer
# ---------------------------------------------------------------------------

class WA1Trainer(CheckpointMixin):
    """Training orchestrator for the WA1 water footprint model."""

    def __init__(
        self,
        config: WA1Config,
        model: WA1Model,
        loss_fn: UWSOLoss,
        train_loader: DataLoader,
        val_loader: DataLoader,
        transform: Log1pZScoreTransform,
        device: Optional[torch.device] = None,
    ) -> None:
        self.config = config
        self.model = model.to(device or config.device)
        self.loss_fn = loss_fn.to(self.model_device)
        self.train_loader = train_loader
        self.val_loader = val_loader
        self.transform = transform
        self.history: List[Dict[str, Any]] = []

        self.optimizer = torch.optim.AdamW(
            list(model.parameters()) + list(loss_fn.parameters()),
            lr=config.learning_rate,
            weight_decay=config.weight_decay,
        )
        self.scheduler = get_lr_scheduler(
            self.optimizer, config.warmup_epochs, config.max_epochs,
        )

    @property
    def model_device(self) -> torch.device:
        return next(self.model.parameters()).device

    def _to_device(self, batch: Dict[str, torch.Tensor]) -> Dict[str, torch.Tensor]:
        dev = self.model_device
        return {k: v.to(dev) if isinstance(v, torch.Tensor) else v
                for k, v in batch.items()}

    def _build_targets(self, batch: Dict[str, torch.Tensor]) -> torch.Tensor:
        """Stack and z-score transform the 3 target columns -> [B, 3]."""
        raw = self.transform.transform(batch["wf_raw"].cpu().numpy(), "raw")
        proc = self.transform.transform(
            batch["wf_processing"].cpu().numpy(), "processing",
        )
        pkg = self.transform.transform(
            batch["wf_packaging"].cpu().numpy(), "packaging",
        )
        return torch.tensor(
            np.stack([raw, proc, pkg], axis=1), dtype=torch.float32,
        ).to(self.model_device)

    # ----- train / val epochs -----

    def train_epoch(self, epoch: int) -> Tuple[float, Dict[str, float]]:
        """Run one training epoch. Returns (epoch_loss, per_head_losses)."""
        self.model.train()
        total_loss = 0.0
        head_sums: Dict[str, float] = {
            "raw": 0.0, "processing": 0.0, "packaging": 0.0,
        }
        n_batches = 0

        for batch in self.train_loader:
            batch = self._to_device(batch)
            targets = self._build_targets(batch)
            out = self.model(batch, tier=None)
            loss, head_losses, _ = self.loss_fn(out["preds"], targets)

            self.optimizer.zero_grad()
            loss.backward()
            self.optimizer.step()

            total_loss += loss.item()
            for h in head_sums:
                head_sums[h] += head_losses[h].item()
            n_batches += 1

        avg = total_loss / max(n_batches, 1)
        avg_heads = {h: v / max(n_batches, 1) for h, v in head_sums.items()}
        return avg, avg_heads

    @torch.no_grad()
    def val_epoch(
        self, tier: Optional[str] = None,
    ) -> Tuple[float, Dict[str, float]]:
        """Run one validation epoch. Returns (val_loss, metrics_dict)."""
        self.model.eval()
        all_preds: Dict[str, List] = {
            "raw": [], "processing": [], "packaging": [],
        }
        all_targets: Dict[str, List] = {
            "raw": [], "processing": [], "packaging": [],
        }
        total_loss = 0.0
        n_batches = 0

        for batch in self.val_loader:
            batch = self._to_device(batch)
            targets = self._build_targets(batch)
            out = self.model(batch, tier=tier)
            loss, _, _ = self.loss_fn(out["preds"], targets)
            total_loss += loss.item()

            preds_np = out["preds"].cpu().numpy()
            targets_np = targets.cpu().numpy()
            for i, h in enumerate(["raw", "processing", "packaging"]):
                all_preds[h].append(preds_np[:, i])
                all_targets[h].append(targets_np[:, i])
            n_batches += 1

        avg_loss = total_loss / max(n_batches, 1)
        preds_z = {h: np.concatenate(v) for h, v in all_preds.items()}
        targets_z = {h: np.concatenate(v) for h, v in all_targets.items()}
        metrics = compute_metrics(preds_z, targets_z, self.transform)
        metrics["val_loss"] = avg_loss
        return avg_loss, metrics

    # ----- full training loop -----

    def train(
        self,
        max_epochs: Optional[int] = None,
        patience: Optional[int] = None,
    ) -> Tuple[Dict[str, float], List[Dict[str, Any]]]:
        """Full training loop with early stopping and checkpointing."""
        max_epochs = max_epochs or self.config.max_epochs
        patience = patience or self.config.patience
        best_val_loss = float("inf")
        best_metrics: Dict[str, float] = {}
        epochs_no_improve = 0
        start_time = time.time()

        for epoch in range(max_epochs):
            train_loss, head_losses = self.train_epoch(epoch)
            val_loss, metrics = self.val_epoch()
            self.scheduler.step()

            entry = {
                "epoch": epoch,
                "train_loss": train_loss,
                "val_loss": val_loss,
                "head_losses": head_losses,
                "lr": self.optimizer.param_groups[0]["lr"],
                **{k: v for k, v in metrics.items() if k != "val_loss"},
            }
            self.history.append(entry)

            improved = val_loss < best_val_loss
            if improved:
                best_val_loss = val_loss
                best_metrics = dict(metrics)
                epochs_no_improve = 0
                self.save_checkpoint(epoch, metrics, tag="best")
            else:
                epochs_no_improve += 1

            logger.info(
                "Epoch %03d  train=%.4f  val=%.4f  lr=%.2e  pat=%d/%d%s",
                epoch, train_loss, val_loss,
                entry["lr"], epochs_no_improve, patience,
                "  *" if improved else "",
            )

            if epochs_no_improve >= patience:
                logger.info("Early stopping at epoch %d", epoch)
                break

        elapsed = time.time() - start_time
        logger.info(
            "Training complete in %.1fs. Best val_loss=%.4f",
            elapsed, best_val_loss,
        )
        self.log_run(best_metrics, elapsed)
        return best_metrics, self.history

    # ----- viability check -----

    def viability_check(self, n_canary: int = 5) -> bool:
        """Run canary epochs and verify learning signal.

        Checks: loss decreasing, no NaN/Inf, no flat loss, val not
        diverging. On fail, logs a diagnostic report. On pass, canary
        epochs count toward total training.
        """
        logger.info("--- viability check: %d canary epochs ---", n_canary)
        train_losses: List[float] = []
        val_losses: List[float] = []

        for epoch in range(n_canary):
            tl, _ = self.train_epoch(epoch)
            vl, metrics = self.val_epoch()
            self.scheduler.step()
            train_losses.append(tl)
            val_losses.append(vl)
            self.history.append({
                "epoch": epoch, "train_loss": tl, "val_loss": vl,
                "lr": self.optimizer.param_groups[0]["lr"],
            })
            logger.info(
                "Canary %d/%d  train=%.4f  val=%.4f",
                epoch + 1, n_canary, tl, vl,
            )

            if not math.isfinite(tl) or not math.isfinite(vl):
                logger.error(
                    "ABORT: NaN/Inf loss at canary epoch %d. "
                    "Likely cause: learning rate too high. "
                    "Try reducing lr by 10x.", epoch,
                )
                return False

        # Check: training loss decreased over canary window
        if train_losses[-1] >= train_losses[0] * 0.99:
            logger.error(
                "ABORT: Training loss flat (%.4f -> %.4f). "
                "Likely cause: bad learning rate or dead gradients. "
                "Try lr *= 0.1 or check architecture.",
                train_losses[0], train_losses[-1],
            )
            return False

        # Check: val loss not immediately diverging
        if len(val_losses) >= 3 and all(
            val_losses[i] > val_losses[i - 1]
            for i in range(1, len(val_losses))
        ):
            logger.error(
                "ABORT: Val loss monotonically increasing (%.4f -> %.4f). "
                "Likely cause: severe overfitting or model too large. "
                "Try increasing dropout or reducing model size.",
                val_losses[0], val_losses[-1],
            )
            return False

        logger.info("--- viability check passed ---")
        return True
