"""CarbonTrainer -- training loop with curriculum learning, three-group loss,
LUPI distillation, early stopping, and viability check."""

import logging
import math
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import torch
from torch.utils.data import DataLoader

from model.carbon_footprint.src.utils.config import CarbonConfig
from model.carbon_footprint.src.training.model import CarbonModel
from model.carbon_footprint.src.training.loss import ThreeGroupLoss
from model.carbon_footprint.src.preprocessing.transforms import (
    HEAD_NAMES,
    Log1pZScoreTransform,
)
from model.carbon_footprint.src.evaluation.metrics import compute_metrics
from model.carbon_footprint.src.training.checkpoint import (
    CheckpointMixin, smoke_test,
)
from model.carbon_footprint.src.training.curriculum import curriculum_tier_probs

logger = logging.getLogger(__name__)

# Re-export smoke_test at module level for convenience
__all__ = ["CarbonTrainer", "get_lr_scheduler", "smoke_test"]

TARGET_KEYS = [f"cf_{h}" for h in HEAD_NAMES]


def get_lr_scheduler(
    optimizer: torch.optim.Optimizer,
    warmup_epochs: int,
    max_epochs: int,
) -> torch.optim.lr_scheduler.LambdaLR:
    """Linear warmup for warmup_epochs, then cosine decay to zero."""
    def lr_lambda(epoch: int) -> float:
        if epoch < warmup_epochs:
            return max(epoch / warmup_epochs, 1e-4)
        progress = (epoch - warmup_epochs) / max(max_epochs - warmup_epochs, 1)
        return 0.5 * (1.0 + math.cos(math.pi * progress))
    return torch.optim.lr_scheduler.LambdaLR(optimizer, lr_lambda)


class CarbonTrainer(CheckpointMixin):
    """Training orchestrator for the carbon footprint model."""

    def __init__(
        self,
        config: CarbonConfig,
        model: CarbonModel,
        loss_fn: ThreeGroupLoss,
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
            model.parameters(),
            lr=config.lr,
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

    def _transform_targets(self, batch: Dict[str, torch.Tensor]) -> None:
        """In-place transform raw targets to z-score space in the batch."""
        for head in HEAD_NAMES:
            key = f"cf_{head}"
            raw = batch[key].cpu().numpy()
            z = self.transform.transform(raw, head)
            batch[key] = torch.tensor(z, dtype=torch.float32).to(
                self.model_device
            )

    # ----- train / val epochs -----

    def train_epoch(self, epoch: int) -> Tuple[float, Dict[str, float]]:
        """Run one training epoch. Returns (epoch_loss, loss_component_dict)."""
        original_probs = self.config.tier_probs
        self.config.tier_probs = curriculum_tier_probs(self.config, epoch)
        self.model.train()
        total_loss = 0.0
        comp_sums: Dict[str, float] = {}
        n_batches = 0

        for batch in self.train_loader:
            batch = self._to_device(batch)
            self._transform_targets(batch)

            out = self.model(batch, tier=None)
            loss, loss_dict = self.loss_fn(out, batch, epoch)

            self.optimizer.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(self.model.parameters(), max_norm=1.0)
            self.optimizer.step()

            total_loss += loss.item()
            for k, v in loss_dict.items():
                if isinstance(v, torch.Tensor):
                    comp_sums[k] = comp_sums.get(k, 0.0) + v.item()
                elif isinstance(v, (int, float)):
                    comp_sums[k] = comp_sums.get(k, 0.0) + v
            n_batches += 1

        self.config.tier_probs = original_probs
        n = max(n_batches, 1)
        avg_comps = {k: v / n for k, v in comp_sums.items()}
        return total_loss / n, avg_comps

    @torch.no_grad()
    def val_epoch(
        self,
        tier: Optional[str] = None,
        loader: Optional[DataLoader] = None,
    ) -> Tuple[float, Dict[str, float]]:
        """Run one validation epoch. Returns (val_loss, metrics_dict)."""
        self.model.eval()
        all_preds: List[np.ndarray] = []
        all_targets: List[np.ndarray] = []
        total_loss = 0.0
        n_batches = 0

        for batch in (loader or self.val_loader):
            batch = self._to_device(batch)
            self._transform_targets(batch)

            out = self.model(batch, tier=tier)
            loss, _ = self.loss_fn(out, batch, 0)
            total_loss += loss.item()

            preds_np = out["preds"].cpu().numpy()
            targets_np = torch.stack(
                [batch[k] for k in TARGET_KEYS], dim=-1,
            ).cpu().numpy()

            all_preds.append(preds_np)
            all_targets.append(targets_np)
            n_batches += 1

        avg_loss = total_loss / max(n_batches, 1)
        preds_z = np.concatenate(all_preds, axis=0)
        targets_z = np.concatenate(all_targets, axis=0)
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
            train_loss, loss_comps = self.train_epoch(epoch)
            val_loss, metrics = self.val_epoch()
            self.scheduler.step()

            entry = {
                "epoch": epoch,
                "train_loss": train_loss,
                "val_loss": val_loss,
                "lr": self.optimizer.param_groups[0]["lr"],
                "distill_coeff": loss_comps.get("distill_coeff", 0.0),
                "L_raw": loss_comps.get("L_raw", 0.0),
                "L_transport": loss_comps.get("L_transport", 0.0),
                "L_processing": loss_comps.get("L_processing", 0.0),
                "L_packaging": loss_comps.get("L_packaging", 0.0),
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
                "Epoch %03d  train=%.4f  val=%.4f  lr=%.2e"
                "  pat=%d/%d%s",
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
            vl, _ = self.val_epoch()
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

        # Check: val loss not monotonically increasing
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
