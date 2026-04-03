"""UWSOLoss -- uncertainty-weighted softmax loss for three water footprint heads.

MSE for raw materials and processing heads, Huber(delta=1.5) for packaging.
UW-SO analytical weighting with learnable log-variance scalars.
Reference: notes/water-model-implementation.md (D8, F13).
"""

from typing import Dict, Tuple

import torch
import torch.nn as nn
import torch.nn.functional as F


class UWSOLoss(nn.Module):
    """Uncertainty-Weighted Softmax (UW-SO) multi-task loss.

    Three heads: raw_materials (MSE), processing (MSE), packaging (Huber).
    Learnable log-variance scalars determine adaptive head weights via
    softmax(-log_var). A regularization term sum(log_var) prevents all
    weights from collapsing to zero.

    Args:
        huber_delta: Delta parameter for Huber loss on the packaging head.
    """

    HEAD_NAMES = ("raw", "processing", "packaging")

    def __init__(self, huber_delta: float = 1.5) -> None:
        super().__init__()
        # Learnable log-variance per head, initialized to 0 (equal weights)
        self.log_vars = nn.Parameter(torch.zeros(3))
        self.huber_delta = huber_delta

    def forward(
        self, preds: torch.Tensor, targets: torch.Tensor
    ) -> Tuple[torch.Tensor, Dict[str, torch.Tensor], Dict[str, torch.Tensor]]:
        """Compute weighted multi-task loss.

        Args:
            preds: [B, 3] predictions in z-score space.
            targets: [B, 3] targets in z-score space.

        Returns:
            total_loss: Scalar combined loss.
            head_losses: Dict mapping head name to unweighted scalar loss.
            weights: Dict mapping head name to softmax weight (for logging).
        """
        # Per-head losses
        loss_raw = F.mse_loss(preds[:, 0], targets[:, 0])
        loss_proc = F.mse_loss(preds[:, 1], targets[:, 1])
        loss_pkg = F.huber_loss(
            preds[:, 2], targets[:, 2], delta=self.huber_delta
        )
        losses = torch.stack([loss_raw, loss_proc, loss_pkg])

        # UW-SO weighting: w_i = softmax(-log_var_i)
        # Clamp log_vars to [-4, 4] to prevent the regularizer sum from
        # drifting to large negative values and making total_loss negative.
        # Range [-4, 4] still allows ~50x weight variation between heads.
        clamped = self.log_vars.clamp(-4.0, 4.0)
        weights = F.softmax(-clamped, dim=0)

        # Combined: sum(w_i * L_i) + sum(log_var_i) as regularization
        total_loss = (weights * losses).sum() + clamped.sum()

        # Build output dicts
        head_losses = {
            name: losses[i].detach()
            for i, name in enumerate(self.HEAD_NAMES)
        }
        weight_dict = {
            name: weights[i].detach()
            for i, name in enumerate(self.HEAD_NAMES)
        }
        return total_loss, head_losses, weight_dict

    def auxiliary_weight_loss(
        self,
        weight_pred: torch.Tensor,
        weight_true: torch.Tensor,
        weight_avail: torch.Tensor,
    ) -> torch.Tensor:
        """MSE loss for auxiliary weight prediction, only on samples where weight is known.

        Args:
            weight_pred: [B] predicted weight from auxiliary head.
            weight_true: [B] actual total_weight_kg.
            weight_avail: [B] boolean mask (True = weight was available/known).

        Returns:
            Scalar loss (zero if no samples have weight available).
        """
        if not weight_avail.any():
            return torch.tensor(0.0, device=weight_pred.device)
        pred = weight_pred[weight_avail]
        true = torch.log1p(weight_true[weight_avail])
        return F.mse_loss(pred, true)
