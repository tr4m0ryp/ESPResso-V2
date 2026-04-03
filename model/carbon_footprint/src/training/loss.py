"""ThreeGroupLoss -- hierarchical three-group loss for carbon footprint model.

Group 1: 4 main task losses with analytical UW-SO + DB-MTL log-normalization.
Group 2: auxiliary losses (distance, mode fraction, weight) with linear warmup.
Group 3: structural losses (distillation warmup+decay, diversity fixed).

Reference: notes/carbon_model_discuss.md (Decisions 14, 15).
"""

import math
from typing import Any, Dict, Tuple

import torch
import torch.nn as nn
import torch.nn.functional as F

from ..utils.config import CarbonConfig


def log_cosh_loss(pred: torch.Tensor, target: torch.Tensor) -> torch.Tensor:
    """Numerically stable log-cosh loss.

    log(cosh(x)) = |x| + log(1 + exp(-2|x|)) - log(2)
                 = |x| + softplus(-2|x|) - log(2)

    Behaves like MSE for small errors, like MAE for large errors.
    Twice differentiable (unlike Huber). No delta hyperparameter.
    """
    diff = pred - target
    abs_diff = diff.abs()
    return torch.mean(abs_diff + F.softplus(-2.0 * abs_diff) - math.log(2.0))


# -- Head loss dispatch --

_LOSS_FNS = {
    "mse": F.mse_loss,
    "log_cosh": log_cosh_loss,
}


class ThreeGroupLoss(nn.Module):
    """Hierarchical three-group loss for the carbon footprint model.

    No learnable parameters (unlike water model). All coefficients come
    from CarbonConfig.

    Args:
        config: CarbonConfig with loss hyperparameters.
    """

    HEAD_NAMES = ("raw_materials", "transport", "processing", "packaging")
    HEAD_INDICES = {name: i for i, name in enumerate(HEAD_NAMES)}

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        self.temperature = config.temperature
        self.aux_alpha = config.aux_alpha
        self.distill_peak = config.distill_peak
        self.distill_floor = config.distill_floor
        self.div_alpha = config.div_alpha
        self.entropy_alpha = config.entropy_alpha
        self.warmup_epochs = config.warmup_epochs
        self.curriculum_warmup_epochs = config.curriculum_warmup_epochs
        self.max_epochs = config.max_epochs

        # Per-head loss function dispatch
        self.head_loss_fns = {}
        for name in self.HEAD_NAMES:
            loss_type = config.head_loss_types[name]
            self.head_loss_fns[name] = _LOSS_FNS[loss_type]

    # -- Group 1: Main task losses --

    def _compute_main_loss(
        self, preds: torch.Tensor, batch: Dict[str, torch.Tensor],
    ) -> Tuple[torch.Tensor, Dict[str, torch.Tensor], Dict[str, float]]:
        """Analytical UW-SO with DB-MTL log-normalization over 4 heads.

        Returns:
            main_loss: scalar weighted loss.
            per_head: dict of per-head scalar losses (detached).
            weights: dict of per-head UW-SO weights (detached).
        """
        target_keys = [
            "cf_raw_materials", "cf_transport", "cf_processing", "cf_packaging",
        ]
        head_losses = []
        per_head = {}

        for i, name in enumerate(self.HEAD_NAMES):
            loss_fn = self.head_loss_fns[name]
            loss_val = loss_fn(preds[:, i], batch[target_keys[i]])
            head_losses.append(loss_val)
            per_head[name] = loss_val.detach()

        losses = torch.stack(head_losses)

        # DB-MTL log-normalization
        log_losses = torch.log1p(losses)

        # Analytical UW-SO: inverse-loss weighting with temperature
        inv = 1.0 / (log_losses.detach() + 1e-6)
        weights = F.softmax(inv / self.temperature, dim=0)

        main_loss = (weights * log_losses).sum()

        weight_dict = {
            name: weights[i].item() for i, name in enumerate(self.HEAD_NAMES)
        }
        return main_loss, per_head, weight_dict

    # -- Group 2: Auxiliary losses --

    def _compute_aux_loss(
        self,
        model_output: Dict[str, Any],
        batch: Dict[str, torch.Tensor],
        epoch: int,
    ) -> Tuple[torch.Tensor, Dict[str, torch.Tensor]]:
        """Auxiliary losses with linear warmup. Skips missing predictions."""
        device = batch["cf_raw_materials"].device
        warmup = min(1.0, epoch / max(self.curriculum_warmup_epochs, 1))
        alpha = warmup * self.aux_alpha

        aux_terms = {}
        count = 0

        # Distance prediction (priv_total_distance_km is already log1p'd in dataset)
        aux_dist = model_output.get("aux_distance_pred")
        if aux_dist is not None:
            target_dist = batch["priv_total_distance_km"]
            aux_terms["L_aux_dist"] = F.mse_loss(aux_dist, target_dist)
            count += 1

        # Mode fraction prediction
        aux_mode = model_output.get("aux_mode_pred")
        if aux_mode is not None:
            target_mode = torch.stack(
                [batch["priv_road_frac"], batch["priv_sea_frac"]], dim=-1,
            )
            aux_terms["L_aux_mode"] = F.mse_loss(aux_mode, target_mode)
            count += 1

        # Weight prediction
        aux_weight = model_output.get("aux_weight_pred")
        if aux_weight is not None:
            target_weight = torch.log1p(batch["total_weight"])
            aux_terms["L_aux_weight"] = F.mse_loss(aux_weight, target_weight)
            count += 1

        if count == 0:
            zero = torch.tensor(0.0, device=device)
            return zero, {"L_aux_dist": zero, "L_aux_mode": zero,
                          "L_aux_weight": zero}

        raw_aux = sum(aux_terms.values()) / count
        aux_loss = alpha * raw_aux

        # Fill missing keys with zero for consistent logging
        for key in ("L_aux_dist", "L_aux_mode", "L_aux_weight"):
            if key not in aux_terms:
                aux_terms[key] = torch.tensor(0.0, device=device)

        return aux_loss, {k: v.detach() for k, v in aux_terms.items()}

    # -- Group 3: Structural losses --

    def _distill_coeff(self, epoch: int) -> float:
        """Distillation coefficient: linear warmup then linear decay."""
        warmup = self.warmup_epochs
        if epoch < warmup:
            return self.distill_peak * (epoch / max(warmup, 1))
        decay_range = max(self.max_epochs - warmup, 1)
        decay_progress = (epoch - warmup) / decay_range
        return (self.distill_floor
                + (self.distill_peak - self.distill_floor)
                * (1.0 - decay_progress))

    def _compute_structural_loss(
        self,
        model_output: Dict[str, Any],
        epoch: int,
    ) -> Tuple[torch.Tensor, Dict[str, torch.Tensor], float]:
        """Distillation + diversity + entropy structural losses.

        Skips L_distill when transport_emb is None (inference / some tiers).
        Adds attention entropy regularization to prevent uniform attention.
        """
        proxy_t = model_output["proxy_transport"]
        proxy_p = model_output["proxy_processing"]
        device = proxy_t.device

        distill_c = self._distill_coeff(epoch)

        # Distillation loss
        transport_emb = model_output.get("transport_emb")
        if transport_emb is not None:
            l_distill = F.mse_loss(proxy_t, transport_emb.detach())
        else:
            l_distill = torch.tensor(0.0, device=device)

        # Diversity loss: (1 + cos_sim) / 2 maps [-1, 1] -> [0, 1].
        # Raw cosine_similarity can go negative when proxies diverge,
        # which made the total loss negative. This remapping keeps
        # L_div in [0, 1] while preserving the same gradient direction:
        # minimizing L_div still pushes cosine similarity toward -1.
        raw_cos = F.cosine_similarity(proxy_t, proxy_p, dim=-1).mean()
        l_div = (1.0 + raw_cos) / 2.0

        # Attention entropy regularization: penalize HIGH entropy (uniform
        # attention). We want the CLS tokens to attend selectively to
        # informative step-location pairs, not spread weight uniformly.
        # L_entropy = max(0, entropy - target) so it only fires when
        # entropy is above the threshold (too uniform).
        attn_entropy = model_output.get("attn_entropy",
                                        torch.tensor(0.0, device=device))
        # Target: ~1.5 nats. For 10 tokens, max entropy is ln(10)=2.3.
        # 1.5 nats means attending to ~4-5 tokens with unequal weights,
        # which is appropriate for supply chain step sequences.
        target_entropy = 1.5
        l_entropy = F.relu(attn_entropy - target_entropy)

        structural = (distill_c * l_distill
                      + self.div_alpha * l_div
                      + self.entropy_alpha * l_entropy)

        parts = {
            "L_distill": l_distill.detach(),
            "L_div": l_div.detach(),
            "raw_cos_div": raw_cos.detach(),
            "L_entropy": l_entropy.detach(),
            "attn_entropy": attn_entropy.detach(),
        }
        return structural, parts, distill_c

    # -- Forward --

    def forward(
        self,
        model_output: Dict[str, Any],
        batch: Dict[str, torch.Tensor],
        epoch: int,
    ) -> Tuple[torch.Tensor, Dict[str, Any]]:
        """Compute total loss from three groups.

        Args:
            model_output: Dict with preds [B,4], proxy_transport, proxy_processing,
                transport_emb, aux_distance_pred, aux_mode_pred, aux_weight_pred.
            batch: Dict with target and privileged feature tensors.
            epoch: Current epoch number (for scheduling).

        Returns:
            total_loss: Scalar combined loss.
            loss_dict: Per-component losses, weights, and coefficients.
        """
        preds = model_output["preds"]

        main_loss, per_head, weights = self._compute_main_loss(preds, batch)
        aux_loss, aux_parts = self._compute_aux_loss(
            model_output, batch, epoch,
        )
        struct_loss, struct_parts, distill_c = self._compute_structural_loss(
            model_output, epoch,
        )

        total_loss = main_loss + aux_loss + struct_loss

        # Safety floor: total loss must never be negative. All three groups
        # are designed to be non-negative individually, but floating-point
        # accumulation could theoretically produce a tiny negative value.
        # clamp preserves gradients when total_loss > 0 (identity), and
        # zeros the gradient only in the degenerate negative case.
        total_loss = total_loss.clamp(min=0.0)

        loss_dict = {
            "main_loss": main_loss.detach(),
            "aux_loss": aux_loss.detach(),
            "structural_loss": struct_loss.detach(),
            "L_raw": per_head["raw_materials"],
            "L_transport": per_head["transport"],
            "L_processing": per_head["processing"],
            "L_packaging": per_head["packaging"],
            **aux_parts,
            **struct_parts,
            "weights": weights,
            "distill_coeff": distill_c,
        }
        return total_loss, loss_dict
