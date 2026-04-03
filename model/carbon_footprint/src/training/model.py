"""CarbonModel -- LUPI-Enhanced Multi-Encoder with Dual CLS.

Wires together MaterialEncoder, StepLocProxy, ProductEncoder,
TransportEncoder (privileged), residual trunk, 4 output heads,
and 3 auxiliary heads. Implements tier-based masking and LUPI
distillation in forward().

Reference: notes/carbon_model_discuss.md (Decisions 4-7, 11-12, 16, 21)
Reference: research/model-design.md (Decided Architecture section)
"""

import random
from typing import Any, Dict, Optional

import torch
import torch.nn as nn
import torch.nn.functional as F

from model.carbon_footprint.src.utils.config import CarbonConfig
from model.carbon_footprint.src.training.encoders import (
    MaterialEncoder, ProductEncoder, TransportEncoder,
)
from model.carbon_footprint.src.training.step_loc_proxy import StepLocProxy
from model.carbon_footprint.src.training.masking import (
    apply_tier_masking, encode_with_fallback,
)


class ResidualBlock(nn.Module):
    """LayerNorm -> Linear -> GELU -> Dropout -> Linear -> residual add."""

    def __init__(self, hidden: int, dropout: float) -> None:
        super().__init__()
        self.block = nn.Sequential(
            nn.LayerNorm(hidden),
            nn.Linear(hidden, hidden),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(hidden, hidden),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return x + self.block(x)


class CarbonModel(nn.Module):
    """Carbon footprint prediction with LUPI distillation and tier masking.

    4 inference-time components: MaterialEncoder(64), StepLocProxy(64+64),
    ProductEncoder(48), packaging scalar(1).
    1 privileged component: TransportEncoder(64), training only.

    Args:
        config: CarbonConfig with all hyperparameters.
    """

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        self.config = config

        # -- Encoders --
        self.material_enc = MaterialEncoder(config)
        self.step_loc_proxy = StepLocProxy(config)
        self.product_enc = ProductEncoder(config)
        self.transport_enc = TransportEncoder(config)

        # -- Learned missing embeddings (for tier masking) --
        self.missing_material = nn.Parameter(
            torch.randn(config.material_out) * 0.02
        )
        self.missing_step_loc_transport = nn.Parameter(
            torch.randn(config.step_loc_out) * 0.02
        )
        self.missing_step_loc_processing = nn.Parameter(
            torch.randn(config.step_loc_out) * 0.02
        )
        self.missing_weight = nn.Parameter(torch.zeros(1))

        # -- Residual trunk --
        # mat_out + proxy_transport + proxy_processing + product_out + 1 (pkg)
        trunk_in_dim = (
            config.material_out + config.step_loc_out + config.step_loc_out
            + config.product_out + 1
        )
        self.trunk_proj = nn.Linear(trunk_in_dim, config.trunk_hidden)
        self.trunk_blocks = nn.ModuleList([
            ResidualBlock(config.trunk_hidden, config.trunk_dropout)
            for _ in range(config.trunk_blocks)
        ])

        # -- 4 output heads --
        def _make_head() -> nn.Sequential:
            return nn.Sequential(
                nn.Linear(config.trunk_hidden, config.trunk_hidden // 2),
                nn.GELU(),
                nn.Linear(config.trunk_hidden // 2, 1),
            )

        self.head_raw = _make_head()
        self.head_transport = _make_head()
        self.head_processing = _make_head()
        self.head_packaging = _make_head()

        # -- 3 auxiliary heads (training only) --
        self.head_aux_distance = nn.Linear(config.step_loc_out, 1)
        self.head_aux_mode = nn.Linear(config.step_loc_out, 2)
        # Weight prediction from category + materials only (no weight input)
        self.head_aux_weight = nn.Sequential(
            nn.Linear(config.category_emb + config.material_out,
                      config.trunk_hidden // 2),
            nn.GELU(),
            nn.Linear(config.trunk_hidden // 2, 1),
        )

        # Initialize weights for stability
        self._init_weights()

    def _init_weights(self) -> None:
        """Xavier init for linear layers, small init for output heads."""
        for module in [self.trunk_proj, *self.trunk_blocks]:
            for m in module.modules():
                if isinstance(m, nn.Linear):
                    nn.init.xavier_uniform_(m.weight)
                    if m.bias is not None:
                        nn.init.zeros_(m.bias)
        # Output heads: small init to start near zero predictions
        for head in [self.head_raw, self.head_transport,
                     self.head_processing, self.head_packaging]:
            for m in head.modules():
                if isinstance(m, nn.Linear):
                    nn.init.xavier_uniform_(m.weight, gain=0.1)
                    if m.bias is not None:
                        nn.init.zeros_(m.bias)

    def _build_priv_distances(
        self, batch: Dict[str, torch.Tensor],
    ) -> torch.Tensor:
        """Stack privileged distance features into [B, 6] tensor.

        NOTE: priv_* values are already log1p-transformed in the dataset
        parser (parsing.py). Do NOT apply log1p again here.
        """
        keys = [
            "priv_road_km", "priv_sea_km", "priv_rail_km",
            "priv_air_km", "priv_waterway_km", "priv_total_distance_km",
        ]
        return torch.stack(
            [batch[k] for k in keys], dim=-1,
        )  # [B, 6]

    def forward(
        self,
        batch: Dict[str, torch.Tensor],
        tier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Forward pass with tier-based masking and LUPI distillation.

        Args:
            batch: Dict of tensors from CarbonDataset.
            tier: Fixed tier (A-F), or None for curriculum (train) / E (eval).

        Returns:
            Dict with keys: preds [B, 4], proxy_transport [B, D],
            proxy_processing [B, D], transport_emb [B, D] or None,
            aux_distance_pred [B], aux_mode_pred [B, 2], aux_weight_pred [B].
        """
        B = batch["category_idx"].shape[0]
        device = batch["category_idx"].device
        cfg = self.config

        # -- Tier masking --
        tm = apply_tier_masking(batch, cfg.tier_probs, tier, self.training)
        avail = tm["avail"]

        # -- Material encoder --
        mat_emb = self.material_enc(
            batch["material_ids"], batch["material_pcts"],
            batch["material_mask"],
        )  # [B, material_out]
        mat_emb = encode_with_fallback(
            mat_emb, avail["materials"], self.missing_material,
        )

        # -- Step-location proxy (geo prior bypass: F23) --
        if tm["has_locations"].any():
            # Run proxy for samples that have location data
            proxy_t, proxy_p = self.step_loc_proxy(
                batch["step_loc_step_ids"], batch["step_loc_coords"],
                batch["step_loc_mask"],
                batch["haversine_sum"], batch["haversine_max"],
                batch["haversine_mean"],
            )
            # For samples without locations, use missing embeddings directly
            proxy_t = encode_with_fallback(
                proxy_t, tm["has_locations"],
                self.missing_step_loc_transport,
            )
            proxy_p = encode_with_fallback(
                proxy_p, tm["has_locations"],
                self.missing_step_loc_processing,
            )
        else:
            # All samples lack locations -- use learned missing embeddings
            proxy_t = self.missing_step_loc_transport.unsqueeze(0).expand(
                B, -1,
            )
            proxy_p = self.missing_step_loc_processing.unsqueeze(0).expand(
                B, -1,
            )

        # -- LUPI: privileged transport (Decision 5) --
        use_priv = (
            (tier == "F")
            or (self.training and random.random() < cfg.priv_ratio)
        )
        if use_priv and self.training:
            transport_emb = self.transport_enc(
                self._build_priv_distances(batch),
            )  # [B, step_loc_out]
            trunk_transport = transport_emb
        else:
            transport_emb = None
            trunk_transport = proxy_t

        # -- Weight masking --
        weight = batch["total_weight"].clone()
        weight_avail = avail["weight"] | avail["packaging"]
        if not weight_avail.all():
            weight[~weight_avail] = F.softplus(self.missing_weight).squeeze()

        # -- Packaging scalar (Decision 16) --
        pkg_scalar = torch.log1p(batch["total_packaging_mass"])  # [B]
        if not avail["packaging"].all():
            pkg_scalar = pkg_scalar.clone()
            pkg_scalar[~avail["packaging"]] = 0.0
        pkg_scalar = pkg_scalar.unsqueeze(-1)  # [B, 1]

        # -- Product encoder --
        product_emb = self.product_enc(
            batch["category_idx"], batch["subcategory_idx"],
            weight, batch["step_zscore"], batch["stage_coverage"],
            tm["mask_flags"],
        )  # [B, product_out]

        # -- Auxiliary predictions (always from proxy, during training) --
        aux_dist = self.head_aux_distance(proxy_t).squeeze(-1)   # [B]
        aux_mode = self.head_aux_mode(proxy_t)                    # [B, 2]
        cat_emb = self.product_enc.cat_emb(batch["category_idx"])  # [B, cat]
        aux_weight = self.head_aux_weight(
            torch.cat([cat_emb, mat_emb], dim=-1),
        ).squeeze(-1)  # [B]

        # -- Residual trunk --
        trunk_in = torch.cat(
            [mat_emb, trunk_transport, proxy_p, product_emb, pkg_scalar],
            dim=-1,
        )  # [B, trunk_in_dim]
        h = F.gelu(self.trunk_proj(trunk_in))  # [B, trunk_hidden]
        for block in self.trunk_blocks:
            h = block(h)

        # -- Output heads --
        preds = torch.cat([
            self.head_raw(h),
            self.head_transport(h),
            self.head_processing(h),
            self.head_packaging(h),
        ], dim=-1)  # [B, 4]

        return {
            "preds": preds,
            "proxy_transport": proxy_t,
            "proxy_processing": proxy_p,
            "transport_emb": transport_emb,
            "aux_distance_pred": aux_dist,
            "aux_mode_pred": aux_mode,
            "aux_weight_pred": aux_weight,
        }
