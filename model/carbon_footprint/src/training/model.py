"""CarbonModel -- LUPI-Enhanced Multi-Encoder with Dual CLS.

Reference: notes/carbon_model_discuss.md (Decisions 4-7, 11-12, 16, 21)
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
from model.carbon_footprint.src.training.material_loc_assign import (
    MaterialLocAssignment,
)
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
    """Carbon footprint prediction with LUPI distillation and tier masking."""

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        self.config = config

        # -- Encoders --
        self.material_enc = MaterialEncoder(config)
        self.step_loc_proxy = StepLocProxy(config)
        self.product_enc = ProductEncoder(config)
        self.transport_enc = TransportEncoder(config)

        # -- Material-location cross-attention assignment --
        self.mat_loc_assign = MaterialLocAssignment(config)
        self.mat_loc_norm = nn.LayerNorm(config.assign_out)

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
        # Transport head: wider input (trunk + mat-loc assignment feature)
        transport_head_in = config.trunk_hidden + config.assign_out
        self.head_transport = nn.Sequential(
            nn.Linear(transport_head_in, transport_head_in // 2),
            nn.GELU(),
            nn.Linear(transport_head_in // 2, 1),
        )
        self.head_processing = _make_head()
        self.head_packaging = _make_head()

        # -- Packaging shortcut --
        pkg_shortcut_in = 1 + config.category_emb + config.subcategory_emb
        self.pkg_shortcut = nn.Sequential(
            nn.Linear(pkg_shortcut_in, 32),
            nn.GELU(),
            nn.Linear(32, 1),
        )

        # -- Processing branch (gradient-isolated) --
        proc_branch_in = config.step_loc_out + config.product_out
        self.processing_branch = nn.Sequential(
            nn.Linear(proc_branch_in, config.trunk_hidden // 2),
            nn.GELU(),
            nn.Linear(config.trunk_hidden // 2, 1),
        )

        # -- Auxiliary heads (training only) --
        self.head_aux_distance = nn.Linear(config.step_loc_out, 1)
        self.head_aux_mode = nn.Linear(config.step_loc_out, 2)
        self.head_aux_weight = nn.Sequential(
            nn.Linear(config.category_emb + config.material_out,
                      config.trunk_hidden // 2),
            nn.GELU(),
            nn.Linear(config.trunk_hidden // 2, 1),
        )

        # Initialize weights for stability
        self._init_weights()

    def _init_weights(self) -> None:
        """Xavier init for trunk, small init for heads/shortcuts."""
        for m in [self.trunk_proj, *self.trunk_blocks]:
            for sub in m.modules():
                if isinstance(sub, nn.Linear):
                    nn.init.xavier_uniform_(sub.weight)
                    if sub.bias is not None:
                        nn.init.zeros_(sub.bias)
        for head in [self.head_raw, self.head_transport,
                     self.head_processing, self.head_packaging,
                     self.pkg_shortcut, self.processing_branch]:
            for sub in head.modules():
                if isinstance(sub, nn.Linear):
                    nn.init.xavier_uniform_(sub.weight, gain=0.1)
                    if sub.bias is not None:
                        nn.init.zeros_(sub.bias)

    def _build_priv_distances(self, batch: Dict[str, torch.Tensor]) -> torch.Tensor:
        """Stack privileged distances [B, 6]. Already log1p'd in parser."""
        return torch.stack([batch[k] for k in (
            "priv_road_km", "priv_sea_km", "priv_rail_km",
            "priv_air_km", "priv_waterway_km", "priv_total_distance_km",
        )], dim=-1)

    def forward(
        self,
        batch: Dict[str, torch.Tensor],
        tier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Forward pass. Returns preds [B,4] + proxy/aux/entropy outputs."""
        B = batch["category_idx"].shape[0]
        device = batch["category_idx"].device
        cfg = self.config

        # -- Tier masking --
        tm = apply_tier_masking(batch, cfg.tier_probs, tier, self.training)
        avail = tm["avail"]

        # -- Material encoder --
        mat_emb, mat_tokens = self.material_enc(
            batch["material_ids"], batch["material_pcts"],
            batch["material_mask"],
        )  # [B, material_out], [B, M, material_out]
        mat_emb = encode_with_fallback(
            mat_emb, avail["materials"], self.missing_material,
        )

        # -- Step-location proxy --
        if tm["has_locations"].any():
            proxy_t, proxy_p, attn_entropy, pre_cls_tok = self.step_loc_proxy(
                batch["step_loc_step_ids"], batch["step_loc_coords"],
                batch["step_loc_mask"],
                batch["haversine_sum"], batch["haversine_max"],
                batch["haversine_mean"],
                batch["distance_histogram"],
                batch["step_pair_distances"],
            )
            proxy_t = encode_with_fallback(
                proxy_t, tm["has_locations"],
                self.missing_step_loc_transport,
            )
            proxy_p = encode_with_fallback(
                proxy_p, tm["has_locations"],
                self.missing_step_loc_processing,
            )
        else:
            proxy_t = self.missing_step_loc_transport.unsqueeze(0).expand(
                B, -1,
            )
            proxy_p = self.missing_step_loc_processing.unsqueeze(0).expand(
                B, -1,
            )
            attn_entropy = torch.tensor(0.0, device=device)
            N = batch["step_loc_step_ids"].shape[1]
            pre_cls_tok = torch.zeros(
                B, N, cfg.step_loc_out, device=device,
            )

        # -- LUPI: privileged transport --
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

        # -- Packaging scalar --
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

        # -- Auxiliary predictions (from proxy, training only) --
        aux_dist = self.head_aux_distance(proxy_t).squeeze(-1)   # [B]
        aux_mode = self.head_aux_mode(proxy_t)                    # [B, 2]
        cat_emb = self.product_enc.cat_emb(batch["category_idx"])  # [B, cat]
        aux_weight = self.head_aux_weight(
            torch.cat([cat_emb, mat_emb], dim=-1),
        ).squeeze(-1)  # [B]

        # -- Residual trunk --
        proxy_p_detached = proxy_p.detach()
        trunk_in = torch.cat(
            [mat_emb, trunk_transport, proxy_p_detached, product_emb,
             pkg_scalar],
            dim=-1,
        )  # [B, trunk_in_dim]
        h = F.gelu(self.trunk_proj(trunk_in))  # [B, trunk_hidden]
        for block in self.trunk_blocks:
            h = block(h)

        # -- Processing branch --
        proc_branch_in = torch.cat([proxy_p, product_emb], dim=-1)
        proc_correction = self.processing_branch(proc_branch_in)  # [B, 1]

        # -- Packaging shortcut --
        subcat_emb = self.product_enc.subcat_emb(batch["subcategory_idx"])
        pkg_shortcut_in = torch.cat(
            [pkg_scalar, cat_emb, subcat_emb], dim=-1,
        )
        pkg_shortcut_pred = self.pkg_shortcut(pkg_shortcut_in)  # [B, 1]

        # -- Material-location assignment (transport-specific feature) --
        mat_loc_feature = self.mat_loc_assign(
            mat_tokens, pre_cls_tok,
            batch["material_mask"], batch["step_loc_mask"],
        )  # [B, assign_out]

        # -- Output heads --
        # Normalize mat_loc_feature to prevent sparse-product amplification
        mat_loc_feature = self.mat_loc_norm(mat_loc_feature)
        transport_features = torch.cat(
            [h, mat_loc_feature], dim=-1,
        )  # [B, trunk_hidden + assign_out]
        preds = torch.cat([
            self.head_raw(h),
            self.head_transport(transport_features),
            self.head_processing(h) + proc_correction,
            self.head_packaging(h) + pkg_shortcut_pred,
        ], dim=-1)  # [B, 4]

        return {
            "preds": preds,
            "proxy_transport": proxy_t,
            "proxy_processing": proxy_p,
            "transport_emb": transport_emb,
            "aux_distance_pred": aux_dist,
            "aux_mode_pred": aux_mode,
            "aux_weight_pred": aux_weight,
            "attn_entropy": attn_entropy,
        }
