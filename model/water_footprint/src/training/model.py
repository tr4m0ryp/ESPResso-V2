"""WA1Model -- Cross-Attention Geo-Aware Water Footprint Network.

Assembles 5 encoders, 2 GeoAttentionBlocks, shared trunk, and 3 output
heads. Implements tier-based masking in forward() per D2 and F11.
"""

from typing import Any, Dict, Optional

import torch
import torch.nn as nn

from model.water_footprint.src.utils.config import WA1Config
from model.water_footprint.src.training.encoders import (
    MaterialEncoder, StepEncoder, LocationEncoder, ProductEncoder,
    PackagingEncoder,
)
from model.water_footprint.src.training.cross_attention import GeoAttentionBlock

# Tier feature sets (F11). Each tier cumulatively unlocks feature groups.
_BASE = {"category", "subcategory", "materials", "percentages"}
TIER_FEATURES: Dict[str, set] = {
    "A": _BASE,
    "B": _BASE | {"total_weight"},
    "C": _BASE | {"total_weight", "locations"},
    "D": _BASE | {"total_weight", "locations", "steps"},
    "E": _BASE | {"total_weight", "locations", "steps", "packaging"},
    "F": _BASE | {"total_weight", "locations", "steps", "packaging",
                   "material_weights"},
}

# Feature group keys used for per-sample availability bitmask
_GROUPS = ("materials", "steps", "locations", "packaging",
           "material_weights", "total_weight")


def _sample_tiers(tier_probs: Dict[str, float], n: int) -> list:
    """Sample tier letters per sample from configured probabilities."""
    tiers = list(tier_probs.keys())
    probs = [tier_probs[t] for t in tiers]
    idx = torch.multinomial(torch.tensor(probs), n, replacement=True)
    return [tiers[i] for i in idx]


def _avail_mask(features_list: list, key: str,
                device: torch.device) -> torch.Tensor:
    """Boolean tensor [B] indicating per-sample availability of a group."""
    return torch.tensor([key in f for f in features_list], device=device)


def _masked_mean_pool(emb: torch.Tensor,
                      mask: torch.Tensor) -> torch.Tensor:
    """Mean-pool [B, N, D] over dim 1 using boolean mask [B, N]."""
    m = mask.float().unsqueeze(-1)  # [B, N, 1]
    return (emb * m).sum(dim=1) / m.sum(dim=1).clamp(min=1)


class WA1Model(nn.Module):
    """WA1 water footprint prediction model.

    Args:
        config: WA1Config with all hyperparameters.
    """

    def __init__(self, config: WA1Config) -> None:
        super().__init__()
        self.config = config
        d = config.encoder_output_dim

        # Encoders
        self.material_enc = MaterialEncoder(config)
        self.step_enc = StepEncoder(config)
        self.location_enc = LocationEncoder(config)
        self.product_enc = ProductEncoder(config)
        self.packaging_enc = PackagingEncoder(config)

        # Cross-attention blocks (share location keys)
        self.material_geo = GeoAttentionBlock(
            d, config.cross_attn_heads, config.vocab_materials,
            config.cross_attn_dropout, config.gate_hidden_dim)
        self.step_geo = GeoAttentionBlock(
            d, config.cross_attn_heads, config.vocab_steps,
            config.cross_attn_dropout, config.gate_hidden_dim)

        # Learned missing embeddings
        self.missing_material = nn.Parameter(torch.randn(d) * 0.02)
        self.missing_step = nn.Parameter(torch.randn(d) * 0.02)
        self.missing_location = nn.Parameter(torch.randn(d) * 0.02)
        self.missing_weight = nn.Parameter(torch.zeros(1))
        self.missing_packaging = nn.Parameter(torch.randn(16) * 0.02)

        # Shared trunk: 104 -> 64
        self.trunk = nn.Sequential(
            nn.Linear(config.trunk_input_dim, config.trunk_hidden_dim),
            nn.BatchNorm1d(config.trunk_hidden_dim),
            nn.GELU(),
            nn.Dropout(config.trunk_dropout),
        )

        # 3 output heads
        self.head_raw = nn.Linear(config.head_input_dim, 1)
        self.head_processing = nn.Linear(config.head_input_dim, 1)
        self.head_packaging = nn.Linear(config.head_input_dim, 1)

        # Auxiliary weight prediction head (D1)
        # Input: cat_emb(8) + mat_pooled(32) = 40 dims
        # Must NOT have access to total_weight (circular dependency)
        self.head_aux_weight = nn.Sequential(
            nn.Linear(config.embed_dim_category + config.encoder_output_dim, 16),
            nn.GELU(),
            nn.Linear(16, 1),
        )

    def _apply_tier_masking(self, batch: Dict[str, torch.Tensor],
                            B: int, device: torch.device,
                            tier: Optional[str]
                            ) -> Dict[str, Any]:
        """Resolve tiers, compute per-sample availability masks."""
        if tier is not None:
            tiers = [tier] * B
        elif self.training:
            tiers = _sample_tiers(self.config.tier_probs, B)
        else:
            tiers = ["F"] * B
        feats = [TIER_FEATURES[t] for t in tiers]
        avail = {g: _avail_mask(feats, g, device) for g in _GROUPS}

        # Subcategory independent mask (p=0.15 during training)
        subcat = batch["subcategory_idx"].clone()
        if self.training:
            subcat[torch.rand(B) < self.config.subcategory_mask_prob] = 0

        # Mask flags for ProductEncoder: [has_mat, has_step, has_loc, has_pkg, has_mw]
        flags = torch.stack([
            avail["materials"].float(), avail["steps"].float(),
            avail["locations"].float(), avail["packaging"].float(),
            avail["material_weights"].float(),
        ], dim=-1)  # [B, 5]

        return {"feats": feats, "avail": avail, "subcat": subcat,
                "flags": flags}

    def _encode_with_fallback(self, emb: torch.Tensor,
                              avail: torch.Tensor,
                              missing: torch.Tensor) -> torch.Tensor:
        """Replace unavailable samples' embeddings with learned missing emb.

        Args:
            emb: [B, N, D] or [B, D] encoded output.
            avail: [B] boolean per-sample availability.
            missing: [D] learned missing embedding.
        """
        if avail.all():
            return emb
        if emb.dim() == 3:
            exp = missing.unsqueeze(0).unsqueeze(0).expand_as(emb)
            return torch.where(avail.view(-1, 1, 1).expand_as(emb), emb, exp)
        exp = missing.unsqueeze(0).expand_as(emb)
        return torch.where(avail.view(-1, 1).expand_as(emb), emb, exp)

    def forward(self, batch: Dict[str, torch.Tensor],
                tier: Optional[str] = None,
                linked_mode: Optional[bool] = None) -> Dict[str, Any]:
        """Forward pass with tier-based masking.

        Args:
            batch: Dict of tensors from WaterFootprintDataset.
            tier: Fixed tier (A-F), or None for random (train) / full (eval).
            linked_mode: True=linked journey locations, False=unlinked set.

        Returns:
            Dict with 'preds' [B, 3] and 'gate_values' dict.
        """
        B = batch["category_idx"].shape[0]
        device = batch["category_idx"].device
        cfg = self.config

        tm = self._apply_tier_masking(batch, B, device, tier)
        avail = tm["avail"]

        # -- Material encoder --
        mat_w = batch["material_weights"].clone()
        mat_w[~avail["material_weights"]] = 0.0
        mat_emb = self.material_enc(
            batch["material_ids"], mat_w,
            batch["material_pcts"], batch["material_mask"])
        mat_emb = self._encode_with_fallback(
            mat_emb, avail["materials"], self.missing_material)

        # -- Step encoder --
        step_emb = self.step_enc(batch["step_ids"], batch["step_mask"])
        step_emb = self._encode_with_fallback(
            step_emb, avail["steps"], self.missing_step)

        # -- Location encoder + cross-attention --
        gate_mat = torch.zeros(B, device=device)
        gate_step = torch.zeros(B, device=device)

        if avail["locations"].any():
            if linked_mode is not None:
                use_linked = linked_mode
            elif self.training:
                use_linked = torch.rand(1).item() < cfg.linked_mode_prob
            else:
                use_linked = False
            if use_linked:
                loc_ids = torch.cat([batch["journey_origin_loc_ids"],
                                     batch["journey_proc_loc_ids"]], dim=1)
                loc_coords = torch.cat([batch["journey_origin_coords"],
                                        batch["journey_proc_coords"]], dim=1)
                loc_mask = (loc_ids != 0)
            else:
                loc_ids = batch["location_ids"]
                loc_coords = batch["location_coords"]
                loc_mask = batch["location_mask"]

            loc_emb = self.location_enc(loc_ids, loc_coords, loc_mask)
            loc_mask_adj = loc_mask.clone()
            loc_mask_adj[~avail["locations"]] = False

            mat_emb, mg = self.material_geo(
                mat_emb, batch["material_ids"], loc_emb, loc_mask_adj)
            step_emb, sg = self.step_geo(
                step_emb, batch["step_ids"], loc_emb, loc_mask_adj)
            gate_mat = mg.squeeze(-1).mean(dim=-1)
            gate_step = sg.squeeze(-1).mean(dim=-1)

        # -- Mean pool sequences --
        mat_pooled = _masked_mean_pool(mat_emb, batch["material_mask"])
        step_pooled = _masked_mean_pool(step_emb, batch["step_mask"])
        # Override unavailable samples with missing embedding
        if not avail["materials"].all():
            mat_pooled[~avail["materials"]] = self.missing_material
        if not avail["steps"].all():
            step_pooled[~avail["steps"]] = self.missing_step

        # -- Product encoder --
        weight = batch["total_weight"].clone()
        weight[~avail["total_weight"]] = self.missing_weight
        product_emb = self.product_enc(
            batch["category_idx"], tm["subcat"], weight, tm["flags"])

        # -- Packaging encoder --
        pkg_ids = batch["pkg_ids"]
        pkg_present = (pkg_ids != 0).float()
        n_pkg = pkg_present.sum(dim=1, keepdim=True).clamp(min=1)
        pkg_masses = batch["total_packaging_mass"].unsqueeze(1) * pkg_present / n_pkg
        pkg_emb = self.packaging_enc(pkg_masses, pkg_ids)
        pkg_emb = self._encode_with_fallback(
            pkg_emb, avail["packaging"], self.missing_packaging)

        # -- Auxiliary weight prediction (D1) --
        # Uses ONLY category embedding + mean-pooled materials (no total_weight)
        cat_emb = self.product_enc.cat_emb(batch["category_idx"])  # [B, 8]
        aux_weight_pred = self.head_aux_weight(
            torch.cat([cat_emb, mat_pooled], dim=-1)
        ).squeeze(-1)  # [B]

        # -- Trunk + heads --
        trunk_in = torch.cat(
            [mat_pooled, step_pooled, product_emb, pkg_emb], dim=-1)
        h = self.trunk(trunk_in)
        preds = torch.cat([self.head_raw(h), self.head_processing(h),
                           self.head_packaging(h)], dim=-1)

        return {
            "preds": preds,
            "gate_values": {"material": gate_mat, "step": gate_step},
            "aux_weight_pred": aux_weight_pred,
        }
