"""Input encoders for the carbon footprint model.

MaterialEncoder, ProductEncoder, and TransportEncoder. StepLocProxy lives
in step_loc_proxy.py due to its size. All take CarbonConfig and use GELU.

Reference: water_footprint/src/training/encoders.py for MaterialEncoder pattern.
"""

import torch
import torch.nn as nn

from model.carbon_footprint.src.utils.config import CarbonConfig

# Number of tier mask flags (one per encoder group, matches water model)
NUM_MASK_FLAGS = 5


class MaterialEncoder(nn.Module):
    """Encode materials: embedding + percentage -> self-attention -> masked pool.

    Input:
        material_ids  [B, max_mat]  -- vocab indices (0=pad)
        material_pcts [B, max_mat]  -- percentage [0,1]
        material_mask [B, max_mat]  -- True where valid

    Output: (pooled [B, material_out], pre_pooled [B, M, material_out])
        pooled: masked-mean-pooled representation (with missing fallback).
        pre_pooled: per-material token embeddings AFTER self-attention,
            before pooling. Used by MaterialLocAssignment for cross-attention.

    When all materials are masked, pooled uses a learned missing embedding
    and pre_pooled is zeroed.
    """

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        self.emb = nn.Embedding(config.vocab_materials, config.material_emb)

        # emb_dim + 1 (percentage) -> material_out
        self.mlp = nn.Sequential(
            nn.Linear(config.material_emb + 1, config.material_out),
            nn.GELU(),
        )

        # 2-head self-attention for material-material interaction
        self.self_attn = nn.MultiheadAttention(
            config.material_out, num_heads=2,
            dropout=0.1, batch_first=True,
        )
        self.norm = nn.LayerNorm(config.material_out)

        # Learned fallback for fully-masked samples
        self.missing_material = nn.Parameter(
            torch.randn(config.material_out) * 0.02
        )

    def forward(
        self,
        material_ids: torch.Tensor,
        material_pcts: torch.Tensor,
        material_mask: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        e = self.emb(material_ids)                            # [B, M, emb]
        pcts = material_pcts.unsqueeze(-1)                    # [B, M, 1]
        x = torch.cat([e, pcts], dim=-1)                      # [B, M, emb+1]
        x = self.mlp(x)                                       # [B, M, out]

        # Self-attention with residual + LayerNorm
        key_padding_mask = ~material_mask                      # True = ignore
        attn_out, _ = self.self_attn(x, x, x,
                                     key_padding_mask=key_padding_mask)
        x = self.norm(x + attn_out)
        x = x * material_mask.unsqueeze(-1).float()            # zero padding

        # Keep pre-pooled tokens for MaterialLocAssignment
        pre_pooled = x                                         # [B, M, out]

        # Masked mean pooling
        mask_sum = material_mask.sum(dim=1, keepdim=True).clamp(min=1).float()
        pooled = x.sum(dim=1) / mask_sum                       # [B, out]

        # Replace fully-masked rows with learned missing embedding
        all_masked = ~material_mask.any(dim=1)                 # [B]
        if all_masked.any():
            pooled = torch.where(
                all_masked.unsqueeze(-1),
                self.missing_material.unsqueeze(0).expand(pooled.shape[0], -1),
                pooled,
            )
        return pooled, pre_pooled


class ProductEncoder(nn.Module):
    """Encode product-level features with completeness heuristics.

    Input:
        category_idx    [B]        -- category vocab index
        subcategory_idx [B]        -- subcategory vocab index
        total_weight    [B]        -- raw weight in kg
        step_zscore     [B]        -- step-count z-score (Decision 12)
        stage_coverage  [B]        -- pipeline coverage ratio (Decision 12)
        mask_flags      [B, 5]     -- tier mask flags

    Decision 16: packaging is a scalar (log1p(total_packaging_mass)), fed as
    an additional feature. Total_packaging_mass is included via total_weight
    or concatenated at the trunk level, so it does not appear here.

    Output: [B, product_out]
    """

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        self.cat_emb = nn.Embedding(config.vocab_categories, config.category_emb)
        self.subcat_emb = nn.Embedding(
            config.vocab_subcategories, config.subcategory_emb
        )

        # cat_emb + subcat_emb + log1p(weight) + step_zscore +
        # stage_coverage + 5 mask_flags
        in_dim = (config.category_emb + config.subcategory_emb
                  + 1 + 1 + 1 + NUM_MASK_FLAGS)

        self.mlp = nn.Sequential(
            nn.Linear(in_dim, config.product_out),
            nn.GELU(),
        )

    def forward(
        self,
        category_idx: torch.Tensor,
        subcategory_idx: torch.Tensor,
        total_weight: torch.Tensor,
        step_zscore: torch.Tensor,
        stage_coverage: torch.Tensor,
        mask_flags: torch.Tensor,
    ) -> torch.Tensor:
        ce = self.cat_emb(category_idx)                       # [B, cat_emb]
        se = self.subcat_emb(subcategory_idx)                 # [B, sub_emb]
        lw = torch.log1p(total_weight).unsqueeze(-1)          # [B, 1]
        zs = step_zscore.unsqueeze(-1)                        # [B, 1]
        sc = stage_coverage.unsqueeze(-1)                     # [B, 1]
        x = torch.cat([ce, se, lw, zs, sc, mask_flags], dim=-1)
        return self.mlp(x)


class TransportEncoder(nn.Module):
    """Privileged encoder: 6 log-distances -> embedding (training only).

    Input:
        priv_distances [B, 6] -- log1p-transformed distances:
            [road_km, sea_km, rail_km, air_km, waterway_km, total_distance_km]

    Output: [B, step_loc_out]

    Decision 5: only used during training. The trunk receives either
    transport_emb (60% of batches) or proxy_transport (40% of batches).
    """

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        self.mlp = nn.Sequential(
            nn.Linear(6, config.step_loc_out),
            nn.GELU(),
        )

    def forward(self, priv_distances: torch.Tensor) -> torch.Tensor:
        return self.mlp(priv_distances)                        # [B, D]
