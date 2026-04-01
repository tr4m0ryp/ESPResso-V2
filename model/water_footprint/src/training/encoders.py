"""Input encoders for the WA1 water footprint model.

Five encoders transform raw padded tensors into fixed-dim embeddings:
MaterialEncoder, StepEncoder, LocationEncoder, ProductEncoder, PackagingEncoder.
All take WA1Config as first argument and use GELU activation.
"""

import math

import torch
import torch.nn as nn

from model.water_footprint.src.utils.config import WA1Config


class MaterialEncoder(nn.Module):
    """Encode materials: embedding + log_weight + percentage -> 64-dim per slot.

    Self-attention lets materials in a blend interact (e.g., cotton+polyester
    behaves differently than either alone).

    Input:  material_ids [B, 5], material_weights [B, 5],
            material_pcts [B, 5], material_mask [B, 5]
    Output: [B, 5, 64]  (padded positions zeroed)
    """

    def __init__(self, config: WA1Config) -> None:
        super().__init__()
        self.emb = nn.Embedding(config.vocab_materials, config.embed_dim_material)
        # 32 (emb) + 1 (log_weight) + 1 (pct) = 34
        self.mlp = nn.Linear(config.embed_dim_material + 2, config.encoder_output_dim)
        # Self-attention for material-material interaction
        self.self_attn = nn.MultiheadAttention(
            config.encoder_output_dim, config.mat_self_attn_heads,
            dropout=0.1, batch_first=True,
        )
        self.norm = nn.LayerNorm(config.encoder_output_dim)

    def forward(
        self,
        material_ids: torch.Tensor,
        material_weights: torch.Tensor,
        material_pcts: torch.Tensor,
        material_mask: torch.Tensor,
    ) -> torch.Tensor:
        e = self.emb(material_ids)                                    # [B, 5, 32]
        log_w = torch.log1p(material_weights).unsqueeze(-1)           # [B, 5, 1]
        pcts = material_pcts.unsqueeze(-1)                            # [B, 5, 1]
        x = torch.cat([e, log_w, pcts], dim=-1)                      # [B, 5, 34]
        x = torch.nn.functional.gelu(self.mlp(x))                    # [B, 5, 64]
        # Self-attention with residual + layernorm
        key_padding_mask = ~material_mask                             # True = ignore
        attn_out, _ = self.self_attn(x, x, x, key_padding_mask=key_padding_mask)
        x = self.norm(x + attn_out)
        x = x * material_mask.unsqueeze(-1).float()                  # zero padding
        return x


class StepEncoder(nn.Module):
    """Encode processing steps: embedding + sinusoidal position -> 64-dim per slot.

    Input:  step_ids [B, 27], step_mask [B, 27]
    Output: [B, 27, 64]  (padded positions zeroed)
    """

    def __init__(self, config: WA1Config) -> None:
        super().__init__()
        self.emb = nn.Embedding(config.vocab_steps, config.embed_dim_step)
        self.pos_dim = 4
        # 24 (emb) + 4 (pos) = 28
        self.mlp = nn.Linear(config.embed_dim_step + self.pos_dim, config.encoder_output_dim)
        # Precompute sinusoidal position encoding for max_steps positions
        pe = self._build_sinusoidal_pe(config.max_steps, self.pos_dim)
        self.register_buffer("pe", pe)

    @staticmethod
    def _build_sinusoidal_pe(max_len: int, dim: int) -> torch.Tensor:
        """Build sinusoidal positional encoding [max_len, dim]."""
        position = torch.arange(max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, dim, 2, dtype=torch.float) * (-math.log(10000.0) / dim)
        )
        pe = torch.zeros(max_len, dim)
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        return pe

    def forward(
        self,
        step_ids: torch.Tensor,
        step_mask: torch.Tensor,
    ) -> torch.Tensor:
        B, S = step_ids.shape
        e = self.emb(step_ids)                                        # [B, 27, 24]
        pos = self.pe[:S].unsqueeze(0).expand(B, -1, -1)              # [B, 27, 4]
        x = torch.cat([e, pos], dim=-1)                               # [B, 27, 28]
        x = torch.nn.functional.gelu(self.mlp(x))                    # [B, 27, 64]
        x = x * step_mask.unsqueeze(-1).float()
        return x


class LocationEncoder(nn.Module):
    """Encode processing locations: embedding + sincos coordinates -> 64-dim per slot.

    Input:  location_ids [B, 8], location_coords [B, 8, 4],
            location_mask [B, 8]
    Output: [B, 8, 64]  (padded positions zeroed)
    """

    def __init__(self, config: WA1Config) -> None:
        super().__init__()
        self.emb = nn.Embedding(config.vocab_countries, config.embed_dim_country)
        # 32 (emb) + 4 (sincos coords) = 36
        self.mlp = nn.Linear(config.embed_dim_country + 4, config.encoder_output_dim)

    def forward(
        self,
        location_ids: torch.Tensor,
        location_coords: torch.Tensor,
        location_mask: torch.Tensor,
    ) -> torch.Tensor:
        e = self.emb(location_ids)                                    # [B, 8, 32]
        x = torch.cat([e, location_coords], dim=-1)                  # [B, 8, 36]
        x = torch.nn.functional.gelu(self.mlp(x))                    # [B, 8, 64]
        x = x * location_mask.unsqueeze(-1).float()
        return x


class ProductEncoder(nn.Module):
    """Encode product-level features: category + subcategory + weight + mask flags.

    Input:  category_idx [B], subcategory_idx [B], total_weight [B],
            mask_flags [B, 5]
    Output: [B, product_enc_output_dim]
    """

    def __init__(self, config: WA1Config) -> None:
        super().__init__()
        self.cat_emb = nn.Embedding(config.vocab_categories, config.embed_dim_category)
        self.subcat_emb = nn.Embedding(
            config.vocab_subcategories, config.embed_dim_subcategory
        )
        # 16 (cat) + 16 (subcat) + 1 (log_weight) + 5 (mask_flags) = 38
        self.mlp = nn.Linear(
            config.embed_dim_category + config.embed_dim_subcategory + 1 + 5,
            config.product_enc_output_dim,
        )

    def forward(
        self,
        category_idx: torch.Tensor,
        subcategory_idx: torch.Tensor,
        total_weight: torch.Tensor,
        mask_flags: torch.Tensor,
    ) -> torch.Tensor:
        ce = self.cat_emb(category_idx)                               # [B, 16]
        se = self.subcat_emb(subcategory_idx)                         # [B, 16]
        lw = torch.log1p(total_weight).unsqueeze(-1)                  # [B, 1]
        x = torch.cat([ce, se, lw, mask_flags], dim=-1)               # [B, 38]
        x = torch.nn.functional.gelu(self.mlp(x))                    # [B, 48]
        return x


class PackagingEncoder(nn.Module):
    """Encode packaging: per-category log masses + category embeddings.

    Input:  pkg_masses [B, 3], pkg_category_ids [B, 3]
    Output: [B, pkg_enc_output_dim]
    """

    def __init__(self, config: WA1Config) -> None:
        super().__init__()
        self.emb = nn.Embedding(config.vocab_packaging, config.embed_dim_category)
        # 3 (log masses) + 3 * 16 (category embeddings) = 51
        self.mlp = nn.Linear(3 + 3 * config.embed_dim_category, config.pkg_enc_output_dim)

    def forward(
        self,
        pkg_masses: torch.Tensor,
        pkg_category_ids: torch.Tensor,
    ) -> torch.Tensor:
        log_m = torch.log1p(pkg_masses)                               # [B, 3]
        e = self.emb(pkg_category_ids)                                # [B, 3, 16]
        e_flat = e.reshape(e.shape[0], -1)                            # [B, 48]
        x = torch.cat([log_m, e_flat], dim=-1)                        # [B, 51]
        x = torch.nn.functional.gelu(self.mlp(x))                    # [B, 32]
        return x
