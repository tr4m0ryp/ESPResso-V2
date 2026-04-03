"""StepLocProxy encoder -- the core architectural innovation.

Dual CLS tokens over a self-attention sequence of (step, location) tokens.
CLS_transport is distilled toward the privileged TransportEncoder (LUPI).
CLS_processing is free, trained only by the processing head task loss.

Reference: Decisions 4, 11 from notes/carbon_model_discuss.md
"""

import torch
import torch.nn as nn

from model.carbon_footprint.src.utils.config import CarbonConfig


class StepLocProxy(nn.Module):
    """Encode step-location tokens via self-attention with dual CLS readout.

    Input:
        step_loc_step_ids  [B, max_tokens]    -- step vocab indices (0=pad)
        step_loc_coords    [B, max_tokens, 4] -- sin/cos encoded lat/lon
        step_loc_mask      [B, max_tokens]     -- True where valid
        haversine_sum      [B]
        haversine_max      [B]
        haversine_mean     [B]

    Output:
        (proxy_transport [B, step_loc_out], proxy_processing [B, step_loc_out])

    When all tokens are masked, returns learned missing embeddings.
    """

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        self.step_emb = nn.Embedding(config.vocab_steps, config.step_emb)
        token_in_dim = config.step_emb + config.coord_dim  # emb + 4

        self.token_mlp = nn.Sequential(
            nn.Linear(token_in_dim, config.step_loc_out),
            nn.GELU(),
        )

        # Dual CLS tokens (Decision 11): init randn * 0.02
        self.cls_transport = nn.Parameter(
            torch.randn(1, 1, config.step_loc_out) * 0.02
        )
        self.cls_processing = nn.Parameter(
            torch.randn(1, 1, config.step_loc_out) * 0.02
        )

        # 1-layer self-attention over [CLS_t, CLS_p, tok_1, ..., tok_N]
        self.self_attn = nn.MultiheadAttention(
            config.step_loc_out,
            config.step_loc_attn_heads,
            dropout=config.trunk_dropout,
            batch_first=True,
        )
        self.norm = nn.LayerNorm(config.step_loc_out)

        # Post-CLS projection: cls_out(D) || haversine_stats(3) -> out_dim
        cls_plus_stats = config.step_loc_out + 3
        self.transport_proj = nn.Sequential(
            nn.Linear(cls_plus_stats, config.step_loc_out),
            nn.GELU(),
        )
        self.processing_proj = nn.Sequential(
            nn.Linear(cls_plus_stats, config.step_loc_out),
            nn.GELU(),
        )

        # Learned fallbacks when all tokens are masked
        self.missing_transport = nn.Parameter(
            torch.randn(config.step_loc_out) * 0.02
        )
        self.missing_processing = nn.Parameter(
            torch.randn(config.step_loc_out) * 0.02
        )

    def forward(
        self,
        step_loc_step_ids: torch.Tensor,
        step_loc_coords: torch.Tensor,
        step_loc_mask: torch.Tensor,
        haversine_sum: torch.Tensor,
        haversine_max: torch.Tensor,
        haversine_mean: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor]:
        B, N = step_loc_step_ids.shape

        # Per-token: step_emb || coords -> MLP
        e = self.step_emb(step_loc_step_ids)             # [B, N, step_emb]
        x = torch.cat([e, step_loc_coords], dim=-1)      # [B, N, step_emb+4]
        x = self.token_mlp(x)                             # [B, N, D]

        # Prepend dual CLS tokens
        cls_t = self.cls_transport.expand(B, -1, -1)      # [B, 1, D]
        cls_p = self.cls_processing.expand(B, -1, -1)     # [B, 1, D]
        tokens = torch.cat([cls_t, cls_p, x], dim=1)      # [B, 2+N, D]

        # Build key_padding_mask: True = ignore. CLS tokens always unmasked.
        cls_mask = torch.ones(B, 2, dtype=torch.bool,
                              device=step_loc_mask.device)
        full_mask = torch.cat([cls_mask, step_loc_mask], dim=1)  # [B, 2+N]
        key_padding_mask = ~full_mask  # True where padded

        # Self-attention with residual + LayerNorm
        attn_out, _ = self.self_attn(
            tokens, tokens, tokens, key_padding_mask=key_padding_mask
        )
        tokens = self.norm(tokens + attn_out)

        # Extract CLS outputs
        cls_transport_out = tokens[:, 0, :]                # [B, D]
        cls_processing_out = tokens[:, 1, :]               # [B, D]

        # Concat haversine stats (log-scaled to prevent overflow) and project
        h_stats = torch.stack([
            torch.log1p(haversine_sum),
            torch.log1p(haversine_max),
            torch.log1p(haversine_mean),
        ], dim=-1)                                         # [B, 3]

        proxy_transport = self.transport_proj(
            torch.cat([cls_transport_out, h_stats], dim=-1)
        )                                                  # [B, D]
        proxy_processing = self.processing_proj(
            torch.cat([cls_processing_out, h_stats], dim=-1)
        )                                                  # [B, D]

        # Handle fully-masked samples (no valid tokens)
        all_masked = ~step_loc_mask.any(dim=1)             # [B]
        if all_masked.any():
            proxy_transport = torch.where(
                all_masked.unsqueeze(-1),
                self.missing_transport.unsqueeze(0).expand(B, -1),
                proxy_transport,
            )
            proxy_processing = torch.where(
                all_masked.unsqueeze(-1),
                self.missing_processing.unsqueeze(0).expand(B, -1),
                proxy_processing,
            )

        return proxy_transport, proxy_processing
