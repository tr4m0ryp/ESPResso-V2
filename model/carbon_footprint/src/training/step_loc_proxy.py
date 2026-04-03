"""StepLocProxy encoder -- the core architectural innovation.

Dual CLS tokens over a self-attention sequence of (step, location) tokens.
CLS_transport is distilled toward the privileged TransportEncoder (LUPI).
CLS_processing is free, trained only by the processing head task loss.

Fixes applied for attention learning:
  - Pre-norm pattern (LayerNorm before attention) for stable gradient flow
  - Low attention dropout (0.05) independent of trunk dropout
  - Haversine stats gated by learned sigmoid to prevent drowning CLS signal
  - Attention entropy returned for diagnostic logging and regularization
  - Missing embeddings detached in proxy output to create gradient pressure

Reference: Decisions 4, 11 from notes/carbon_model_discuss.md
"""

import math

import torch
import torch.nn as nn
import torch.nn.functional as F

from model.carbon_footprint.src.utils.config import CarbonConfig

# Low dropout for the attention layer itself. Trunk dropout (0.20) is far
# too aggressive for a single-layer attention over 5-40 tokens -- it kills
# gradient signal through the attention weights during early training.
_ATTN_DROPOUT = 0.05


def _attn_entropy(weights: torch.Tensor, mask: torch.Tensor) -> torch.Tensor:
    """Mean entropy of attention distributions over valid (non-padded) tokens.

    Args:
        weights: [B, heads, seq, seq] attention weights from MHA.
        mask: [B, seq] True where token is valid (not padded).

    Returns:
        Scalar mean entropy across batch and heads (CLS rows only).
    """
    # We only care about the CLS rows (indices 0 and 1) attending to tokens.
    cls_weights = weights[:, :, :2, :]          # [B, H, 2, seq]

    # Mask out padding positions for clean entropy calculation
    pad_mask = mask.unsqueeze(1).unsqueeze(2)   # [B, 1, 1, seq]
    # Replace padded positions with zero probability (already near-zero from
    # key_padding_mask, but be explicit)
    cls_weights = cls_weights * pad_mask.float()
    # Re-normalize over valid positions
    denom = cls_weights.sum(dim=-1, keepdim=True).clamp(min=1e-8)
    cls_weights = cls_weights / denom

    # Shannon entropy: -sum(p * log(p)), with 0*log(0)=0
    log_w = torch.log(cls_weights + 1e-8)
    entropy = -(cls_weights * log_w).sum(dim=-1)  # [B, H, 2]
    return entropy.mean()


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
        (proxy_transport [B, D], proxy_processing [B, D], attn_entropy scalar)

    When all tokens are masked, returns learned missing embeddings and
    zero entropy.
    """

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        D = config.step_loc_out
        self.step_emb = nn.Embedding(config.vocab_steps, config.step_emb)
        token_in_dim = config.step_emb + config.coord_dim  # emb + 4

        self.token_mlp = nn.Sequential(
            nn.Linear(token_in_dim, D),
            nn.GELU(),
        )

        # Dual CLS tokens (Decision 11): init with slightly larger scale
        # so they are distinguishable from each other and from token MLP
        # output at initialization. Orthogonal-ish init via randn * 0.1.
        self.cls_transport = nn.Parameter(torch.randn(1, 1, D) * 0.1)
        self.cls_processing = nn.Parameter(torch.randn(1, 1, D) * 0.1)

        # Pre-norm: normalize tokens BEFORE attention so Q/K dot products
        # start in a well-scaled regime. This is the single most important
        # fix -- without it, the softmax sees near-uniform dot products and
        # produces uniform attention weights that carry no gradient signal.
        self.pre_norm = nn.LayerNorm(D)

        # 1-layer self-attention over [CLS_t, CLS_p, tok_1, ..., tok_N]
        # Uses dedicated low dropout, NOT trunk_dropout.
        self.self_attn = nn.MultiheadAttention(
            D,
            config.step_loc_attn_heads,
            dropout=_ATTN_DROPOUT,
            batch_first=True,
        )
        self.post_norm = nn.LayerNorm(D)

        # Haversine gate: learned sigmoid controls how much the haversine
        # stats contribute vs the CLS attention output. Without this gate,
        # the projection MLP can route everything through the 3 scalar stats
        # and ignore the CLS output entirely, making attention useless.
        self.haversine_gate = nn.Sequential(
            nn.Linear(3, D),
            nn.Sigmoid(),
        )
        self.haversine_proj = nn.Linear(3, D)

        # Post-CLS projection now works in D-space (CLS and gated haversine
        # are both D-dimensional, combined via element-wise product + add).
        self.transport_proj = nn.Sequential(
            nn.Linear(D, D),
            nn.GELU(),
        )
        self.processing_proj = nn.Sequential(
            nn.Linear(D, D),
            nn.GELU(),
        )

        # Learned fallbacks when all tokens are masked
        self.missing_transport = nn.Parameter(torch.randn(D) * 0.02)
        self.missing_processing = nn.Parameter(torch.randn(D) * 0.02)

    def forward(
        self,
        step_loc_step_ids: torch.Tensor,
        step_loc_coords: torch.Tensor,
        step_loc_mask: torch.Tensor,
        haversine_sum: torch.Tensor,
        haversine_max: torch.Tensor,
        haversine_mean: torch.Tensor,
    ) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        B, N = step_loc_step_ids.shape
        device = step_loc_step_ids.device

        # Per-token: step_emb || coords -> MLP
        e = self.step_emb(step_loc_step_ids)             # [B, N, step_emb]
        x = torch.cat([e, step_loc_coords], dim=-1)      # [B, N, step_emb+4]
        x = self.token_mlp(x)                             # [B, N, D]

        # Prepend dual CLS tokens
        cls_t = self.cls_transport.expand(B, -1, -1)      # [B, 1, D]
        cls_p = self.cls_processing.expand(B, -1, -1)     # [B, 1, D]
        tokens = torch.cat([cls_t, cls_p, x], dim=1)      # [B, 2+N, D]

        # Build key_padding_mask: True = ignore. CLS tokens always unmasked.
        cls_mask = torch.ones(B, 2, dtype=torch.bool, device=device)
        full_mask = torch.cat([cls_mask, step_loc_mask], dim=1)  # [B, 2+N]
        key_padding_mask = ~full_mask  # True where padded

        # Pre-norm before attention (critical for gradient flow)
        normed = self.pre_norm(tokens)

        # Self-attention with attention weights for entropy diagnostic
        attn_out, attn_weights = self.self_attn(
            normed, normed, normed,
            key_padding_mask=key_padding_mask,
            need_weights=True,
            average_attn_weights=False,  # [B, heads, seq, seq]
        )
        tokens = self.post_norm(tokens + attn_out)  # residual + post-norm

        # Compute attention entropy for logging / regularization
        entropy = _attn_entropy(attn_weights, full_mask)

        # Extract CLS outputs
        cls_transport_out = tokens[:, 0, :]                # [B, D]
        cls_processing_out = tokens[:, 1, :]               # [B, D]

        # Gated haversine fusion: the gate controls per-dimension mixing
        # between CLS attention output and haversine scalar features.
        # This prevents haversine from drowning the attention signal.
        h_stats = torch.stack([
            torch.log1p(haversine_sum),
            torch.log1p(haversine_max),
            torch.log1p(haversine_mean),
        ], dim=-1)                                         # [B, 3]
        h_gate = self.haversine_gate(h_stats)              # [B, D] in (0,1)
        h_val = self.haversine_proj(h_stats)               # [B, D]

        # transport CLS: blend with gated haversine
        fused_t = cls_transport_out + h_gate * h_val
        proxy_transport = self.transport_proj(fused_t)      # [B, D]

        # processing CLS: blend with gated haversine
        fused_p = cls_processing_out + h_gate * h_val
        proxy_processing = self.processing_proj(fused_p)    # [B, D]

        # Handle fully-masked samples (no valid tokens).
        # CRITICAL: detach the missing embeddings so the model cannot
        # reduce loss by making the proxy output mimic the missing embedding.
        # This creates gradient pressure for the proxy to produce something
        # genuinely different (and useful) when tokens are available.
        all_masked = ~step_loc_mask.any(dim=1)             # [B]
        if all_masked.any():
            proxy_transport = torch.where(
                all_masked.unsqueeze(-1),
                self.missing_transport.detach().unsqueeze(0).expand(B, -1),
                proxy_transport,
            )
            proxy_processing = torch.where(
                all_masked.unsqueeze(-1),
                self.missing_processing.detach().unsqueeze(0).expand(B, -1),
                proxy_processing,
            )
            # Zero entropy for fully-masked samples (no attention to measure)
            has_tokens = (~all_masked).float().mean()
            entropy = entropy * has_tokens

        return proxy_transport, proxy_processing, entropy
