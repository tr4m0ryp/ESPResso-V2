"""Cross-attention material-location soft assignment module.

Solves the bipartite assignment problem: materials and step-locations are
unlinked at inference. The user specifies materials and locations separately
but not which material is processed where. This module learns a soft
assignment via cross-attention with Sinkhorn normalization, producing a
transport-relevant feature that augments the transport head.

Architecture:
  1. Project materials and step-locations to common D_assign space.
  2. Cross-attention: materials (query) attend to locations (key/value).
  3. Sinkhorn normalization (K iterations) on attention logits to produce
     approximately doubly-stochastic assignment weights.
  4. Weighted location features per material.
  5. Concatenate with material embeddings, MLP, masked mean pool.

Reference: task specification for MaterialLocAssignment.
"""

import torch
import torch.nn as nn
import torch.nn.functional as F

from model.carbon_footprint.src.utils.config import CarbonConfig


def _sinkhorn(
    log_alpha: torch.Tensor,
    mat_mask: torch.Tensor,
    loc_mask: torch.Tensor,
    iters: int,
) -> torch.Tensor:
    """Sinkhorn normalization on log-space attention logits.

    Produces approximately doubly-stochastic assignment weights:
    each material distributes weight across locations, each location
    receives weight from materials, with row/column sums near 1.0.

    Masked positions get -inf before exponentiation so they contribute
    zero weight. After Sinkhorn, valid rows/cols sum to approximately 1.

    Args:
        log_alpha: [B, M, N] raw attention logits.
        mat_mask: [B, M] True where material is valid.
        loc_mask: [B, N] True where location is valid.
        iters: number of alternating row/column normalization steps.

    Returns:
        [B, M, N] approximately doubly-stochastic assignment weights.
    """
    # Mask invalid positions with -inf so exp(-inf) = 0
    mat_valid = mat_mask.unsqueeze(2).float()   # [B, M, 1]
    loc_valid = loc_mask.unsqueeze(1).float()   # [B, 1, N]
    mask_2d = mat_valid * loc_valid              # [B, M, N]

    # Mask invalid positions with large negative (not -inf to avoid NaN
    # in logsumexp when entire rows/columns are masked during tier masking)
    log_alpha = log_alpha.masked_fill(mask_2d == 0, -1e9)

    for _ in range(iters):
        # Row normalization (across locations for each material)
        row_lse = torch.logsumexp(log_alpha, dim=2, keepdim=True)
        log_alpha = log_alpha - row_lse
        # Column normalization (across materials for each location)
        col_lse = torch.logsumexp(log_alpha, dim=1, keepdim=True)
        log_alpha = log_alpha - col_lse

    # Convert from log-space and zero out masked positions
    weights = log_alpha.exp() * mask_2d
    # Clamp any residual NaN from fully-masked samples to zero
    weights = torch.nan_to_num(weights, nan=0.0)
    return weights


class MaterialLocAssignment(nn.Module):
    """Cross-attention soft assignment between materials and step-locations.

    Input:
        mat_tokens:  [B, M, D_mat]  -- pre-pooled material embeddings
        loc_tokens:  [B, N, D_loc]  -- pre-CLS step-location embeddings
        mat_mask:    [B, M]         -- True where material is valid
        loc_mask:    [B, N]         -- True where location is valid

    Output: [B, assign_out] -- transport-relevant feature from soft assignment.

    When either materials or locations are fully masked for a sample,
    returns a learned missing embedding for that sample.
    """

    def __init__(self, config: CarbonConfig) -> None:
        super().__init__()
        D_a = config.assign_dim

        # Project both modalities to common dimension
        self.mat_proj = nn.Linear(config.material_out, D_a)
        self.loc_proj = nn.Linear(config.step_loc_out, D_a)

        # Scaling factor for dot-product attention
        self.scale = D_a ** -0.5

        self.sinkhorn_iters = config.sinkhorn_iters

        # Output MLP: combine material embedding with assigned location feature
        self.out_mlp = nn.Sequential(
            nn.Linear(config.material_out + D_a, config.assign_out),
            nn.GELU(),
        )

        # Learned fallback for fully-masked samples
        self.missing_assign = nn.Parameter(
            torch.randn(config.assign_out) * 0.02
        )

    def forward(
        self,
        mat_tokens: torch.Tensor,
        loc_tokens: torch.Tensor,
        mat_mask: torch.Tensor,
        loc_mask: torch.Tensor,
    ) -> torch.Tensor:
        B = mat_tokens.shape[0]

        # Project to common space
        mat_q = self.mat_proj(mat_tokens)   # [B, M, D_a]
        loc_k = self.loc_proj(loc_tokens)   # [B, N, D_a]
        loc_v = loc_k                       # share K/V projection

        # Cross-attention logits: materials query, locations key
        logits = torch.bmm(mat_q, loc_k.transpose(1, 2)) * self.scale
        # logits: [B, M, N]

        # Sinkhorn normalization for doubly-stochastic assignment
        weights = _sinkhorn(logits, mat_mask, loc_mask, self.sinkhorn_iters)
        # weights: [B, M, N]

        # Weighted location features per material
        assigned_locs = torch.bmm(weights, loc_v)  # [B, M, D_a]

        # Concatenate material embeddings with assigned location features
        combined = torch.cat([mat_tokens, assigned_locs], dim=-1)
        # combined: [B, M, D_mat + D_a]

        out = self.out_mlp(combined)  # [B, M, assign_out]

        # Zero padding positions before pooling
        out = out * mat_mask.unsqueeze(-1).float()

        # Masked mean pooling over materials
        mask_sum = mat_mask.sum(dim=1, keepdim=True).clamp(min=1).float()
        pooled = out.sum(dim=1) / mask_sum  # [B, assign_out]

        # Replace fully-masked rows (no materials OR no locations) with
        # learned missing embedding
        no_mats = ~mat_mask.any(dim=1)     # [B]
        no_locs = ~loc_mask.any(dim=1)     # [B]
        invalid = no_mats | no_locs        # [B]
        if invalid.any():
            pooled = torch.where(
                invalid.unsqueeze(-1),
                self.missing_assign.unsqueeze(0).expand(B, -1),
                pooled,
            )

        return pooled
