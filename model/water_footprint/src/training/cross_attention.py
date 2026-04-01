"""Cross-attention and confidence gate for geographic location matching.

CrossAttentionModule: multi-head cross-attention where queries (materials
or steps) attend to keys (locations). Returns attended output and max
attention scores indicating match quality.

ConfidenceGate: sigmoid MLP that blends cross-attention output with a
learned prior embedding based on match quality. When no locations are
available, the gate approaches 0 and falls back entirely to the prior.

GeoAttentionBlock: convenience wrapper composing CrossAttentionModule +
ConfidenceGate + nn.Embedding prior for use in WA1Model.
"""

import math

import torch
import torch.nn as nn
import torch.nn.functional as F


class CrossAttentionModule(nn.Module):
    """Multi-head cross-attention: queries attend to keys/values.

    Args:
        d_model: Model dimension (default 32).
        n_heads: Number of attention heads (default 2).
        dropout: Dropout on attention weights (default 0.15).
    """

    def __init__(self, d_model: int = 32, n_heads: int = 2,
                 dropout: float = 0.15) -> None:
        super().__init__()
        assert d_model % n_heads == 0, "d_model must be divisible by n_heads"
        self.d_model = d_model
        self.n_heads = n_heads
        self.d_k = d_model // n_heads

        self.w_q = nn.Linear(d_model, d_model)
        self.w_k = nn.Linear(d_model, d_model)
        self.w_v = nn.Linear(d_model, d_model)
        self.w_o = nn.Linear(d_model, d_model)
        self.attn_dropout = nn.Dropout(dropout)

    def forward(
        self, queries: torch.Tensor, keys: torch.Tensor,
        values: torch.Tensor, key_mask: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Compute multi-head cross-attention with masked keys.

        Args:
            queries: [B, N, d_model] query embeddings.
            keys: [B, K, d_model] key embeddings.
            values: [B, K, d_model] value embeddings.
            key_mask: [B, K] boolean mask (True=real, False=padding).

        Returns:
            attended: [B, N, d_model] attended output.
            max_attn_scores: [B, N] max attention weight per query,
                indicating match quality. Zero when all keys are masked.
        """
        B, N, _ = queries.shape
        K = keys.shape[1]

        # Project and reshape to [B, heads, seq, d_k]
        q = self._reshape_heads(self.w_q(queries), B, N)
        k = self._reshape_heads(self.w_k(keys), B, K)
        v = self._reshape_heads(self.w_v(values), B, K)

        # Scaled dot-product attention: [B, heads, N, K]
        scores = torch.matmul(q, k.transpose(-2, -1)) / math.sqrt(self.d_k)

        # Expand key_mask to [B, 1, 1, K] for broadcasting
        mask_expanded = key_mask.unsqueeze(1).unsqueeze(2)  # [B, 1, 1, K]

        # Check which batches have at least one real key
        any_real = key_mask.any(dim=-1)  # [B]

        # Mask padding positions to -inf; if ALL keys masked, use 0 instead
        # to avoid NaN from softmax(-inf, ..., -inf)
        scores = scores.masked_fill(~mask_expanded, float("-inf"))

        # For batches with all keys masked, replace scores with zeros
        # so softmax produces uniform (then we zero out the output)
        all_masked = ~any_real  # [B]
        if all_masked.any():
            scores[all_masked] = 0.0

        attn_weights = F.softmax(scores, dim=-1)  # [B, heads, N, K]
        attn_weights = self.attn_dropout(attn_weights)

        # Zero out attention for all-masked batches (no real keys to attend to)
        if all_masked.any():
            attn_weights[all_masked] = 0.0

        # Weighted sum: [B, heads, N, d_k]
        context = torch.matmul(attn_weights, v)

        # Reshape back: [B, N, d_model]
        context = context.transpose(1, 2).contiguous().view(B, N, self.d_model)
        attended = self.w_o(context)

        # Max attention score per query: mean over heads, then max over keys
        # attn_weights: [B, heads, N, K] -> mean over heads -> [B, N, K]
        mean_weights = attn_weights.mean(dim=1)
        max_attn_scores = mean_weights.max(dim=-1).values  # [B, N]

        # Ensure all-masked batches produce zero scores
        if all_masked.any():
            max_attn_scores[all_masked] = 0.0

        return attended, max_attn_scores

    def _reshape_heads(self, x: torch.Tensor, B: int,
                       seq_len: int) -> torch.Tensor:
        """Reshape [B, seq, d_model] to [B, heads, seq, d_k]."""
        return x.view(B, seq_len, self.n_heads, self.d_k).transpose(1, 2)


class ConfidenceGate(nn.Module):
    """Sigmoid MLP gate blending cross-attention output with a prior.

    When match quality (max_scores) is high, gate approaches 1 and the
    cross-attention output dominates. When max_scores are zero (no
    locations available), gate approaches 0 and the prior dominates.

    Args:
        d_model: Model dimension (default 32).
        hidden: Hidden layer size for the gate MLP (default 8).
    """

    def __init__(self, d_model: int = 32, hidden: int = 8) -> None:
        super().__init__()
        # Input: d_model (cross_attn_output) + 1 (max_score) = d_model + 1
        self.gate_mlp = nn.Sequential(
            nn.Linear(d_model + 1, hidden),
            nn.GELU(),
            nn.Linear(hidden, 1),
            nn.Sigmoid(),
        )
        # Initialize final bias negative so gate starts near 0 (prior-heavy)
        # This ensures untrained model defaults to prior, which is safer
        nn.init.constant_(self.gate_mlp[2].bias, -2.0)

    def forward(
        self, cross_attn_output: torch.Tensor, max_scores: torch.Tensor,
        prior: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Blend cross-attention output with prior using learned gate.

        Args:
            cross_attn_output: [B, N, d_model] from CrossAttentionModule.
            max_scores: [B, N] max attention weights from CrossAttentionModule.
            prior: [B, N, d_model] learned prior embeddings.

        Returns:
            blended: [B, N, d_model] gated combination.
            gate_values: [B, N, 1] gate activations for monitoring.
        """
        # Concatenate cross-attn output with max_score indicator
        gate_input = torch.cat(
            [cross_attn_output, max_scores.unsqueeze(-1)], dim=-1
        )  # [B, N, d_model + 1]

        gate_values = self.gate_mlp(gate_input)  # [B, N, 1]
        blended = gate_values * cross_attn_output + (1 - gate_values) * prior
        return blended, gate_values


class GeoAttentionBlock(nn.Module):
    """Cross-attention block with prior fallback for geographic matching.

    Wraps CrossAttentionModule + ConfidenceGate + nn.Embedding prior.
    Used twice in WA1: once for materials, once for processing steps.
    Both instances share the same location keys.

    Args:
        d_model: Model dimension.
        n_heads: Number of attention heads.
        n_vocab: Vocabulary size for the prior embedding.
        dropout: Attention dropout rate.
        gate_hidden: Hidden dim for confidence gate MLP.
    """

    def __init__(self, d_model: int, n_heads: int, n_vocab: int,
                 dropout: float, gate_hidden: int = 8) -> None:
        super().__init__()
        self.cross_attn = CrossAttentionModule(d_model, n_heads, dropout)
        self.gate = ConfidenceGate(d_model, gate_hidden)
        self.prior = nn.Embedding(n_vocab, d_model)

    def forward(
        self, queries: torch.Tensor, query_ids: torch.Tensor,
        keys: torch.Tensor, key_mask: torch.Tensor
    ) -> tuple[torch.Tensor, torch.Tensor]:
        """Run cross-attention with prior fallback.

        Args:
            queries: [B, N, d_model] from encoder.
            query_ids: [B, N] vocabulary indices for prior lookup.
            keys: [B, K, d_model] from location encoder (used as both K, V).
            key_mask: [B, K] boolean mask (True=real, False=padding).

        Returns:
            output: [B, N, d_model] gated combination.
            gate_vals: [B, N, 1] gate activations for monitoring.
        """
        attended, max_scores = self.cross_attn(queries, keys, keys, key_mask)
        prior_emb = self.prior(query_ids)
        output, gate_vals = self.gate(attended, max_scores, prior_emb)
        return output, gate_vals
