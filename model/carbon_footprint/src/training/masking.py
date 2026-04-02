"""Tier-based masking for the carbon footprint model.

6 tiers (A-F) reflecting real-world supply chain transparency levels.
Each tier cumulatively unlocks feature groups. During training, tiers are
sampled per-sample from a curriculum distribution. During eval, tier can
be forced or defaults to E (full traceability, no privileged data).

Reference: Decisions 12, 21 from notes/carbon_model_discuss.md
Reference: model/water_footprint/src/training/model.py for pattern
"""

from typing import Any, Dict, List, Optional, Set

import torch

# -- Tier feature sets (Decision 12, 21) --
# Each tier cumulatively unlocks feature groups.
_BASE: Set[str] = {"category", "materials", "percentages"}

TIER_FEATURES: Dict[str, Set[str]] = {
    "A": _BASE,
    "B": _BASE | {"weight", "packaging"},
    "C": _BASE | {"weight", "packaging", "steps"},
    "D": _BASE | {"weight", "packaging", "steps", "partial_locations"},
    "E": _BASE | {"weight", "packaging", "steps", "locations"},
    "F": _BASE | {"weight", "packaging", "steps", "locations", "transport"},
}

# Feature groups checked in masking
_GROUPS = (
    "materials", "weight", "packaging", "steps",
    "partial_locations", "locations", "transport",
)


def sample_tiers(tier_probs: Dict[str, float], n: int) -> List[str]:
    """Sample n tier letters from configured probabilities."""
    tiers = list(tier_probs.keys())
    probs = [tier_probs[t] for t in tiers]
    idx = torch.multinomial(torch.tensor(probs), n, replacement=True)
    return [tiers[i] for i in idx]


def avail_mask(
    features_list: List[Set[str]], key: str, device: torch.device,
) -> torch.Tensor:
    """Boolean tensor [B] indicating per-sample availability of a group."""
    return torch.tensor(
        [key in f for f in features_list], device=device, dtype=torch.bool,
    )


def apply_tier_masking(
    batch: Dict[str, torch.Tensor],
    tier_probs: Dict[str, float],
    tier: Optional[str],
    training: bool,
) -> Dict[str, Any]:
    """Resolve tiers and compute per-sample availability masks.

    Args:
        batch: Dict of tensors from CarbonDataset.
        tier_probs: Curriculum tier probabilities.
        tier: Fixed tier (A-F), or None for random (train) / E (eval).
        training: Whether model is in training mode.

    Returns:
        Dict with keys: tiers, feats, avail, has_locations, has_steps,
        has_transport, mask_flags.
    """
    B = batch["category_idx"].shape[0]
    device = batch["category_idx"].device

    if tier is not None:
        tiers = [tier] * B
    elif training:
        tiers = sample_tiers(tier_probs, B)
    else:
        tiers = ["E"] * B

    feats = [TIER_FEATURES[t] for t in tiers]
    avail = {g: avail_mask(feats, g, device) for g in _GROUPS}

    # Derived convenience flags
    has_locations = avail["locations"] | avail["partial_locations"]
    has_steps = (
        avail["steps"] | avail["partial_locations"] | avail["locations"]
    )

    # Mask flags for ProductEncoder: [has_mat, has_steps, has_loc, has_pkg, has_transport]
    mask_flags = torch.stack([
        avail["materials"].float(),
        has_steps.float(),
        has_locations.float(),
        avail["packaging"].float(),
        avail["transport"].float(),
    ], dim=-1)  # [B, 5]

    return {
        "tiers": tiers,
        "feats": feats,
        "avail": avail,
        "has_locations": has_locations,
        "has_steps": has_steps,
        "has_transport": avail["transport"],
        "mask_flags": mask_flags,
    }


def encode_with_fallback(
    emb: torch.Tensor,
    avail: torch.Tensor,
    missing: torch.Tensor,
) -> torch.Tensor:
    """Replace unavailable samples' embeddings with learned missing embedding.

    Args:
        emb: [B, D] encoded output.
        avail: [B] boolean per-sample availability.
        missing: [D] learned missing embedding.

    Returns:
        [B, D] with unavailable rows replaced by missing embedding.
    """
    if avail.all():
        return emb
    exp = missing.unsqueeze(0).expand_as(emb)
    return torch.where(avail.unsqueeze(-1).expand_as(emb), emb, exp)
