"""Curriculum learning schedule for tier probability interpolation.

Linearly interpolates from curriculum_start_probs (easy, F-heavy) to the
target tier_probs over curriculum_warmup_epochs. After warmup, returns
the target distribution unchanged.
"""

from typing import Dict

from model.carbon_footprint.src.utils.config import CarbonConfig


def curriculum_tier_probs(config: CarbonConfig, epoch: int) -> Dict[str, float]:
    """Compute interpolated tier probabilities for the given epoch.

    During warmup, linearly blends start_probs toward target tier_probs.
    After warmup (or if warmup <= 0), returns tier_probs directly.
    """
    warmup = config.curriculum_warmup_epochs
    if warmup <= 0 or epoch >= warmup:
        return config.tier_probs
    alpha = epoch / warmup
    start = config.curriculum_start_probs
    target = config.tier_probs
    return {t: start[t] * (1 - alpha) + target[t] * alpha for t in target}
