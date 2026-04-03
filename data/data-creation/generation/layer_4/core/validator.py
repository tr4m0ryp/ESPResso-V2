"""Validation logic for Layer 4: Packaging Material Estimation."""

import logging
import math
import threading
from collections import Counter
from typing import Any, Dict, List

from ..config.config import Layer4Config
from ..models.models import Layer4Record, ValidationResult

logger = logging.getLogger(__name__)

_EXPECTED_CATEGORIES: List[str] = ["Paper/Cardboard", "Plastic", "Other/Unspecified"]
_EXPECTED_COUNT: int = 3


class PackagingValidator:
    """Validates Layer 4 packaging records and accumulates batch-level statistics."""

    def __init__(self, config: Layer4Config):
        self.config = config
        self._batch_stats = self._init_batch_stats()
        self._stats_lock = threading.Lock()

    # -- public API --------------------------------------------------------

    def validate(self, record: Layer4Record) -> ValidationResult:
        """Run all hard and soft checks. Hard failures set is_valid=False."""
        errors: List[str] = []
        warnings: List[str] = []

        cats = record.packaging_categories
        masses = record.packaging_masses_kg

        # -- hard checks (errors) ------------------------------------------

        # 1. Category count
        if len(cats) != _EXPECTED_COUNT:
            errors.append(
                f"packaging_categories must have exactly {_EXPECTED_COUNT} entries, "
                f"got {len(cats)}"
            )
        else:
            # 2. Category names (only meaningful when count is correct)
            if cats != _EXPECTED_CATEGORIES:
                errors.append(
                    f"packaging_categories must be {_EXPECTED_CATEGORIES} in order, "
                    f"got {cats}"
                )

        # 3. Mass count
        if len(masses) != _EXPECTED_COUNT:
            errors.append(
                f"packaging_masses_kg must have exactly {_EXPECTED_COUNT} entries, "
                f"got {len(masses)}"
            )
        else:
            # 4. Non-negative masses (only meaningful when count is correct)
            negative = [i for i, m in enumerate(masses) if m < 0.0]
            if negative:
                errors.append(
                    f"packaging_masses_kg contains negative values at indices "
                    f"{negative}: {[masses[i] for i in negative]}"
                )

            # 5. At least one positive mass
            if sum(masses) <= 0.0:
                errors.append(
                    "packaging_masses_kg must sum to a positive value; "
                    "all masses are zero"
                )

        # -- soft checks (warnings) ----------------------------------------

        total_mass = sum(masses) if masses else 0.0

        # 6. Packaging mass too low (absolute)
        if total_mass < self.config.min_packaging_mass_kg:
            warnings.append(
                f"Total packaging mass {total_mass:.4f} kg is below "
                f"minimum {self.config.min_packaging_mass_kg} kg"
            )

        # 7. Packaging mass too high (absolute, PEFCR-based)
        max_mass = self.config.max_packaging_mass_kg
        if total_mass > max_mass:
            warnings.append(
                f"Total packaging mass {total_mass:.4f} kg exceeds "
                f"maximum {max_mass} kg"
            )

        # 8. Reasoning length
        reasoning_stripped = record.packaging_reasoning.strip()
        if len(reasoning_stripped) < self.config.min_reasoning_length:
            warnings.append(
                f"packaging_reasoning is too short: {len(reasoning_stripped)} chars "
                f"(minimum {self.config.min_reasoning_length})"
            )

        self._update_batch_stats(record)

        return ValidationResult(
            is_valid=len(errors) == 0,
            errors=errors,
            warnings=warnings,
        )

    def validate_batch_summary(self) -> Dict[str, Any]:
        """Return aggregate statistics over all records validated since last reset.

        Logs warnings for metric values outside expected ranges.
        """
        stats = self._batch_stats
        total = stats["total_records"]

        if total == 0:
            logger.warning("validate_batch_summary called with no records processed")
            return {
                "total_records": 0,
                "records_with_warnings": 0,
                "duplicate_count": 0,
                "duplicate_percentage": 0.0,
                "category_usage": {},
                "mean_packaging_ratio": None,
                "distance_mass_correlation": None,
                "zero_mass_count": 0,
            }

        # Duplicate analysis
        triplet_counts = stats["mass_triplets"]
        duplicate_count = sum(c - 1 for c in triplet_counts.values() if c > 1)
        duplicate_percentage = (duplicate_count / total) * 100.0

        # Category usage: percentage of records where each category mass > 0
        nonzero = stats["category_nonzero_counts"]
        category_usage = {
            _EXPECTED_CATEGORIES[i]: (nonzero[i] / total) * 100.0
            for i in range(_EXPECTED_COUNT)
        }

        # Mean packaging ratio
        ratios = stats["packaging_ratios"]
        mean_packaging_ratio = sum(ratios) / len(ratios) if ratios else None

        # Pearson correlation between total_distance_km and total packaging mass
        distances = stats["distances"]
        total_masses = stats["total_masses"]
        distance_mass_correlation = _pearson(distances, total_masses)

        # Zero-mass records (sum of masses == 0)
        zero_mass_count = sum(
            1 for m in total_masses if m == 0.0
        )

        summary = {
            "total_records": total,
            "records_with_warnings": stats["records_with_warnings"],
            "duplicate_count": duplicate_count,
            "duplicate_percentage": round(duplicate_percentage, 4),
            "category_usage": {k: round(v, 4) for k, v in category_usage.items()},
            "mean_packaging_ratio": (
                round(mean_packaging_ratio, 6) if mean_packaging_ratio is not None else None
            ),
            "distance_mass_correlation": (
                round(distance_mass_correlation, 6)
                if distance_mass_correlation is not None
                else None
            ),
            "zero_mass_count": zero_mass_count,
        }

        # Log warnings for out-of-range batch metrics
        if duplicate_percentage > 5.0:
            logger.warning(
                "High duplicate rate in batch: %.2f%% (%d records)",
                duplicate_percentage,
                duplicate_count,
            )
        if zero_mass_count > 0:
            logger.warning(
                "%d record(s) have zero total packaging mass", zero_mass_count
            )
        if mean_packaging_ratio is not None:
            logger.info(
                "Batch mean packaging ratio: %.6f", mean_packaging_ratio
            )
        if distance_mass_correlation is not None and abs(distance_mass_correlation) < 0.05:
            logger.warning(
                "Near-zero distance/mass correlation (%.4f); "
                "packaging masses may be independent of transport distance",
                distance_mass_correlation,
            )

        return summary

    def reset(self) -> None:
        """Reset batch statistics to initial state."""
        self._batch_stats = self._init_batch_stats()

    # -- private helpers ---------------------------------------------------

    def _init_batch_stats(self) -> Dict[str, Any]:
        """Return a fresh batch statistics accumulator."""
        return {
            "total_records": 0,
            "records_with_warnings": 0,
            "mass_triplets": Counter(),
            "category_nonzero_counts": [0] * _EXPECTED_COUNT,
            "packaging_ratios": [],
            "distances": [],
            "total_masses": [],
        }

    def _update_batch_stats(self, record: Layer4Record) -> None:
        """Update batch statistics with data from a single record (thread-safe)."""
        with self._stats_lock:
            self._update_batch_stats_unlocked(record)

    def _update_batch_stats_unlocked(self, record: Layer4Record) -> None:
        """Non-locking inner implementation of batch stats update."""
        stats = self._batch_stats
        stats["total_records"] += 1

        masses = record.packaging_masses_kg
        total_mass = sum(masses) if masses else 0.0
        product_weight = record.total_weight_kg

        # Mass triplet for duplicate detection (rounded to 4 decimal places)
        if len(masses) == _EXPECTED_COUNT:
            key = tuple(round(m, 4) for m in masses)
            stats["mass_triplets"][key] += 1

        # Per-category nonzero tracking
        for i in range(_EXPECTED_COUNT):
            if i < len(masses) and masses[i] > 0.0:
                stats["category_nonzero_counts"][i] += 1

        # Packaging ratio
        if product_weight > 0.0:
            stats["packaging_ratios"].append(total_mass / product_weight)

        # Distance and mass lists for correlation
        stats["distances"].append(record.total_distance_km)
        stats["total_masses"].append(total_mass)

        # Record has warnings if any soft check fires
        has_warning = False
        if (
            total_mass < self.config.min_packaging_mass_kg
            or total_mass > self.config.max_packaging_mass_kg
        ):
            has_warning = True
        if len(record.packaging_reasoning.strip()) < self.config.min_reasoning_length:
            has_warning = True
        if has_warning:
            stats["records_with_warnings"] += 1


# -- module-level utility --------------------------------------------------


def _pearson(x: List[float], y: List[float]) -> float | None:
    """Pearson correlation coefficient. None when undefined (n<2 or zero variance)."""
    n = len(x)
    if n != len(y) or n < 2:
        return None

    sum_x = sum(x)
    sum_y = sum(y)
    sum_xy = sum(xi * yi for xi, yi in zip(x, y))
    sum_x2 = sum(xi * xi for xi in x)
    sum_y2 = sum(yi * yi for yi in y)

    numerator = n * sum_xy - sum_x * sum_y
    denom_sq = (n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2)

    if denom_sq <= 0.0:
        return None

    return numerator / math.sqrt(denom_sq)
