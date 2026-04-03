"""
Correlation analyzer for Layer 5 statistical validation.

Checks cross-layer correlations: weight-packaging mass correlation
and material-rarity vs transport distance.
"""

import logging
import math
import statistics
from collections import Counter
from typing import Dict, List, Optional, Tuple

from data.data_generation.layer_5.models.models import CompleteProductRecord

logger = logging.getLogger(__name__)


class CorrelationAnalyzer:
    """Cross-layer correlation analysis."""

    def __init__(self):
        self.weight_packaging_pairs: List[Tuple[float, float]] = []
        self.material_transport_pairs: List[Tuple[str, float]] = []

    def check_weight_packaging_correlation(
        self,
        record: CompleteProductRecord,
        material_counts: Counter,
        transport_distances: List[float],
    ) -> Tuple[bool, List[str]]:
        """Check that heavier products have proportionally more packaging.

        After 100+ records, compute Pearson correlation between product
        weight and packaging mass. Flag if correlation is negative or
        below 0.1 (products should have positive correlation: heavier
        products generally require more packaging).
        """
        if len(self.weight_packaging_pairs) < 100:
            return True, []

        weights = [w for w, _ in self.weight_packaging_pairs]
        pkg_masses = [p for _, p in self.weight_packaging_pairs]

        corr = self._pearson_correlation(weights, pkg_masses)
        if corr is None:
            return True, []

        if corr < 0.1:
            issue = (
                f"Weight-packaging correlation is {corr:.3f} "
                f"(expected >= 0.1); heavier products should "
                f"have more packaging"
            )
            logger.debug(issue)
            return False, [issue]

        return True, []

    def check_material_transport_correlation(
        self,
        record: CompleteProductRecord,
        material_counts: Counter,
        transport_distances: List[float],
    ) -> Tuple[bool, List[str]]:
        """Check that exotic/specialty materials correlate with longer transport.

        Track material rarity (how many records use each material). After
        100+ records, check if records with rare materials (used in <5%
        of records) tend to have longer transport distances than the
        median. This is a soft check -- produces warnings, not hard
        failures.
        """
        if len(self.material_transport_pairs) < 100:
            return True, []

        total_records = len(transport_distances)
        if total_records == 0:
            return True, []

        # Identify rare materials: those appearing in <5% of records
        rarity_threshold = 0.05
        rare_materials: set = set()
        for material, count in material_counts.items():
            if count / total_records < rarity_threshold:
                rare_materials.add(material)

        if not rare_materials:
            return True, []

        # Collect transport distances for records with rare materials
        rare_distances: List[float] = []
        all_distances: List[float] = []
        for material, distance in self.material_transport_pairs:
            all_distances.append(distance)
            if material.lower() in rare_materials:
                rare_distances.append(distance)

        if not rare_distances or not all_distances:
            return True, []

        median_distance = statistics.median(all_distances)
        rare_median = statistics.median(rare_distances)

        issues: List[str] = []
        if rare_median < median_distance:
            issue = (
                f"Rare-material transport median ({rare_median:.0f} km) "
                f"is below overall median ({median_distance:.0f} km); "
                f"specialty materials typically travel farther"
            )
            logger.debug(issue)
            issues.append(issue)

        # Soft check: always return True so it only warns
        return True, issues

    def update(self, record: CompleteProductRecord) -> None:
        """Update correlation tracking data with a processed record."""
        self.weight_packaging_pairs.append(
            (record.total_weight_kg, record.total_packaging_mass_kg)
        )
        for material in record.materials:
            self.material_transport_pairs.append(
                (material, record.total_transport_distance_km)
            )

    @staticmethod
    def _pearson_correlation(
        xs: List[float], ys: List[float]
    ) -> Optional[float]:
        """Compute Pearson correlation coefficient between two sequences.

        Returns None if either sequence has zero variance.
        """
        n = len(xs)
        if n < 2:
            return None

        mean_x = sum(xs) / n
        mean_y = sum(ys) / n

        numerator = sum(
            (x - mean_x) * (y - mean_y) for x, y in zip(xs, ys)
        )
        denom_x = math.sqrt(sum((x - mean_x) ** 2 for x in xs))
        denom_y = math.sqrt(sum((y - mean_y) ** 2 for y in ys))

        if denom_x == 0 or denom_y == 0:
            return None

        return numerator / (denom_x * denom_y)

    def reset(self) -> None:
        """Reset correlation tracking state."""
        self.weight_packaging_pairs = []
        self.material_transport_pairs = []
