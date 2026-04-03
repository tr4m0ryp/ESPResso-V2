"""
Outlier detector for Layer 5 statistical validation.

Detects statistical outliers in weight, packaging mass, and transport
distance using z-score analysis. Packaging uses absolute mass (not
ratio to product weight), since PEFCR baselines produce per-piece
packaging ranges that naturally vary by garment type.
"""

import statistics
from typing import Any, Dict, List, Optional

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import CompleteProductRecord


class OutlierDetector:
    """Z-score based outlier detection on running statistics."""

    def __init__(self, config: Layer5Config):
        self.config = config
        self.weight_values: List[float] = []
        self.packaging_masses: List[float] = []
        self.transport_distances: List[float] = []

    def check_outliers(
        self, record: CompleteProductRecord
    ) -> Dict[str, Any]:
        """Check for statistical outliers using configurable sigma thresholds."""
        outlier_type: Optional[str] = None
        is_outlier = False

        # Weight outlier detection
        if len(self.weight_values) >= 10:
            weight_mean = statistics.mean(self.weight_values)
            weight_stdev = statistics.stdev(self.weight_values)

            if weight_stdev > 0:
                z_score = (
                    abs(record.total_weight_kg - weight_mean) / weight_stdev
                )
                if z_score > self.config.outlier_weight_sigma:
                    outlier_type = "weight"
                    is_outlier = True

        # Packaging mass outlier detection (absolute mass, not ratio)
        if len(self.packaging_masses) >= 10:
            pkg_mean = statistics.mean(self.packaging_masses)
            pkg_stdev = statistics.stdev(self.packaging_masses)

            if pkg_stdev > 0:
                z_score = (
                    abs(record.total_packaging_mass_kg - pkg_mean)
                    / pkg_stdev
                )
                if z_score > self.config.outlier_ratio_sigma:
                    if not is_outlier:
                        outlier_type = "packaging_mass"
                        is_outlier = True

        # Transport distance outlier detection
        if len(self.transport_distances) >= 10:
            distance_mean = statistics.mean(self.transport_distances)
            distance_stdev = statistics.stdev(self.transport_distances)

            if distance_stdev > 0:
                z_score = (
                    abs(record.total_transport_distance_km - distance_mean)
                    / distance_stdev
                )
                if z_score > self.config.outlier_transport_sigma:
                    if not is_outlier:
                        outlier_type = "transport_distance"
                        is_outlier = True

        return {
            "is_outlier": is_outlier,
            "outlier_type": outlier_type,
        }

    def update(self, record: CompleteProductRecord) -> None:
        """Update tracking data with a processed record."""
        self.weight_values.append(record.total_weight_kg)
        self.packaging_masses.append(record.total_packaging_mass_kg)
        self.transport_distances.append(record.total_transport_distance_km)

    def reset(self) -> None:
        """Reset outlier tracking state."""
        self.weight_values = []
        self.packaging_masses = []
        self.transport_distances = []
