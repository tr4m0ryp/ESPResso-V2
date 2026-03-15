"""
Statistical Validator for Layer 5: Stage 3 Statistical Quality (V2).

Performs distribution monitoring, deduplication, outlier detection,
and cross-layer correlation checks on the complete dataset. Runs
without LLM calls -- pure statistical analysis on running state.
"""

import json
import logging
import hashlib
import math
import statistics
from typing import Dict, List, Optional, Any, Set, Tuple
from collections import Counter

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import (
    StatisticalQualityResult,
    CompleteProductRecord,
)

logger = logging.getLogger(__name__)


class StatisticalValidator:
    """Performs statistical analysis, deduplication, and outlier detection."""

    def __init__(self, config: Layer5Config):
        self.config = config
        self._initialize_tracking_data()

    def _initialize_tracking_data(self) -> None:
        """Initialize data structures for statistical tracking."""
        # For deduplication
        self.record_hashes: Set[str] = set()
        self.exact_duplicates: int = 0
        self.near_duplicates: int = 0

        # For distribution monitoring
        self.material_counts: Counter = Counter()
        self.category_counts: Counter = Counter()
        self.transport_type_counts: Counter = Counter()
        self.packaging_category_counts: Counter = Counter()

        # For outlier detection
        self.weight_values: List[float] = []
        self.packaging_ratios: List[float] = []
        self.transport_distances: List[float] = []

        # Statistical summaries
        self.weight_stats: Dict[str, float] = {}
        self.packaging_ratio_stats: Dict[str, float] = {}
        self.transport_distance_stats: Dict[str, float] = {}

        # For cross-layer correlation checks
        self.weight_packaging_pairs: List[Tuple[float, float]] = []
        self.material_transport_pairs: List[Tuple[str, float]] = []

    def validate_record(
        self, record: CompleteProductRecord
    ) -> StatisticalQualityResult:
        """
        Perform statistical validation on a single record.

        Args:
            record: Complete product record to validate

        Returns:
            StatisticalQualityResult with all check outcomes
        """
        try:
            # Deduplication check
            duplicate_check = self._check_duplicates(record)

            # Distribution monitoring
            distribution_check = self._check_distributions(record)

            # Outlier detection
            outlier_check = self._check_outliers(record)

            # Cross-layer correlation checks
            wp_ok, wp_issues = self._check_weight_packaging_correlation(
                record
            )
            mt_ok, mt_issues = self._check_material_transport_correlation(
                record
            )

            # Update tracking data (after checks, so current record
            # does not influence its own check)
            self._update_tracking_data(record)

            # Merge all distribution issues
            all_issues = (
                distribution_check["issues"] + wp_issues + mt_issues
            )

            result = StatisticalQualityResult(
                is_duplicate=duplicate_check["is_duplicate"],
                duplicate_similarity=duplicate_check["similarity"],
                is_outlier=outlier_check["is_outlier"],
                outlier_type=outlier_check["outlier_type"],
                material_distribution_ok=distribution_check["material_ok"],
                category_distribution_ok=distribution_check["category_ok"],
                transport_distribution_ok=distribution_check["transport_ok"],
                packaging_distribution_ok=distribution_check["packaging_ok"],
                weight_packaging_correlation_ok=wp_ok,
                material_transport_correlation_ok=mt_ok,
                distribution_issues=all_issues,
            )

            logger.debug(
                "Statistical validation completed for %s: "
                "duplicate=%s, outlier=%s",
                record.subcategory_name,
                result.is_duplicate,
                result.is_outlier,
            )

            return result

        except Exception as e:
            logger.error(
                "Statistical validation failed for %s: %s",
                record.subcategory_id,
                e,
            )
            return StatisticalQualityResult(
                is_duplicate=False,
                duplicate_similarity=0.0,
                is_outlier=False,
                outlier_type=None,
                material_distribution_ok=True,
                category_distribution_ok=True,
                transport_distribution_ok=True,
                packaging_distribution_ok=True,
                weight_packaging_correlation_ok=True,
                material_transport_correlation_ok=True,
                distribution_issues=[
                    f"Statistical validation error: {str(e)}"
                ],
            )

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def _check_duplicates(
        self, record: CompleteProductRecord
    ) -> Dict[str, Any]:
        """Check for exact and near duplicates."""
        record_hash = self._compute_record_hash(record)

        # Exact duplicate
        if record_hash in self.record_hashes:
            self.exact_duplicates += 1
            return {
                "is_duplicate": True,
                "similarity": 1.0,
                "duplicate_type": "exact",
            }

        # Near duplicate via similarity metric
        similarity = self._compute_similarity_score(record, record_hash)

        if similarity >= self.config.dedup_similarity_threshold:
            self.near_duplicates += 1
            return {
                "is_duplicate": True,
                "similarity": similarity,
                "duplicate_type": "near",
            }

        # New unique record
        self.record_hashes.add(record_hash)

        return {
            "is_duplicate": False,
            "similarity": similarity,
            "duplicate_type": None,
        }

    def _compute_record_hash(
        self, record: CompleteProductRecord
    ) -> str:
        """Create MD5 hash for deduplication detection."""
        key_parts = [
            record.subcategory_id,
            tuple(sorted(record.materials)),
            round(record.total_weight_kg, 1),
            tuple(sorted(record.preprocessing_steps)),
            round(record.total_transport_distance_km, -2),
            tuple(sorted(record.packaging_categories)),
        ]

        key_str = json.dumps(key_parts, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _compute_similarity_score(
        self, record: CompleteProductRecord, record_hash: str
    ) -> float:
        """Compute similarity score against existing records.

        Currently returns 0 for new records (hash-based dedup handles
        exact matches). Could be extended with Jaccard similarity on
        materials, weighted distance on numerics, etc.
        """
        return 0.0

    # ------------------------------------------------------------------
    # Distribution monitoring
    # ------------------------------------------------------------------

    def _check_distributions(
        self, record: CompleteProductRecord
    ) -> Dict[str, Any]:
        """Check distribution constraints and coverage."""
        issues: List[str] = []

        material_ok = self._check_material_distribution(record, issues)
        category_ok = self._check_category_distribution(record, issues)
        transport_ok = self._check_transport_distribution(record, issues)
        packaging_ok = self._check_packaging_distribution(record, issues)

        return {
            "material_ok": material_ok,
            "category_ok": category_ok,
            "transport_ok": transport_ok,
            "packaging_ok": packaging_ok,
            "issues": issues,
        }

    def _check_material_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check material distribution constraints."""
        for material in record.materials:
            self.material_counts[material.lower()] += 1

        total_materials = sum(self.material_counts.values())
        if total_materials > 0:
            max_pct = self.config.max_single_material_pct
            for material, count in self.material_counts.items():
                percentage = count / total_materials
                if percentage > max_pct:
                    issues.append(
                        f"Material '{material}' over-represented: "
                        f"{percentage:.1%}"
                    )
                    return False

        return True

    def _check_category_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check category distribution constraints."""
        self.category_counts[record.subcategory_id] += 1
        return True

    def _check_transport_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check transport distribution constraints."""
        self.transport_type_counts[record.supply_chain_type] += 1

        total_transport = sum(self.transport_type_counts.values())
        if total_transport > 100:
            for transport_type, count in self.transport_type_counts.items():
                percentage = count / total_transport
                # Flag any single transport type exceeding 80%
                if percentage > 0.80:
                    issues.append(
                        f"Transport type '{transport_type}' "
                        f"over-represented: {percentage:.1%}"
                    )
                    return False

        return True

    def _check_packaging_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check packaging distribution constraints."""
        for category in record.packaging_categories:
            self.packaging_category_counts[category] += 1

        total_packaging = sum(self.packaging_category_counts.values())
        if total_packaging > 100:
            for category, count in self.packaging_category_counts.items():
                percentage = count / total_packaging
                # Flag any single packaging category exceeding 80%
                if percentage > 0.80:
                    issues.append(
                        f"Packaging category '{category}' "
                        f"over-represented: {percentage:.1%}"
                    )
                    return False

        return True

    # ------------------------------------------------------------------
    # Outlier detection
    # ------------------------------------------------------------------

    def _check_outliers(
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

        # Packaging ratio outlier detection
        if record.total_weight_kg > 0 and len(self.packaging_ratios) >= 10:
            current_ratio = (
                record.total_packaging_mass_kg / record.total_weight_kg
            )
            ratio_mean = statistics.mean(self.packaging_ratios)
            ratio_stdev = statistics.stdev(self.packaging_ratios)

            if ratio_stdev > 0:
                z_score = abs(current_ratio - ratio_mean) / ratio_stdev
                if z_score > self.config.outlier_ratio_sigma:
                    if not is_outlier:
                        outlier_type = "packaging_ratio"
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

    # ------------------------------------------------------------------
    # Cross-layer correlation checks
    # ------------------------------------------------------------------

    def _check_weight_packaging_correlation(
        self, record: CompleteProductRecord
    ) -> Tuple[bool, List[str]]:
        """Check that heavier products have proportionally more packaging.

        After 100+ records, compute Pearson correlation between product
        weight and packaging mass. Flag if correlation is negative or
        below 0.1 (products should have positive correlation: heavier
        products generally require more packaging).

        Returns:
            Tuple of (is_ok, list of issues)
        """
        if len(self.weight_packaging_pairs) < 100:
            return True, []

        weights = [w for w, _ in self.weight_packaging_pairs]
        pkg_masses = [p for _, p in self.weight_packaging_pairs]

        corr = self._pearson_correlation(weights, pkg_masses)
        if corr is None:
            # Cannot compute (zero variance); treat as ok
            return True, []

        if corr < 0.1:
            issue = (
                f"Weight-packaging correlation is {corr:.3f} "
                f"(expected >= 0.1); heavier products should "
                f"have more packaging"
            )
            logger.warning(issue)
            return False, [issue]

        return True, []

    def _check_material_transport_correlation(
        self, record: CompleteProductRecord
    ) -> Tuple[bool, List[str]]:
        """Check that exotic/specialty materials correlate with longer transport.

        Track material rarity (how many records use each material). After
        100+ records, check if records with rare materials (used in <5%
        of records) tend to have longer transport distances than the
        median. This is a soft check -- produces warnings, not hard
        failures.

        Returns:
            Tuple of (is_ok, list of issues)
        """
        if len(self.material_transport_pairs) < 100:
            return True, []

        # Determine total record count from transport distances tracked
        total_records = len(self.transport_distances)
        if total_records == 0:
            return True, []

        # Identify rare materials: those appearing in <5% of records
        rarity_threshold = 0.05
        rare_materials: set = set()
        for material, count in self.material_counts.items():
            if count / total_records < rarity_threshold:
                rare_materials.add(material)

        if not rare_materials:
            return True, []

        # Collect transport distances for records that contain at least
        # one rare material
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
            logger.warning(issue)
            issues.append(issue)

        # Soft check: always return True so it only warns
        return True, issues

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

    # ------------------------------------------------------------------
    # Tracking data updates
    # ------------------------------------------------------------------

    def _update_tracking_data(
        self, record: CompleteProductRecord
    ) -> None:
        """Update tracking data for statistical analysis."""
        # Weight for outlier detection
        self.weight_values.append(record.total_weight_kg)

        # Packaging ratio for outlier detection
        if record.total_weight_kg > 0:
            packaging_ratio = (
                record.total_packaging_mass_kg / record.total_weight_kg
            )
            self.packaging_ratios.append(packaging_ratio)

        # Transport distance for outlier detection
        self.transport_distances.append(record.total_transport_distance_km)

        # Cross-layer correlation: weight vs packaging mass
        self.weight_packaging_pairs.append(
            (record.total_weight_kg, record.total_packaging_mass_kg)
        )

        # Cross-layer correlation: material vs transport distance
        # Store one entry per material in the record so that per-material
        # rarity can be assessed
        for material in record.materials:
            self.material_transport_pairs.append(
                (material, record.total_transport_distance_km)
            )

    # ------------------------------------------------------------------
    # Summary and reset
    # ------------------------------------------------------------------

    def get_statistical_summary(self) -> Dict[str, Any]:
        """Get summary of statistical validation results."""
        summary: Dict[str, Any] = {
            "total_records_checked": len(self.record_hashes),
            "exact_duplicates_found": self.exact_duplicates,
            "near_duplicates_found": self.near_duplicates,
            "unique_records": (
                len(self.record_hashes) - self.exact_duplicates
            ),
            "material_coverage": len(self.material_counts),
            "category_coverage": len(self.category_counts),
            "transport_type_coverage": len(self.transport_type_counts),
            "packaging_category_coverage": len(
                self.packaging_category_counts
            ),
            "weight_statistics": self._compute_statistics(
                self.weight_values
            ),
            "packaging_ratio_statistics": self._compute_statistics(
                self.packaging_ratios
            ),
            "transport_distance_statistics": self._compute_statistics(
                self.transport_distances
            ),
        }

        # Cross-layer correlation statistics
        if len(self.weight_packaging_pairs) >= 2:
            weights = [w for w, _ in self.weight_packaging_pairs]
            pkg_masses = [p for _, p in self.weight_packaging_pairs]
            wp_corr = self._pearson_correlation(weights, pkg_masses)
            summary["weight_packaging_correlation"] = wp_corr
        else:
            summary["weight_packaging_correlation"] = None

        summary["weight_packaging_pairs_count"] = len(
            self.weight_packaging_pairs
        )
        summary["material_transport_pairs_count"] = len(
            self.material_transport_pairs
        )

        return summary

    def _compute_statistics(
        self, values: List[float]
    ) -> Dict[str, float]:
        """Compute basic statistics for a list of values."""
        if not values:
            return {}

        return {
            "count": len(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "stdev": (
                statistics.stdev(values) if len(values) > 1 else 0.0
            ),
            "min": min(values),
            "max": max(values),
        }

    def reset_statistical_tracking(self) -> None:
        """Reset all statistical tracking data."""
        self._initialize_tracking_data()
        logger.info("Reset statistical tracking data")
