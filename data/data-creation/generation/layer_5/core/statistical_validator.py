"""
Statistical Validator for Layer 5: Stage 3 Statistical Quality (V2).

Performs distribution monitoring, deduplication, outlier detection,
and cross-layer correlation checks on the complete dataset. Runs
without LLM calls -- pure statistical analysis on running state.

Delegates to specialized sub-modules:
    - DedupChecker: hash-based duplicate detection
    - DistributionChecker: material/category/transport/packaging coverage
    - OutlierDetector: z-score outlier detection
    - CorrelationAnalyzer: cross-layer correlation checks
"""

import logging
import statistics
import threading
from typing import Any, Dict, List

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.core.correlation_analyzer import CorrelationAnalyzer
from data.data_generation.layer_5.core.dedup_checker import DedupChecker
from data.data_generation.layer_5.core.distribution_checker import DistributionChecker
from data.data_generation.layer_5.core.outlier_detector import OutlierDetector
from data.data_generation.layer_5.models.models import (
    StatisticalQualityResult,
    CompleteProductRecord,
)

logger = logging.getLogger(__name__)


class StatisticalValidator:
    """Performs statistical analysis, deduplication, and outlier detection."""

    def __init__(self, config: Layer5Config):
        self.config = config
        self._lock = threading.Lock()
        self._dedup = DedupChecker(config)
        self._distribution = DistributionChecker(config)
        self._outlier = OutlierDetector(config)
        self._correlation = CorrelationAnalyzer()

    # -- Public properties for backward compatibility ----------------------

    @property
    def record_hashes(self):
        return self._dedup.record_hashes

    @property
    def exact_duplicates(self):
        return self._dedup.exact_duplicates

    @property
    def near_duplicates(self):
        return self._dedup.near_duplicates

    @property
    def material_counts(self):
        return self._distribution.material_counts

    @property
    def category_counts(self):
        return self._distribution.category_counts

    @property
    def transport_type_counts(self):
        return self._distribution.transport_type_counts

    @property
    def packaging_category_counts(self):
        return self._distribution.packaging_category_counts

    @property
    def weight_values(self):
        return self._outlier.weight_values

    @property
    def transport_distances(self):
        return self._outlier.transport_distances

    @property
    def weight_packaging_pairs(self):
        return self._correlation.weight_packaging_pairs

    @property
    def material_transport_pairs(self):
        return self._correlation.material_transport_pairs

    # -- Main validation ---------------------------------------------------

    def validate_record(
        self, record: CompleteProductRecord
    ) -> StatisticalQualityResult:
        """Perform statistical validation on a single record (thread-safe)."""
        try:
            with self._lock:
                duplicate_check = self._dedup.check_duplicates(record)

                distribution_check = self._distribution.check_distributions(
                    record
                )

                outlier_check = self._outlier.check_outliers(record)

                wp_ok, wp_issues = (
                    self._correlation.check_weight_packaging_correlation(
                        record,
                        self._distribution.material_counts,
                        self._outlier.transport_distances,
                    )
                )
                mt_ok, mt_issues = (
                    self._correlation.check_material_transport_correlation(
                        record,
                        self._distribution.material_counts,
                        self._outlier.transport_distances,
                    )
                )

                # Update tracking data (after checks, so current record
                # does not influence its own check)
                self._outlier.update(record)
                self._correlation.update(record)

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

    # -- Summary and reset -------------------------------------------------

    def get_statistical_summary(self) -> Dict[str, Any]:
        """Get summary of statistical validation results."""
        summary: Dict[str, Any] = {
            "total_records_checked": len(self._dedup.record_hashes),
            "exact_duplicates_found": self._dedup.exact_duplicates,
            "near_duplicates_found": self._dedup.near_duplicates,
            "unique_records": (
                len(self._dedup.record_hashes)
                - self._dedup.exact_duplicates
            ),
            "material_coverage": len(self._distribution.material_counts),
            "category_coverage": len(self._distribution.category_counts),
            "transport_type_coverage": len(
                self._distribution.transport_type_counts
            ),
            "packaging_category_coverage": len(
                self._distribution.packaging_category_counts
            ),
            "weight_statistics": self._compute_statistics(
                self._outlier.weight_values
            ),
            "packaging_mass_statistics": self._compute_statistics(
                self._outlier.packaging_masses
            ),
            "transport_distance_statistics": self._compute_statistics(
                self._outlier.transport_distances
            ),
        }

        if len(self._correlation.weight_packaging_pairs) >= 2:
            weights = [
                w for w, _ in self._correlation.weight_packaging_pairs
            ]
            pkg_masses = [
                p for _, p in self._correlation.weight_packaging_pairs
            ]
            wp_corr = self._correlation._pearson_correlation(
                weights, pkg_masses
            )
            summary["weight_packaging_correlation"] = wp_corr
        else:
            summary["weight_packaging_correlation"] = None

        summary["weight_packaging_pairs_count"] = len(
            self._correlation.weight_packaging_pairs
        )
        summary["material_transport_pairs_count"] = len(
            self._correlation.material_transport_pairs
        )

        return summary

    @staticmethod
    def _compute_statistics(
        values: List[float],
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
        self._dedup.reset()
        self._distribution.reset()
        self._outlier.reset()
        self._correlation.reset()
        logger.info("Reset statistical tracking data")
