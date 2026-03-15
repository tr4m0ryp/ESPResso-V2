"""
Statistical Validator for Layer 3: Stage 4 Validation

Performs batch-level analysis after records are generated. Detects quality
issues that single-record checks cannot catch: duplicate transport plans,
location clustering, distance outliers, and mode distribution skew.
"""

import hashlib
import json
import logging
import statistics
from collections import Counter
from typing import Any, Dict, List, Set

from data.data_generation.layer_3.config.config import (
    ALLOWED_TRANSPORT_MODES,
    Layer3Config,
)
from data.data_generation.layer_3.models.models import (
    Layer3Record,
    StatisticalValidationResult,
)

logger = logging.getLogger(__name__)


class StatisticalValidator:
    """Performs statistical analysis, deduplication, and outlier detection.

    Tracks batch-level statistics across all validated records and flags
    issues such as duplicate transport plans, location concentration,
    distance outliers (z-score), and transport mode distribution skew.
    """

    def __init__(self, config: Layer3Config):
        self.config = config
        self._initialize_tracking()

    def _initialize_tracking(self) -> None:
        """Initialize batch-level tracking data structures."""
        self.record_hashes: Set[str] = set()
        self.location_counts: Counter = Counter()
        self.distance_values: List[float] = []
        self.mode_counts: Counter = Counter()
        self.total_records: int = 0

        # Dedup counters
        self.exact_duplicates: int = 0
        self.near_duplicates: int = 0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate_record(
        self, record: Layer3Record
    ) -> StatisticalValidationResult:
        """Validate a single record against batch-level statistics.

        Call this for each record as it is generated. Updates internal
        tracking and returns a per-record result.
        """
        issues: List[str] = []

        # 1. Duplicate detection
        dup_result = self._check_duplicates(record)

        # 2. Location diversity
        location_ok = self._check_location_diversity(record, issues)

        # 3. Distance outliers
        outlier_result = self._check_distance_outliers(record, issues)

        # 4. Mode distribution
        mode_ok = self._check_mode_distribution(record, issues)

        # Update tracking after all checks
        self._update_tracking(record)

        result = StatisticalValidationResult(
            is_duplicate=dup_result["is_duplicate"],
            duplicate_similarity=dup_result["similarity"],
            is_outlier=outlier_result["is_outlier"],
            outlier_type=outlier_result["outlier_type"],
            location_diversity_ok=location_ok,
            mode_distribution_ok=mode_ok,
            distribution_issues=issues,
        )

        logger.debug(
            "Statistical validation for %s: duplicate=%s, outlier=%s, "
            "location_ok=%s, mode_ok=%s",
            record.subcategory_name,
            result.is_duplicate,
            result.is_outlier,
            result.location_diversity_ok,
            result.mode_distribution_ok,
        )

        return result

    def get_batch_summary(self) -> Dict[str, Any]:
        """Get summary statistics for the batch."""
        summary: Dict[str, Any] = {
            "total_records": self.total_records,
            "unique_records": len(self.record_hashes),
            "exact_duplicates": self.exact_duplicates,
            "near_duplicates": self.near_duplicates,
            "distance_statistics": self._compute_basic_stats(
                self.distance_values
            ),
            "mode_distribution": dict(self.mode_counts),
            "location_distribution": dict(self.location_counts),
        }

        # Add mode coverage info
        all_modes = set(ALLOWED_TRANSPORT_MODES)
        observed_modes = {
            mode for mode in self.mode_counts if self.mode_counts[mode] > 0
        }
        summary["missing_modes"] = sorted(all_modes - observed_modes)

        return summary

    def reset(self) -> None:
        """Reset tracking for a new batch."""
        self._initialize_tracking()
        logger.info("Statistical validator tracking data reset")

    # ------------------------------------------------------------------
    # Check 1: Duplicate detection
    # ------------------------------------------------------------------

    def _compute_record_hash(self, record: Layer3Record) -> str:
        """Hash based on leg sequence for dedup detection."""
        key_parts = []
        for leg in sorted(
            record.transport_legs, key=lambda l: l.leg_index
        ):
            key_parts.append(
                (leg.material, leg.from_location, leg.to_location)
            )
        key_str = json.dumps(key_parts, sort_keys=True)
        return hashlib.md5(key_str.encode()).hexdigest()

    def _compute_location_overlap(
        self, record: Layer3Record
    ) -> float:
        """Compute max location overlap against existing records.

        Measures what fraction of this record's locations already appear
        in previously seen records. Returns a value in [0.0, 1.0].
        """
        if not record.transport_legs:
            return 0.0

        record_locations = set()
        for leg in record.transport_legs:
            record_locations.add(leg.from_location)
            record_locations.add(leg.to_location)

        if not record_locations:
            return 0.0

        # Check overlap against the set of all previously seen locations
        # aggregated from location_counts keys (city part)
        seen_cities: Set[str] = set()
        for key in self.location_counts:
            # key is (step_type, city) tuple stored as a tuple
            _, city = key
            seen_cities.add(city)

        if not seen_cities:
            return 0.0

        overlap = record_locations & seen_cities
        return len(overlap) / len(record_locations)

    def _check_duplicates(
        self, record: Layer3Record
    ) -> Dict[str, Any]:
        """Check for exact and near-duplicate records."""
        record_hash = self._compute_record_hash(record)

        # Exact duplicate
        if record_hash in self.record_hashes:
            self.exact_duplicates += 1
            return {
                "is_duplicate": True,
                "similarity": 1.0,
                "duplicate_type": "exact",
            }

        # Near-duplicate: >90% location overlap
        overlap = self._compute_location_overlap(record)
        if overlap > 0.9:
            self.near_duplicates += 1
            return {
                "is_duplicate": True,
                "similarity": overlap,
                "duplicate_type": "near",
            }

        # Not a duplicate -- add hash to tracking set
        self.record_hashes.add(record_hash)

        return {
            "is_duplicate": False,
            "similarity": overlap,
            "duplicate_type": None,
        }

    # ------------------------------------------------------------------
    # Check 2: Location diversity
    # ------------------------------------------------------------------

    def _check_location_diversity(
        self, record: Layer3Record, issues: List[str]
    ) -> bool:
        """Flag if any single city dominates a step type (>30%).

        Only triggers after 50+ records so the percentages are meaningful.
        """
        threshold = getattr(
            self.config, "location_diversity_threshold", 0.30
        )

        # Not enough data yet -- skip the check but still return ok
        if self.total_records < 50:
            return True

        # Build per-step-type totals from current location_counts
        step_totals: Counter = Counter()
        for (step_type, _city), count in self.location_counts.items():
            step_totals[step_type] += count

        # Check each (step_type, city) pair
        ok = True
        for (step_type, city), count in self.location_counts.items():
            total = step_totals[step_type]
            if total == 0:
                continue
            ratio = count / total
            if ratio > threshold:
                msg = (
                    "Location concentration: %s accounts for %.1f%% "
                    "of '%s' steps (threshold %.0f%%)"
                    % (city, ratio * 100, step_type, threshold * 100)
                )
                issues.append(msg)
                ok = False

        return ok

    # ------------------------------------------------------------------
    # Check 3: Distance outliers
    # ------------------------------------------------------------------

    def _check_distance_outliers(
        self, record: Layer3Record, issues: List[str]
    ) -> Dict[str, Any]:
        """Flag records whose total_distance_km z-score exceeds threshold.

        Only triggers after 10+ records to ensure a meaningful standard
        deviation estimate.
        """
        zscore_threshold = getattr(
            self.config, "distance_outlier_zscore", 3.0
        )

        if len(self.distance_values) < 10:
            return {"is_outlier": False, "outlier_type": None}

        mean = statistics.mean(self.distance_values)
        stdev = statistics.stdev(self.distance_values)

        if stdev == 0:
            return {"is_outlier": False, "outlier_type": None}

        z = abs(record.total_distance_km - mean) / stdev

        if z > zscore_threshold:
            issues.append(
                "Distance outlier: %.1f km (z-score %.2f, "
                "mean %.1f, stdev %.1f)"
                % (record.total_distance_km, z, mean, stdev)
            )
            return {
                "is_outlier": True,
                "outlier_type": "distance",
            }

        return {"is_outlier": False, "outlier_type": None}

    # ------------------------------------------------------------------
    # Check 4: Mode distribution
    # ------------------------------------------------------------------

    def _check_mode_distribution(
        self, record: Layer3Record, issues: List[str]
    ) -> bool:
        """Flag skewed transport mode distribution.

        After 50+ records, flags if:
        - Any single mode exceeds 80% of all mode occurrences.
        - Any of the five allowed modes has zero occurrences.
        """
        max_pct = getattr(
            self.config, "mode_max_single_percentage", 0.80
        )

        if self.total_records < 50:
            return True

        total_mode_uses = sum(self.mode_counts.values())
        if total_mode_uses == 0:
            return True

        ok = True

        # Check for over-representation
        for mode, count in self.mode_counts.items():
            ratio = count / total_mode_uses
            if ratio > max_pct:
                issues.append(
                    "Mode skew: '%s' accounts for %.1f%% of all mode "
                    "occurrences (threshold %.0f%%)"
                    % (mode, ratio * 100, max_pct * 100)
                )
                ok = False

        # Check for absent modes
        for mode in ALLOWED_TRANSPORT_MODES:
            if self.mode_counts[mode] == 0:
                issues.append(
                    "Missing transport mode: '%s' has 0 occurrences "
                    "after %d records" % (mode, self.total_records)
                )
                ok = False

        return ok

    # ------------------------------------------------------------------
    # Tracking helpers
    # ------------------------------------------------------------------

    def _update_tracking(self, record: Layer3Record) -> None:
        """Update internal tracking data after validating a record."""
        self.total_records += 1
        self.distance_values.append(record.total_distance_km)

        for leg in record.transport_legs:
            # Location tracking: (step_type, city) pairs
            # Use from_step as the step_type, paired with from_location
            self.location_counts[(leg.from_step, leg.from_location)] += 1
            self.location_counts[(leg.to_step, leg.to_location)] += 1

            # Mode tracking
            for mode in leg.transport_modes:
                self.mode_counts[mode] += 1

    def _compute_basic_stats(
        self, values: List[float]
    ) -> Dict[str, float]:
        """Compute basic descriptive statistics for a list of values."""
        if not values:
            return {}

        result: Dict[str, float] = {
            "count": len(values),
            "mean": statistics.mean(values),
            "median": statistics.median(values),
            "min": min(values),
            "max": max(values),
        }
        if len(values) > 1:
            result["stdev"] = statistics.stdev(values)
        else:
            result["stdev"] = 0.0

        return result
