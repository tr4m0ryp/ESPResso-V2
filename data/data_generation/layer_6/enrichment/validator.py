"""Validation module for LLM-extracted transport mode distances.

Checks that extracted per-mode distances sum to within tolerance of the
known total_distance_km for a record.  Collects failures for batch retry.

Public API:
    validate_extraction() -- validate a single extraction result
    ValidationResult      -- dataclass holding pass/fail and details
    FailedRecordCollector -- accumulates failures for retry batching
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List

logger = logging.getLogger(__name__)

_MODE_KEYS: tuple[str, ...] = (
    'road_km',
    'sea_km',
    'rail_km',
    'air_km',
    'inland_waterway_km',
)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ValidationResult:
    """Outcome of validating one LLM-extracted distance record.

    Attributes:
        record_id:       Source record identifier.
        is_valid:        True when extracted distances sum within tolerance.
        expected_km:     Known total distance from Layer 4 data.
        extracted_km:    Sum of all extracted mode distances.
        discrepancy_pct: Relative deviation: abs(extracted - expected) / expected.
                         Set to 0.0 when expected == 0 and all modes are 0.
                         Set to 1.0 (100%) when expected == 0 but modes are non-zero.
        mode_distances:  Dict mapping mode name to extracted km value.
    """

    record_id: str
    is_valid: bool
    expected_km: float
    extracted_km: float
    discrepancy_pct: float
    mode_distances: Dict[str, float]


# ---------------------------------------------------------------------------
# Core validation function
# ---------------------------------------------------------------------------

def validate_extraction(
    extracted: Dict,
    total_distance_km: float,
    tolerance: float = 0.01,
) -> ValidationResult:
    """Validate that extracted mode distances sum to ~total_distance_km.

    Args:
        extracted: Dict with keys: id, road_km, sea_km, rail_km,
                   air_km, inland_waterway_km.  Missing mode keys default
                   to 0.0.
        total_distance_km: Known total from Layer 4 data.
        tolerance: Maximum allowed relative discrepancy (0.01 == 1%).

    Returns:
        ValidationResult with pass/fail and diagnostic details.

    Notes:
        - Any mode distance < 0 causes an immediate failure.
        - When total_distance_km == 0 and all modes are 0, the result is valid.
        - When total_distance_km == 0 but any mode is non-zero, the result
          is invalid (discrepancy_pct is reported as 1.0).
    """
    record_id: str = str(extracted.get('id', ''))

    mode_distances: Dict[str, float] = {
        key: float(extracted.get(key, 0.0))
        for key in _MODE_KEYS
    }

    # Fail immediately if any mode value is negative.
    for mode, km in mode_distances.items():
        if km < 0:
            logger.debug(
                'record %s: negative distance for %s (%.4f km)',
                record_id, mode, km,
            )
            extracted_total = sum(mode_distances.values())
            discrepancy = _compute_discrepancy(extracted_total, total_distance_km)
            return ValidationResult(
                record_id=record_id,
                is_valid=False,
                expected_km=total_distance_km,
                extracted_km=extracted_total,
                discrepancy_pct=discrepancy,
                mode_distances=mode_distances,
            )

    extracted_total: float = sum(mode_distances.values())

    # Edge case: expected total is zero.
    if total_distance_km == 0.0:
        if extracted_total == 0.0:
            return ValidationResult(
                record_id=record_id,
                is_valid=True,
                expected_km=0.0,
                extracted_km=0.0,
                discrepancy_pct=0.0,
                mode_distances=mode_distances,
            )
        # Non-zero extraction against zero expected total -- always invalid.
        logger.debug(
            'record %s: expected 0 km but extracted %.4f km',
            record_id, extracted_total,
        )
        return ValidationResult(
            record_id=record_id,
            is_valid=False,
            expected_km=0.0,
            extracted_km=extracted_total,
            discrepancy_pct=1.0,
            mode_distances=mode_distances,
        )

    discrepancy = abs(extracted_total - total_distance_km) / total_distance_km
    is_valid = discrepancy <= tolerance

    if not is_valid:
        logger.debug(
            'record %s: discrepancy %.4f%% exceeds tolerance %.4f%%'
            ' (expected=%.2f km, extracted=%.2f km)',
            record_id,
            discrepancy * 100,
            tolerance * 100,
            total_distance_km,
            extracted_total,
        )

    return ValidationResult(
        record_id=record_id,
        is_valid=is_valid,
        expected_km=total_distance_km,
        extracted_km=extracted_total,
        discrepancy_pct=discrepancy,
        mode_distances=mode_distances,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _compute_discrepancy(extracted_total: float, expected: float) -> float:
    """Return relative discrepancy, handling zero expected value."""
    if expected == 0.0:
        return 1.0 if extracted_total != 0.0 else 0.0
    return abs(extracted_total - expected) / expected


# ---------------------------------------------------------------------------
# Batch failure collector
# ---------------------------------------------------------------------------

class FailedRecordCollector:
    """Collects failed validation records for batch retry.

    Usage::

        collector = FailedRecordCollector()
        result = validate_extraction(extracted, total_km)
        if not result.is_valid:
            collector.add_failure(original_record, result)

        retry_batch = collector.get_retry_batch()
        stats = collector.summary()
    """

    def __init__(self) -> None:
        self._total_validated: int = 0
        self._passed: int = 0
        self.failed: List[Dict] = []
        self.results: List[ValidationResult] = []

    # ------------------------------------------------------------------
    # Recording outcomes
    # ------------------------------------------------------------------

    def record_pass(self) -> None:
        """Increment pass counter without storing any extra data."""
        self._total_validated += 1
        self._passed += 1

    def add_failure(self, record: Dict, result: ValidationResult) -> None:
        """Store a failed record and its validation result for retry.

        Args:
            record: The original record dict as passed to the extractor.
            result: The ValidationResult returned by validate_extraction().
        """
        self._total_validated += 1
        self.failed.append(record)
        self.results.append(result)

        logger.debug(
            'failure collected for record %s (discrepancy=%.4f%%)',
            result.record_id,
            result.discrepancy_pct * 100,
        )

    # ------------------------------------------------------------------
    # Retrieval
    # ------------------------------------------------------------------

    def get_retry_batch(self) -> List[Dict]:
        """Return the list of failed original record dicts for retry.

        Returns:
            List of record dicts that failed validation.  The list is a
            shallow copy; modifying it does not affect internal state.
        """
        return list(self.failed)

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def summary(self) -> Dict:
        """Return counts of validation outcomes.

        Returns:
            Dict with keys:
                total_validated -- number of records passed to the collector
                passed           -- records that passed validation
                failed           -- records that failed validation
                retry_pending    -- same as failed (alias for caller clarity)
        """
        failed_count = len(self.failed)
        return {
            'total_validated': self._total_validated,
            'passed': self._passed,
            'failed': failed_count,
            'retry_pending': failed_count,
        }
