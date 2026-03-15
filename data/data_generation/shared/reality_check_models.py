"""
Data structures for per-layer LLM reality validation.

Used by the RealityChecker base class and all layer orchestrators.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class RecordCheckResult:
    """Result of checking a single record for realism."""
    record_index: int
    passed: bool
    justification: str
    improvement_hint: str
    raw_record: Any


@dataclass
class BatchCheckResult:
    """Aggregated result of checking a batch of records."""
    passed_records: List[RecordCheckResult]
    failed_records: List[RecordCheckResult]
    total_checked: int
    pass_rate: float


@dataclass
class RealityCheckStats:
    """Cumulative statistics across all reality check passes."""
    total_checked: int = 0
    total_passed_first: int = 0
    total_regenerated: int = 0
    total_passed_second: int = 0
    total_discarded: int = 0
    discarded_log: List[Dict] = field(default_factory=list)
