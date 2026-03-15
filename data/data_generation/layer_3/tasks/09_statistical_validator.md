# Task 09: Statistical Validator

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. The statistical validator runs batch-level analysis
after all records are generated. It detects quality issues that
single-record checks cannot catch: duplicate transport plans, location
clustering, distance outliers, and mode distribution skew. This is Stage 4
of the validation pipeline.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for data structures
- Standard library only (hashlib, statistics, collections)
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/core/statistical_validator.py` -- Pattern
  for StatisticalValidator class: init tracking data, validate_record(),
  hash-based dedup, z-score outlier detection, distribution monitoring
- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Section 9.5 for
  statistical validation design
- `data/data_generation/layer_3/models/models.py` --
  StatisticalValidationResult dataclass (from task 01)

## Dependencies

- Task 01 (Layer3Record, StatisticalValidationResult models)

## The task

Create `core/statistical_validator.py` with a `StatisticalValidator` class.

### StatisticalValidator class

```python
class StatisticalValidator:
    def __init__(self, config: Layer3Config):
        self.config = config
        self._initialize_tracking()

    def _initialize_tracking(self):
        """Initialize batch-level tracking data structures."""
        self.record_hashes: Set[str] = set()
        self.location_counts: Counter = Counter()  # (step_type, city) -> count
        self.distance_values: List[float] = []
        self.mode_counts: Counter = Counter()
        self.total_records: int = 0

    def validate_record(
        self, record: Layer3Record
    ) -> StatisticalValidationResult:
        """
        Validate a single record against batch-level statistics.
        Call this for each record as it is generated.
        Updates internal tracking and returns per-record result.
        """

    def get_batch_summary(self) -> Dict[str, Any]:
        """Get summary statistics for the batch."""

    def reset(self):
        """Reset tracking for a new batch."""
        self._initialize_tracking()
```

### Four statistical checks

1. **Duplicate detection** (`_check_duplicates`):
   - Hash based on the ordered sequence of (material, from_location,
     to_location) tuples across all legs
   - Use `hashlib.md5` for the hash
   - Flag exact duplicates (hash collision)
   - Track near-duplicates via location overlap (>90% same locations)

2. **Location diversity** (`_check_location_diversity`):
   - Track (step_type, city) pairs across the batch
   - After sufficient records (>50), flag if any single city accounts
     for >30% of a given step type
   - E.g., if >30% of "spinning" steps are in Shanghai, flag it
   - Use `config.location_diversity_threshold`

3. **Distance outliers** (`_check_distance_outliers`):
   - Track total_distance_km values across the batch
   - After sufficient records (>10), compute z-score for current record
   - Flag if z-score > `config.distance_outlier_zscore` (default 3.0)
   - Use `statistics.mean()` and `statistics.stdev()`

4. **Mode distribution** (`_check_mode_distribution`):
   - Track transport mode usage across all legs in the batch
   - After sufficient records (>50), flag if any single mode exceeds
     `config.mode_max_single_percentage` (default 80%) of all mode
     occurrences
   - Also flag if any of the 5 modes has 0 occurrences

### Hash computation

```python
def _compute_record_hash(self, record: Layer3Record) -> str:
    """Hash based on leg sequence for dedup detection."""
    key_parts = []
    for leg in sorted(record.transport_legs, key=lambda l: l.leg_index):
        key_parts.append((leg.material, leg.from_location, leg.to_location))
    key_str = json.dumps(key_parts, sort_keys=True)
    return hashlib.md5(key_str.encode()).hexdigest()
```

## Acceptance criteria

1. `validate_record(record)` returns a StatisticalValidationResult
2. Duplicate records are flagged (is_duplicate=True)
3. Distance outliers are detected after 10+ records
4. Location diversity issues are flagged after 50+ records
5. Mode distribution skew is detected
6. `get_batch_summary()` returns meaningful statistics
7. `reset()` clears all tracking data
