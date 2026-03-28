# Task 006: Distance Validator

## Objective
Create the validation module that checks LLM-extracted transport mode distances against the known total_distance_km for each record. 1% tolerance (D8). Collects failed records for batch retry.

## Scope
**Files to create:**
- `data/data_generation/layer_6/enrichment/validator.py`

**Files to read (not modify):**
- `data/data_generation/layer_6/enrichment/config.py` -- EnrichmentConfig (from task 001)
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- LLM calls or retry logic
- Orchestration

## Dependencies
- **Requires:** 001 (EnrichmentConfig for tolerance setting)
- **Produces:** `validate_extraction()` and `ValidationResult` used by task 009

## Technical Details

### Data Structures
```python
@dataclass
class ValidationResult:
    record_id: str
    is_valid: bool
    expected_km: float
    extracted_km: float
    discrepancy_pct: float
    mode_distances: Dict[str, float]  # the extracted values
```

### Main Function
```python
def validate_extraction(
    extracted: Dict,
    total_distance_km: float,
    tolerance: float = 0.01
) -> ValidationResult:
    """Validate that extracted mode distances sum to ~total_distance_km.

    Args:
        extracted: Dict with keys: id, road_km, sea_km, rail_km,
                   air_km, inland_waterway_km
        total_distance_km: Known total from Layer 4 data
        tolerance: Maximum allowed relative discrepancy (0.01 = 1%)

    Returns:
        ValidationResult with pass/fail and details.
    """
```

### Validation Logic
1. Sum all mode distances: `extracted_total = road_km + sea_km + rail_km + air_km + inland_waterway_km`
2. Calculate discrepancy: `abs(extracted_total - total_distance_km) / total_distance_km`
3. Pass if discrepancy <= tolerance (1%)
4. Handle edge case: total_distance_km == 0 (pass if all extracted are 0)
5. Handle edge case: any mode distance < 0 (always fail)

### Batch Collector
```python
class FailedRecordCollector:
    """Collects failed validation records for batch retry."""

    def __init__(self):
        self.failed: List[Dict] = []  # original record dicts
        self.results: List[ValidationResult] = []

    def add_failure(self, record: Dict, result: ValidationResult):
        ...

    def get_retry_batch(self) -> List[Dict]:
        ...

    def summary(self) -> Dict:
        """Return counts: total_validated, passed, failed, retry_pending."""
```

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file

## Verification
- [ ] validate_extraction with exact match returns is_valid=True
- [ ] validate_extraction with 2% discrepancy returns is_valid=False
- [ ] validate_extraction with 0.5% discrepancy returns is_valid=True
- [ ] Edge case: total_distance_km=0 handled
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- None expected -- this is straightforward arithmetic validation
