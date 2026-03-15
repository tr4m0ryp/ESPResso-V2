Rewrite `core/validator.py` with a PackagingValidator class implementing per-record and
dataset-level validation checks.

## What to remove

All V1 validation logic. The current `validator.py` (if any V1 code exists) should be
fully replaced.

## PackagingValidator class

```python
class PackagingValidator:
    def __init__(self, config: Layer4Config):
        self.config = config
        self._batch_stats = self._init_batch_stats()
```

### validate(record: Layer4Record) -> ValidationResult

Run all per-record validation checks. Return a ValidationResult with `is_valid=True` if
all hard checks pass (errors list empty). Soft issues go into warnings.

**Hard checks (errors -- record is rejected):**

1. **Category count**: `len(packaging_categories)` must be exactly 3.
2. **Category names**: must be exactly `["Paper/Cardboard", "Plastic", "Other/Unspecified"]`
   in that order.
3. **Mass count**: `len(packaging_masses_kg)` must be exactly 3.
4. **Non-negative masses**: all three masses must be >= 0.0.
5. **At least one positive**: `sum(packaging_masses_kg)` must be > 0.0.

**Soft checks (warnings -- record is accepted with warning):**

6. **Packaging ratio low**: total packaging mass < `config.min_packaging_ratio * total_weight_kg`.
   Warning: "Packaging ratio {ratio:.1%} below minimum {min:.1%}".
7. **Packaging ratio high**: total packaging mass > `config.max_packaging_ratio * total_weight_kg`.
   Warning: "Packaging ratio {ratio:.1%} above maximum {max:.1%}".
8. **Reasoning quality**: `len(packaging_reasoning.strip())` < `config.min_reasoning_length`.
   Warning: "Reasoning too short ({length} chars)".

After validation, update batch stats by calling `_update_batch_stats(record)`.

### validate_batch_summary() -> Dict[str, Any]

Return dataset-level aggregate statistics after all records have been processed. Used by
the orchestrator for post-generation reporting.

Returns a dict with:

```python
{
    "total_records": int,
    "records_with_warnings": int,
    "duplicate_count": int,            # records with identical mass triplets
    "duplicate_percentage": float,
    "category_usage": {
        "Paper/Cardboard": float,      # percentage of records with non-zero mass
        "Plastic": float,
        "Other/Unspecified": float,
    },
    "mean_packaging_ratio": float,     # mean(total_pkg_mass / product_weight)
    "distance_mass_correlation": float, # Pearson correlation coefficient
    "zero_mass_count": int,            # records where all masses are 0 (should be 0)
}
```

**Expected ranges for healthy datasets:**
- `duplicate_percentage` < 5%
- `category_usage["Paper/Cardboard"]` > 80%
- `category_usage["Plastic"]` > 70%
- `category_usage["Other/Unspecified"]` < 30%
- `mean_packaging_ratio` between 3% and 8%
- `distance_mass_correlation` > 0 (positive)
- `zero_mass_count` == 0

Log warnings for any values outside expected ranges.

### _init_batch_stats() -> Dict[str, Any]

Initialize internal tracking state:

```python
{
    "total_records": 0,
    "records_with_warnings": 0,
    "mass_triplets": Counter(),        # for duplicate detection
    "category_nonzero_counts": {"Paper/Cardboard": 0, "Plastic": 0, "Other/Unspecified": 0},
    "packaging_ratios": [],            # total_pkg_mass / total_weight_kg per record
    "distances": [],                   # total_distance_km per record
    "total_masses": [],                # sum of packaging masses per record
}
```

### _update_batch_stats(record: Layer4Record) -> None

Update internal counters after validating a record:
- Increment `total_records`
- Add mass triplet as a tuple `(paper, plastic, other)` rounded to 4 decimal places
  to the Counter for duplicate detection
- Update category nonzero counts
- Append packaging ratio and distance for correlation analysis

### reset() -> None

Reset all batch stats to initial state. Called between generation runs.

## Design rules

- Validation is stateless per-record (no dependencies between records for hard checks).
- Batch-level stats are accumulated across records for post-generation reporting only
  (never block a record based on batch stats).
- Use `numpy` or `scipy.stats.pearsonr` for correlation calculation in
  `validate_batch_summary()`. If not available, fall back to a simple manual calculation.
- No emojis.

## Files to modify

- `core/validator.py` -- complete rewrite

## Reference

- Layer 3 deterministic validator: `layer_3/core/deterministic_validator.py`
- Design doc: `layer_4/DESIGN_V2.md` section 9 (Validation)
