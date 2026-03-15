# Task 07: Deterministic Validator

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. The deterministic validator runs code-based checks
on every generated Layer 3 record without LLM calls. It implements all 12
checks from the design doc plus corrective auto-fixes. This is Stage 1
of the validation pipeline.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere
- Errors are blocking (record is rejected). Warnings are non-blocking
  (record passes with flag).

## Reference files to study

- `data/data_generation/layer_5/core/deterministic_validator.py` -- Pattern
  for DeterministicValidator class: init with config, validate_record()
  method returning a result dataclass, grouped check methods
- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Section 9.2 lists
  all 12 deterministic checks, section 9.3 lists corrective fixes
- `data/data_generation/layer_3/models/models.py` -- ValidationResult
  and Layer3Record dataclasses (from task 01)

## Dependencies

- Task 01 (Layer3Record, TransportLeg, ValidationResult models)

## The task

Create `core/deterministic_validator.py` with a `DeterministicValidator`
class.

### DeterministicValidator class

```python
class DeterministicValidator:
    def __init__(self, config: Layer3Config):
        self.config = config

    def validate(self, record: Layer3Record) -> ValidationResult:
        """Run all 12 deterministic checks on a record."""
        errors = []
        warnings = []
        # Run each check, collecting errors and warnings
        # Return ValidationResult

    def validate_and_correct(self, record: Layer3Record) -> ValidationResult:
        """Run validation + corrective fixes. Returns result with
        corrected_record if fixes were applied."""
        result = self.validate(record)
        if result.warnings or not result.is_valid:
            corrected, corrections = self._apply_corrections(record)
            result.corrections_applied = corrections
            result.corrected_record = corrected
        return result
```

### The 12 checks (implement each as a private method)

1. **`_check_schema_completeness(record)`** -- Every leg has all 13
   required fields with correct types. Error if any field missing.

2. **`_check_coordinate_range(record)`** -- lat in [-90, 90], lon in
   [-180, 180] for from_lat, from_lon, to_lat, to_lon in every leg.
   Error if out of range.

3. **`_check_land_validation(record)`** -- Coordinates not in ocean
   for land-based locations. Warning if suspicious (not error, since
   the land/sea mask may have edge cases). For V1, this can be a stub
   that always passes.

4. **`_check_distance_bounds(record)`** -- Each leg distance_km >= 1
   and <= 25000. Error if out of range. Use config thresholds.

5. **`_check_material_coverage(record)`** -- Every material in
   `record.materials` appears as the `material` field in at least one
   leg. Error if a material is missing.

6. **`_check_step_coverage(record)`** -- Every step in
   `step_material_mapping` appears as from_step or to_step in the
   legs for that material. Warning if a step is missing (Sonnet may
   merge similar steps).

7. **`_check_leg_continuity(record)`** -- For each material's leg
   chain (sorted by leg_index), to_location of leg N must equal
   from_location of leg N+1. Error if discontinuous.

8. **`_check_transport_modes(record)`** -- All modes in the allowed
   set from config. transport_modes array is non-empty per leg.
   Error if invalid mode or empty.

9. **`_check_reasoning_quality(record)`** -- Each leg's reasoning is
   non-empty and at least config.min_reasoning_length characters.
   Warning if too short.

10. **`_check_convergence(record)`** -- All material chains converge
    at the assembly step. Find the assembly step (last shared step
    across materials) and verify all materials route there. Error if
    materials fail to converge.

11. **`_check_warehouse_terminus(record)`** -- The final leg (highest
    leg_index) ends at a warehouse-like destination. All material
    chains must terminate at the same final destination. Error if
    different endpoints.

12. **`_check_leg_indexing(record)`** -- Leg indices are sequential
    starting from 0, no gaps, no duplicates within a material chain.
    Warning if gaps exist (corrective fix can re-index).

### Corrective fixes (in _apply_corrections)

- Recompute total_distance_km if it mismatches sum of leg distances
- Round coordinates to config.coordinate_decimal_places
- Strip/collapse whitespace in reasoning
- Re-index legs sequentially if gaps exist

## Acceptance criteria

1. `validate(valid_record).is_valid` returns True for a well-formed record
2. `validate(record_with_ocean_coords)` produces a warning
3. `validate(record_missing_material)` produces an error
4. `validate_and_correct(record)` returns corrected_record with fixes applied
5. All 12 check methods exist and are called by validate()
6. Config thresholds are used (min_leg_distance_km, etc.)
