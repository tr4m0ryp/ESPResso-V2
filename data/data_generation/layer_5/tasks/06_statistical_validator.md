# Task 06: Statistical Validator

## Codebase context

ESPResso-V2 Layer 5 V2 keeps and strengthens the statistical validator from V1.
It is the only validation stage that runs without LLM calls and catches dataset-level
issues that per-record checks miss: deduplication, distribution monitoring, outlier
detection, and new cross-layer correlation checks.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere
- Sequential processing only (maintains running state)

## Reference files to study

- `data/data_generation/layer_5/core/statistical_validator.py` -- V1 class to extend
- `data/data_generation/layer_5/config/config.py` -- V2 config (from task 02)
- `data/data_generation/layer_3/core/statistical_validator.py` -- Pattern for statistical checks

## Dependencies

- Task 01 (CompleteProductRecord, StatisticalQualityResult models)
- Task 02 (Layer5Config with statistical settings)

## The task

Rewrite `core/statistical_validator.py` to use V2 models and add cross-layer
correlation checks.

### Keep from V1

- `_initialize_tracking_data()` method and all tracking data structures
- `_check_duplicates()` with MD5 hash deduplication
- `_compute_record_hash()` method
- `_check_distributions()` and all 4 sub-methods (material, category, transport, packaging)
- `_check_outliers()` with 3 z-score checks (weight 3-sigma, ratio 2-sigma, transport 2.5-sigma)
- `_update_tracking_data()`
- `get_statistical_summary()`
- `reset_statistical_tracking()`

### Modify

- Return `StatisticalQualityResult` instead of `StatisticalValidationResult`
- Use config thresholds: `config.dedup_similarity_threshold` instead of hardcoded 0.95,
  `config.outlier_weight_sigma` instead of hardcoded 3.0, etc.
- Use `config.max_single_material_pct` instead of hardcoded 0.30 in distribution thresholds

### Add: Cross-layer correlation checks

Add two new private methods and integrate them into `validate_record()`:

```python
def _check_weight_packaging_correlation(self, record: CompleteProductRecord) -> Tuple[bool, List[str]]:
    """Check that heavier products have proportionally more packaging.

    After 100+ records, compute Pearson correlation between product weight
    and packaging mass. Flag if correlation is negative or below 0.1
    (products should have positive correlation: heavier = more packaging).

    Returns:
        Tuple of (is_ok, list of issues)
    """

def _check_material_transport_correlation(self, record: CompleteProductRecord) -> Tuple[bool, List[str]]:
    """Check that exotic/specialty materials correlate with longer transport.

    Track material rarity (how many records use each material). After 100+
    records, check if records with rare materials (used in <5% of records)
    tend to have longer transport distances than the median.

    This is a soft check -- produces warnings, not errors.

    Returns:
        Tuple of (is_ok, list of issues)
    """
```

### Add tracking data for correlations

```python
# In _initialize_tracking_data():
self.weight_packaging_pairs: List[Tuple[float, float]] = []  # (product_weight, packaging_mass)
self.material_transport_pairs: List[Tuple[str, float]] = []   # (material_name, transport_distance)
```

Update `_update_tracking_data()` to populate these.

## Acceptance criteria

1. `validate_record(record)` returns `StatisticalQualityResult` (not V1 StatisticalValidationResult)
2. Deduplication still works (exact hash + near-duplicate detection)
3. Distribution monitoring still works with 4 sub-checks
4. Outlier detection uses config sigma thresholds
5. After 100 records, `weight_packaging_correlation_ok` is populated
6. After 100 records, `material_transport_correlation_ok` is populated
7. `get_statistical_summary()` includes correlation statistics
8. Sequential processing maintained (no threading)

## Files to modify

- `core/statistical_validator.py` -- rewrite with V2 models + correlation checks

## Reference

- V1 statistical validator: `layer_5/core/statistical_validator.py`
- Pearson correlation: `statistics` stdlib or manual computation
