# Task 09: I/O Layer Update

## Codebase context

ESPResso-V2 Layer 5 V2 changes the validation result structure. The incremental
output writer must be updated to write V2 result fields (passport status,
coherence scores, sampled reward) instead of V1 fields (deterministic check
results, semantic scores, per-record reward).

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/io/writer_incremental.py` -- Current V1 writer to modify
- `data/data_generation/layer_5/io/writer.py` -- Original writer (may be removed if unused)
- `data/data_generation/layer_5/models/models.py` -- V2 models (from task 01)

## Dependencies

- Task 01 (CompleteValidationResult V2 with passport, coherence, statistical, reward fields)

## The task

Modify `io/writer_incremental.py` to use V2 model fields.

### Keep unchanged

- `IncrementalValidationOutputWriter.__init__()` structure
- `write_batch()` method logic (separate by decision, write temp files, track)
- `_write_temp_file()` method structure
- `merge_final_outputs()` and `_merge_category_files()` logic
- `_append_file_to_writer()`, `_get_fieldnames_from_file()`
- `_log_progress_if_needed()`, `_cleanup_temp_files()`
- `_safe_getattr()` helper function
- `write_validation_summary()` structure

### Modify: `_get_extended_fieldnames()`

Update fieldnames to reflect V2 result structure:

```python
def _get_extended_fieldnames(self) -> List[str]:
    return [
        'record_id', 'subcategory_name', 'category_name',
        'materials', 'material_weights_kg', 'material_percentages',
        'preprocessing_steps',
        'total_weight_kg', 'total_transport_distance_km', 'supply_chain_type',
        'transport_items', 'total_packaging_mass_kg',
        'packaging_items', 'packaging_categories',
        # V2 validation fields
        'passport_valid',
        'lifecycle_coherence_score', 'cross_layer_contradiction_score',
        'overall_coherence_score', 'coherence_recommendation',
        'is_duplicate', 'is_outlier',
        'reward_sampled', 'reward_score', 'dataset_quality_estimate',
        'final_decision', 'final_score', 'validation_timestamp'
    ]
```

### Modify: `_create_extended_row()`

Update to extract V2 fields:

```python
def _create_extended_row(self, result: CompleteValidationResult) -> Dict[str, Any]:
    record = result.complete_record
    # ... (keep existing record field extraction) ...
    return {
        # ... (keep record fields: record_id through packaging_categories) ...
        'passport_valid': _safe_getattr(result, 'passport.is_valid', True),
        'lifecycle_coherence_score': float(
            _safe_getattr(result, 'coherence.lifecycle_coherence_score', 0.0) or 0.0
        ),
        'cross_layer_contradiction_score': float(
            _safe_getattr(result, 'coherence.cross_layer_contradiction_score', 0.0) or 0.0
        ),
        'overall_coherence_score': float(
            _safe_getattr(result, 'coherence.overall_coherence_score', 0.0) or 0.0
        ),
        'coherence_recommendation': _safe_getattr(
            result, 'coherence.recommendation', 'review'
        ),
        'is_duplicate': _safe_getattr(result, 'statistical.is_duplicate', False),
        'is_outlier': _safe_getattr(result, 'statistical.is_outlier', False),
        'reward_sampled': _safe_getattr(result, 'reward.was_sampled', False),
        'reward_score': float(
            _safe_getattr(result, 'reward.reward_score', 0.0) or 0.0
        ),
        'dataset_quality_estimate': float(
            _safe_getattr(result, 'reward.dataset_estimated_quality', 0.0) or 0.0
        ),
        'final_decision': _safe_getattr(result, 'final_decision', 'review'),
        'final_score': float(_safe_getattr(result, 'final_score', 0.0) or 0.0),
        'validation_timestamp': _safe_getattr(
            result, 'metadata.validation_timestamp', datetime.now().isoformat()
        ) or datetime.now().isoformat()
    }
```

### Modify: `_validate_result()`

Update required attributes check:
- Replace `'deterministic'` with `'passport'`
- Remove check for `deterministic.is_valid` and `deterministic.errors`
- Add check for `passport.is_valid`

### Modify: rejected row extra fields

Update rejected row to use V2 decision_factors:
```python
row['rejection_reason'] = '; '.join(
    _safe_getattr(result, 'decision_factors', []) or []
)
row['validation_errors'] = json.dumps(
    _safe_getattr(result, 'passport.errors', []) or []
)
```

## Acceptance criteria

1. `write_batch([v2_result], 0)` writes CSV with V2 fieldnames
2. CSV headers include `passport_valid`, `lifecycle_coherence_score`, `reward_sampled`
3. CSV headers do NOT include V1 fields (`deterministic_validation_passed`, `plausibility_score`)
4. `_validate_result()` checks for `passport` attribute, not `deterministic`
5. `merge_final_outputs()` produces valid final CSV files
6. `write_validation_summary()` still works

## Files to modify

- `io/writer_incremental.py` -- modify as described

## Files to remove

- `io/writer.py` -- if it only supports V1 models, remove it

## Reference

- V1 writer: `layer_5/io/writer_incremental.py` (current file)
