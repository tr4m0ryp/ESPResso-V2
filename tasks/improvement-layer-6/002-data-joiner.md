# Task 002: Data Joiner

## Objective
Create the module that joins Layer 5 validated records with Layer 4 transport_legs data. Layer 5 has the validated record set (50,480 rows) but lacks transport_legs. Layer 4 has transport_legs. Join via preprocessing path ID extracted from record_id.

## Scope
**Files to create:**
- `data/data_generation/layer_6/enrichment/data_joiner.py`

**Files to read (not modify):**
- `data/datasets/pre-model/generated/layer_5/layer_5_validated_dataset.csv` -- input data
- `data/datasets/pre-model/generated/layer_4/layer_4_complete_dataset.parquet` -- transport_legs source
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- LLM calls, prompt building, or orchestration
- Modifying Layer 5 or Layer 4 data files

## Dependencies
- **Requires:** none
- **Produces:** `join_transport_legs(layer5_path, layer4_path) -> pd.DataFrame` function used by task 009

## Technical Details
The join logic:

1. Read Layer 5 CSV into DataFrame (50,480 rows, 27 cols).
2. Extract `pp_id` from `record_id` column using regex: `r'(pp-\d+)'`.
   - record_id format: `cl-2-3_pp-015810` -> extract `pp-015810`
3. Read Layer 4 parquet, selecting only: `preprocessing_path_id`, `transport_legs`, `total_distance_km`.
4. Inner join: Layer 5 `pp_id` == Layer 4 `preprocessing_path_id`.
5. All 50,480 records should match (verified during design phase).
6. Return merged DataFrame with all Layer 5 columns + `transport_legs` + `total_distance_km` (from Layer 4).
7. Log warnings for any unmatched records.

The function signature:
```python
def join_transport_legs(
    layer5_path: str,
    layer4_path: str
) -> pd.DataFrame:
    """Join Layer 5 validated data with Layer 4 transport_legs.

    Returns DataFrame with all Layer 5 columns plus transport_legs
    and total_distance_km from Layer 4.
    """
```

Also provide a helper:
```python
def extract_pp_id(record_id: str) -> str:
    """Extract pp-XXXXXX from record_id like cl-2-3_pp-015810."""
```

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file
- Use logging, not print statements

## Verification
- [ ] Function loads without import errors
- [ ] extract_pp_id('cl-2-3_pp-015810') returns 'pp-015810'
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If Layer 5 record_id format doesn't match expected pattern
- If join produces significantly fewer than 50,480 rows
