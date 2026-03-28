# Task 004: Update Layer 6 Configuration

## Objective
Update the existing Layer 6 config to support the new enriched dataset input path and the renamed output columns (D10). The TRANSPORT_MODE_PARAMS constants remain for backward compatibility but the enriched path becomes the new default input.

## Scope
**Files to modify:**
- `data/data_generation/layer_6/config/config.py`

**Files to read (not modify):**
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- Creating new files
- Modifying any other Layer 6 source files

## Dependencies
- **Requires:** none
- **Produces:** Updated config with enriched input path and column name constants used by tasks 007, 008

## Technical Details

### Changes to config.py

1. Add a new input path default pointing to the enriched dataset:
```python
enriched_input_path: str = field(
    default=(
        'data/datasets/pre-model/generated/layer_6'
        '/pre_layer6_enriched.parquet'
    )
)
```

2. Add a boolean flag to switch between enriched and legacy mode:
```python
use_enriched_transport: bool = field(default=True)
```

3. Add column name constants for the new output schema (D10):
```python
# Output column names for transport data
TRANSPORT_DISTANCES_COL = 'transport_mode_distances_km'
TRANSPORT_FRACTIONS_COL = 'transport_mode_fractions'
TRANSPORT_EF_COL = 'effective_ef_g_co2e_tkm'
```

4. Keep existing TRANSPORT_MODE_PARAMS and TRANSPORT_EMISSION_FACTORS unchanged -- they are still used as emission factor lookup even in the new approach. Only the mode selection model is replaced.

5. Add the enrichment output directory path:
```python
enrichment_output_dir: str = field(
    default='data/datasets/pre-model/generated/layer_6'
)
```

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file
- Preserve all existing config fields and their defaults
- Add new fields at the end of the dataclass

## Verification
- [ ] File imports successfully: `python3 -c "from data.data_generation.layer_6.config.config import Layer6Config, TRANSPORT_DISTANCES_COL"`
- [ ] Existing fields unchanged
- [ ] New fields have sensible defaults
- [ ] File stays under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If config.py is already close to 300 lines and additions would exceed limit
