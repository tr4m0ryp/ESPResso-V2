# Task 008: Processing and Calculator Updates

## Objective
Update the Layer 6 calculator and processing modules to read the enriched dataset (with per-mode distance columns) and use the new actual transport calculation instead of the logit model. Update output column names per D10.

## Scope
**Files to modify:**
- `data/data_generation/layer_6/core/calculator.py`
- `data/data_generation/layer_6/core/_processing.py`

**Files to read (not modify):**
- `data/data_generation/layer_6/core/components.py` -- to understand new transport function signature (task 007)
- `data/data_generation/layer_6/config/config.py` -- updated config (task 004)
- `data/data_generation/layer_6/core/transport_model.py` -- existing model
- `data/data_generation/layer_6/core/databases.py` -- material/processing databases
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- The transport calculation itself (task 007)
- Enrichment code
- Config changes (task 004)

## Dependencies
- **Requires:** 004 (config with new column names and use_enriched_transport flag)
- **Produces:** Updated calculator and processing that work with enriched data. Used by task 010 for integration.

## Technical Details

### Changes to calculator.py

The CarbonFootprintCalculator class currently calls `calculate_transport(weight_kg, distance_km, self.transport_model)`. It needs to:

1. Check `config.use_enriched_transport` flag.
2. If True: read per-mode distances from the record (columns: `road_km`, `sea_km`, `rail_km`, `air_km`, `inland_waterway_km`) and call `calculate_transport_from_actuals()`.
3. If False: fall back to existing logit model (backward compat).
4. Store the new output fields: `transport_mode_distances_km`, `transport_mode_fractions`, `effective_ef_g_co2e_tkm`.

Find the method that calls `calculate_transport()` (likely `calculate_record()`) and add the branching logic.

### Changes to _processing.py

This module handles input file reading and output assembly.

1. **Input reading**: When `use_enriched_transport=True`, read from `config.enriched_input_path` instead of `config.input_path`. The enriched parquet has the 5 mode distance columns.
2. **Output assembly**: Replace the old column names in the output dict:
   - `transport_mode_probabilities` -> `transport_mode_distances_km` (D10)
   - `weighted_ef_g_co2e_tkm` -> `effective_ef_g_co2e_tkm` (D10)
   - Add `transport_mode_fractions` (D10)
3. **Record processing**: Extract mode distance values from the input record and pass to calculator.

### Mode Distance Extraction from Record
The enriched dataset has flat columns: `road_km`, `sea_km`, `rail_km`, `air_km`, `inland_waterway_km`. Assemble into dict:
```python
mode_distances = {
    'road': record.get('road_km', 0.0),
    'sea': record.get('sea_km', 0.0),
    'rail': record.get('rail_km', 0.0),
    'air': record.get('air_km', 0.0),
    'inland_waterway': record.get('inland_waterway_km', 0.0),
}
```

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file
- Preserve backward compatibility: the old logit path must still work when use_enriched_transport=False
- Do NOT break existing imports or function signatures used by other modules

## Verification
- [ ] Calculator works with use_enriched_transport=True (reads mode distances)
- [ ] Calculator works with use_enriched_transport=False (falls back to logit)
- [ ] Output dict uses new column names (D10)
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If calculator.py or _processing.py structure differs significantly from expected
- If modifications would exceed 300-line limit (propose a split)
