# Task 010: Integration Verification

## Objective
Verify that all components work together: the enrichment pipeline can process a small sample of records, and the modified calculation engine can read the enriched output and produce valid carbon footprints.

## Scope
**Files to create:**
- `data/data_generation/layer_6/enrichment/smoke_test.py` -- standalone integration test

**Files to read (not modify):**
- All enrichment modules (tasks 001-006, 009)
- All modified Layer 6 core modules (tasks 007, 008)
- Updated config (task 004)
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- Running the full 50K record enrichment
- Modifying any implementation files

## Dependencies
- **Requires:** all previous tasks (001-009)
- **Produces:** Verified working pipeline

## Technical Details

### Smoke Test Script
Create a script that:

1. **Test data joiner**: Run join_transport_legs() on full data, verify 50,480 rows returned.

2. **Test prompt builder**: Take 3 sample records, build a batch prompt, verify it contains the right record_ids and stripped leg data (no coordinates).

3. **Test validator**: Create mock extraction results with known values:
   - One that passes (< 1% discrepancy)
   - One that fails (> 1% discrepancy)
   - Edge case: total_distance_km = 0
   Verify all return correct is_valid.

4. **Test transport calculation**: Call `calculate_transport_from_actuals()` with known inputs:
   - weight_kg=1.0, mode_distances={"road": 1000, "sea": 5000, "rail": 0, "air": 0, "inland_waterway": 0}
   - Verify: footprint = (1/1000) * (1000*74/1000 + 5000*10.3/1000) = 0.074 + 0.0515 = 0.1255 kgCO2e
   - Verify: mode_fractions = {"road": 0.1667, "sea": 0.8333, ...}
   - Verify: effective_ef = 0.1667*74 + 0.8333*10.3 = 20.93 g CO2e/tkm

5. **Test backward compatibility**: Verify calculate_transport_logit() still works with the old multinomial logit model.

6. **Test config**: Verify EnrichmentConfig defaults are valid, Layer6Config has new fields.

7. **Test output column names**: Verify the new column name constants exist in config.

### Run Instructions
```bash
python3 -m data.data_generation.layer_6.enrichment.smoke_test
```

Or:
```bash
python3 data/data_generation/layer_6/enrichment/smoke_test.py
```

### Expected Output
Print PASS/FAIL for each test. Return exit code 0 if all pass, 1 if any fail.

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only create files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file
- Do NOT make API calls -- this is a unit/integration test with mock data only
- Do NOT modify any implementation files

## Verification
- [ ] All smoke tests pass
- [ ] No import errors across the full module tree
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If any import fails (indicates a task produced incompatible code)
- If transport calculation produces incorrect results
- Report failures with specific details so upstream tasks can be fixed
