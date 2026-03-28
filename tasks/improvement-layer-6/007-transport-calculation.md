# Task 007: Transport Calculation Replacement

## Objective
Replace the multinomial logit transport model with a direct calculation using actual per-mode distances. Modify components.py to accept mode distances instead of a single total distance, and update transport_model.py accordingly.

## Scope
**Files to modify:**
- `data/data_generation/layer_6/core/components.py` -- calculate_transport() function
- `data/data_generation/layer_6/core/transport_model.py` -- replace or refactor

**Files to read (not modify):**
- `data/data_generation/layer_6/config/config.py` -- TRANSPORT_EMISSION_FACTORS (unchanged)
- `data/data_generation/layer_6/core/calculator.py` -- to understand how calculate_transport is called
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- calculator.py and _processing.py modifications (task 008)
- Enrichment code
- Config changes (task 004)

## Dependencies
- **Requires:** 004 (updated config with column name constants)
- **Produces:** New `calculate_transport()` signature and `compute_transport_from_actuals()` used by task 008

## Technical Details

### New Transport Formula (D1)
```
CF_transport = SUM over modes:
    (W_total / 1000) * mode_distance_km * (EF_mode / 1000)
```

### Changes to components.py

Replace the current `calculate_transport()` function (lines 81-105) which takes `(weight_kg, distance_km, transport_model)` and delegates to the logit model.

New signature:
```python
def calculate_transport_from_actuals(
    weight_kg: float,
    mode_distances_km: Dict[str, float],
    emission_factors: Dict[str, float]
) -> Tuple[float, Dict[str, float], Dict[str, float], float]:
    """Calculate transport CF from actual per-mode distances.

    Args:
        weight_kg: Total product weight in kg.
        mode_distances_km: Dict mapping mode -> distance in km.
            Keys: road, sea, rail, air, inland_waterway
        emission_factors: Dict mapping mode -> EF in g CO2e/tkm.

    Returns:
        Tuple of:
        - footprint_kg_co2e (float)
        - mode_distances_km (dict, pass-through for output)
        - mode_fractions (dict, fraction of total distance per mode)
        - effective_ef (float, actual weighted EF in g CO2e/tkm)
    """
```

Implementation:
1. For each mode with distance > 0: `cf += (weight_kg / 1000) * distance_km * (EF / 1000)`
2. Compute total distance: `total_km = sum(mode_distances_km.values())`
3. Compute fractions: `mode_fractions[mode] = distance / total_km` (handle total_km == 0)
4. Compute effective EF: `effective_ef = sum(fraction * EF for mode) if total_km > 0 else 0`
5. Return all four values

### Keep old function
Keep the existing `calculate_transport()` function but rename it to `calculate_transport_logit()` for backward compatibility. The new function is `calculate_transport_from_actuals()`.

### Changes to transport_model.py
The TransportModeModel class stays as-is (it may be used for comparison or legacy mode). No deletions. Add a comment at the top noting the enriched transport path uses `calculate_transport_from_actuals()` in components.py instead.

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file
- Do NOT delete the existing logit calculation -- rename it
- Keep all existing imports and other functions in components.py unchanged

## Verification
- [ ] New function computes correctly: 1.0 kg, {"road": 1000, "sea": 5000} -> known result
- [ ] Mode fractions sum to 1.0
- [ ] Effective EF is weighted average of mode EFs by distance fraction
- [ ] Old logit function still accessible as calculate_transport_logit()
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If components.py is near 300 lines and additions would exceed limit (split if needed)
