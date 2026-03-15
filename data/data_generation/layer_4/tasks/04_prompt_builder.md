Create `prompts/builder.py` with a PromptBuilder class that assembles system and user
prompts for the packaging generator.

## PromptBuilder class

```python
class PromptBuilder:
    def __init__(self, config: Layer4Config):
        self._system_prompt: Optional[str] = None
        self._system_prompts_dir = (
            Path(__file__).resolve().parent / "system"
        )
```

### get_system_prompt() -> str

Load and return the system prompt. Cache on first call.

- Read all `.txt` files from `prompts/system/` sorted by filename.
- Concatenate with double newline separators.
- Cache in `self._system_prompt` for reuse across all records.
- Raise `FileNotFoundError` if the system directory is empty or missing.

This is the same pattern as Layer 3's PromptBuilder.

### build_user_prompt(record: Dict[str, Any]) -> str

Build the per-record user prompt from a Layer 3 record dictionary.

The prompt must include:

1. **Product context** (from L1/L2 fields):
   - `subcategory_name` and `category_name`
   - `total_weight_kg`
   - `materials` (parsed from JSON if string)

2. **Transport journey summary** (derived from `transport_legs`):
   Parse `transport_legs` from JSON if needed. Extract:
   - `total_distance_km` from the record
   - All unique transport modes across all legs (deduplicated, sorted)
   - Origin: `from_location` of the first leg (by `leg_index`)
   - Destination: `to_location` of the last leg (by `leg_index`)
   - Number of legs
   - Brief leg summary: list each leg as
     `"{from_location} -> {to_location} ({distance_km} km, {modes})"` on its own line

Output format:

```
Predict the packaging for this textile product:

Product: {subcategory_name} ({category_name})
Product weight: {total_weight_kg} kg
Materials: {materials_comma_separated}

Transport journey ({total_distance_km} km total, {n_legs} legs):
- Modes used: {unique_modes_comma_separated}
- Origin: {first_leg_from_location}
- Destination: {last_leg_to_location}
- Legs:
  1. {from_location} -> {to_location} ({distance_km} km, {modes})
  2. {from_location} -> {to_location} ({distance_km} km, {modes})
  ...
```

### build_correction_prompt(record: Dict[str, Any], failures: List[str]) -> str

Same as `build_user_prompt()` but appends a CORRECTIONS block at the end:

```
CORRECTIONS NEEDED:
Your previous response had the following issues:
- {failure_1}
- {failure_2}
...

Please fix these issues and respond with a corrected JSON object.
```

### Helper: _parse_json_field(value: Any) -> Any

Same pattern as Layer 3 -- if value is already list/dict, return as-is. If string, parse
with `json.loads`. Return empty list/dict on failure.

### Helper: _extract_transport_summary(record: Dict[str, Any]) -> Dict[str, Any]

Extract transport journey details from a Layer 3 record. Returns a dict with keys:
- `total_distance_km`: float
- `unique_modes`: List[str] (sorted, deduplicated)
- `origin`: str (from_location of first leg)
- `destination`: str (to_location of last leg)
- `n_legs`: int
- `leg_summaries`: List[str] (one line per leg)

Handles edge case where `transport_legs` is empty by returning sensible defaults
("Unknown" for locations, 0.0 for distance, empty lists).

## Files to create

- `prompts/builder.py`

## Files to modify

- `prompts/prompts.py` -- keep temporarily as a no-op for backward compatibility, or
  delete entirely if no other code imports it. Check imports first.

## Files to remove

- `prompts/reality_check_prompts.py` -- V2 does not use LLM-based validation for packaging

## Design rules

- System prompt is loaded once and cached. Builder does NOT call the API.
- User prompt is built per-record. Keep it concise -- the LLM should focus on the JSON
  output, not on parsing long prompts.
- Use full package path imports.
- No emojis.

## Reference

- Layer 3 prompt builder: `layer_3/prompts/builder.py`
- Design doc: `layer_4/DESIGN_V2.md` sections 6.1, 6.2, 6.3
