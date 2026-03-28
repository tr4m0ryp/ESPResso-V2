# Task 003: Prompt Builder

## Objective
Create the system prompt and user prompt builder for the transport distance extraction LLM calls. The prompts instruct Claude Sonnet 4.5 to extract per-mode distance totals from transport leg reasoning text.

## Scope
**Files to create:**
- `data/data_generation/layer_6/enrichment/prompt_builder.py`

**Files to read (not modify):**
- `data/data_generation/layer_3/prompts/builder.py` -- reference for prompt building pattern
- `data/data_generation/layer_4/prompts/builder.py` -- reference for batch prompt pattern
- `data/data_generation/layer_5/core/coherence_prompt.py` -- reference for batch record formatting
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- LLM client calls or orchestration
- Validation logic

## Dependencies
- **Requires:** none
- **Produces:** `get_system_prompt()` and `build_batch_prompt(records)` functions used by task 009

## Technical Details

### System Prompt (D12)
```
You are a transport logistics data extraction engine. Your task is to read textile supply chain transport leg data and extract the total distance traveled by each transport mode.

TASK
For each record, you receive a JSON array of transport legs. Each leg has:
- transport_modes: ordered list of modes used (e.g., ["road", "sea", "road"])
- distance_km: total distance for that leg
- reasoning: narrative describing the journey with per-segment distances

EXTRACTION RULES
1. For SINGLE-MODE legs (transport_modes has one entry): assign the full distance_km to that mode.
2. For MULTI-MODE legs (transport_modes has multiple entries): read the reasoning field and extract the distance for each segment. The reasoning always describes each segment with its distance (e.g., "Trucked 430 km to port. Shipped 2180 km. Final 340 km by road.").
3. Sum all distances per mode across ALL legs in the record.
4. The five valid modes are: road, sea, rail, air, inland_waterway. Return 0.0 for any mode not used.
5. Round all distances to 1 decimal place.

OUTPUT FORMAT
Return a JSON array with one object per record, in the order received. Each object:
{
  "id": "<the record id provided>",
  "road_km": <float>,
  "sea_km": <float>,
  "rail_km": <float>,
  "air_km": <float>,
  "inland_waterway_km": <float>
}

CRITICAL RULES
- Extract distances ONLY from the reasoning text. Do not estimate or infer.
- If the reasoning does not specify per-segment distances for a multi-mode leg, divide the leg distance proportionally by the number of modes (fallback only).
- Output ONLY the JSON array. No explanation, no markdown fences, no preamble.
```

### User Prompt Builder
Function `build_batch_prompt(records: List[Dict]) -> str` that:

1. Takes a list of record dicts, each containing at minimum: `record_id`, `total_distance_km`, `transport_legs` (JSON string or list).
2. For each record, strips transport_legs to only: `transport_modes`, `distance_km`, `reasoning` per leg. Drops coordinates, locations, from_step, to_step, leg_index to save tokens.
3. Formats as:
```
Extract transport mode distances for the following {n} records.

--- Record 1 (id: {record_id}) ---
total_distance_km: {total_km}
transport_legs:
[
  {"transport_modes": [...], "distance_km": ..., "reasoning": "..."},
  ...
]

--- Record 2 (id: {record_id}) ---
...
```

### Helper
Function `strip_leg_fields(leg: dict) -> dict` that keeps only `transport_modes`, `distance_km`, `reasoning` from a leg dict.

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file
- System prompt stored as a module-level constant string

## Verification
- [ ] `get_system_prompt()` returns the system prompt string
- [ ] `build_batch_prompt([...])` formats correctly with stripped legs
- [ ] `strip_leg_fields()` removes coordinates and location fields
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If existing prompt patterns in other layers suggest a significantly different approach
