# Task 04: Cross-Layer Coherence System Prompt

## Codebase context

ESPResso-V2 Layer 5 V2 replaces per-layer semantic validation with a cross-layer
coherence check. The LLM prompt must focus exclusively on whether the combination
of materials, processing, transport, and packaging tells a coherent lifecycle
story -- not whether individual layer fields are valid (upstream validators handle
that). The prompt processes 50 records per batch.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Prompts are built programmatically in Python, not stored as separate files
- No emojis anywhere
- The prompt must produce parseable JSON output

## Reference files to study

- `data/data_generation/layer_5/core/semantic_validator.py` -- V1 prompt building
  methods (`_build_batch_semantic_validation_prompt`, `_build_semantic_validation_prompt`)
- `data/data_generation/layer_3/core/prompt_builder.py` -- Pattern for prompt building
- `data/data_generation/layer_4/core/prompt_builder.py` -- Pattern for prompt building

## Dependencies

- Task 01 (CompleteProductRecord, CrossLayerCoherenceResult models)

## The task

Create `core/coherence_prompt.py` with a `CoherencePromptBuilder` class.

### CoherencePromptBuilder class

```python
class CoherencePromptBuilder:
    def __init__(self):
        self._system_prompt = self._build_system_prompt()

    @property
    def system_prompt(self) -> str:
        """Return the system prompt for cross-layer coherence evaluation."""
        return self._system_prompt

    def build_batch_prompt(self, records: List[CompleteProductRecord]) -> str:
        """Build a prompt for evaluating a batch of records (up to 50).

        Args:
            records: List of records to evaluate (max 50)

        Returns:
            User prompt string with all records formatted for evaluation
        """
```

### System prompt content

The system prompt should establish the LLM as a cross-layer coherence evaluator
(not a per-field validator). Key instructions:

```
You are a textile product lifecycle coherence evaluator. Your job is to assess
whether the COMBINATION of material composition, processing steps, transport
logistics, and packaging configuration forms a coherent, plausible product
lifecycle. You are NOT checking individual field validity (that has already
been done). You are checking whether the layers make sense TOGETHER.
```

Rules to include:
1. DO NOT validate individual fields (weights, distances, percentages)
2. DO NOT use thinking tags
3. Output ONLY the JSON object, no other text
4. Focus on contradictions between layers and lifecycle plausibility

### Batch prompt format

For each record in the batch, format a compact summary:

```
RECORD {i} (ID: {subcategory_id}):
  Product: {subcategory_name} ({category_name}), {total_weight_kg:.3f}kg
  Materials: {material1} ({pct1:.0f}%), {material2} ({pct2:.0f}%), ...
  Processing: {step1}, {step2}, ...
  Transport: {total_transport_distance_km}km ({supply_chain_type})
  Packaging: {total_packaging_mass_kg:.3f}kg ({categories})
```

### Expected JSON output format

```json
{
  "record_id_1": {
    "lifecycle_coherence_score": 0.XX,
    "cross_layer_contradiction_score": 0.XX,
    "overall_coherence_score": 0.XX,
    "contradictions_found": ["specific contradiction 1"],
    "recommendation": "accept"
  },
  ...
}
```

### Scoring guidelines (include in prompt)

- **lifecycle_coherence_score**: Does the material sourcing -> processing ->
  transport -> packaging chain tell a plausible story? E.g., exotic materials
  from specific regions should have longer transport, heavier products need
  more packaging.
- **cross_layer_contradiction_score**: Absence of contradictions. 1.0 means
  no contradictions found. E.g., "silk" with "injection molding" processing
  is contradictory; "short_haul" with 15000km distance is contradictory.
- **overall_coherence_score**: Combined assessment of lifecycle plausibility.

### Parsing helper

```python
def parse_batch_response(self, response: str, record_ids: List[str]) -> Dict[str, CrossLayerCoherenceResult]:
    """Parse LLM response into CrossLayerCoherenceResult objects.

    Handles JSON extraction, markdown code block stripping, and fallback
    to default scores on parse failure.

    Args:
        response: Raw LLM response string
        record_ids: Expected record IDs to match against

    Returns:
        Dict mapping record ID to CrossLayerCoherenceResult
    """
```

Parse logic:
1. Strip markdown code blocks (```json ... ```)
2. Parse JSON
3. For each record_id in the response, create a CrossLayerCoherenceResult
4. For missing record_ids, create a default result (all scores 0.7, recommendation "review")
5. On JSON parse failure, return default results for all records

## Acceptance criteria

1. `CoherencePromptBuilder().system_prompt` returns a non-empty string
2. `build_batch_prompt([record1, ..., record50])` returns a prompt containing all 50 record summaries
3. Prompt does NOT mention per-field validation (weight sums, percentage sums, etc.)
4. Prompt explicitly asks for JSON output with the 3 score fields + contradictions + recommendation
5. `parse_batch_response(valid_json, ids)` returns correct CrossLayerCoherenceResult objects
6. `parse_batch_response(invalid_json, ids)` returns default results without raising

## Files to create

- `core/coherence_prompt.py`

## Files to remove

None. The V1 prompt logic lives inside `semantic_validator.py` which is removed in task 05.

## Reference

- V1 prompts: `layer_5/core/semantic_validator.py` methods `_build_batch_semantic_validation_prompt` and `_build_semantic_validation_prompt`
- Layer 3 prompt builder: `layer_3/core/prompt_builder.py`
