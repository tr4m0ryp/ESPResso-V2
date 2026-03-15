# Task 05: Cross-Layer Coherence Validator

## Codebase context

ESPResso-V2 Layer 5 V2 replaces per-layer semantic validation with a cross-layer
coherence checker. This validator sends batches of 50 records to the LLM to
evaluate lifecycle coherence. It replaces the V1 SemanticValidator which sent
batches of 10 and also checked per-layer field validity.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/core/semantic_validator.py` -- V1 class being replaced
- `data/data_generation/layer_5/clients/api_client.py` -- Layer5Client for LLM calls
- `data/data_generation/layer_3/core/semantic_validator.py` -- Pattern for semantic validation

## Dependencies

- Task 01 (CompleteProductRecord, CrossLayerCoherenceResult models)
- Task 02 (Layer5Config with coherence settings)
- Task 04 (CoherencePromptBuilder)

## The task

Create `core/coherence_validator.py` with a `CoherenceValidator` class.
Remove `core/semantic_validator.py`.

### CoherenceValidator class

```python
class CoherenceValidator:
    def __init__(self, config: Layer5Config, api_client: Layer5Client):
        self.config = config
        self.api_client = api_client
        self.prompt_builder = CoherencePromptBuilder()

    def validate_batch(self, records: List[CompleteProductRecord]) -> Dict[str, CrossLayerCoherenceResult]:
        """Evaluate cross-layer coherence for a batch of up to 50 records.

        Builds a batch prompt via CoherencePromptBuilder, sends it to the LLM
        via api_client.generate_batch_semantic_evaluation(), and parses the
        response into CrossLayerCoherenceResult objects.

        Args:
            records: List of records to evaluate (max 50 per call,
                     caller is responsible for chunking)

        Returns:
            Dict mapping subcategory_id to CrossLayerCoherenceResult
        """
```

### Implementation details

1. Build prompt using `self.prompt_builder.build_batch_prompt(records)`
2. Calculate token budget: `self.config.max_tokens_instruct` (8000 for 50 records)
3. Call `self.api_client.generate_batch_semantic_evaluation(prompt, temperature, max_tokens)`
4. Parse response using `self.prompt_builder.parse_batch_response(response, record_ids)`
5. Fill in missing results with defaults (all scores 0.7, recommendation "review")
6. Log summary: how many records evaluated, mean coherence score, any parse failures

### Error handling

- If the API returns None, return default results for all records
- If JSON parsing fails, return default results for all records
- Log warnings for partial results (some records parsed, some not)
- Never raise -- always return a result dict covering all input records

### No single-record method

Unlike V1 SemanticValidator, there is no `validate_record()` method. All
coherence evaluation is batched. The orchestrator must chunk records into
groups of `config.coherence_batch_size` (50) before calling `validate_batch()`.

## Acceptance criteria

1. `CoherenceValidator(config, client).validate_batch(records)` returns a dict with one entry per record
2. Batch size of 50 records works without truncation
3. On API failure, returns default results (no exception raised)
4. On partial JSON parse, returns defaults for missing records
5. Uses `CoherencePromptBuilder` for prompt construction and response parsing
6. No per-layer field validation logic exists (no weight checks, distance checks, etc.)

## Files to create

- `core/coherence_validator.py`

## Files to remove

- `core/semantic_validator.py` -- replaced by coherence_validator.py

## Reference

- V1 semantic validator: `layer_5/core/semantic_validator.py` (being removed)
- API client: `layer_5/clients/api_client.py`
