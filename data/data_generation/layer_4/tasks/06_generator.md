Rewrite `core/generator.py` with a PackagingGenerator class that generates one packaging
configuration per Layer 3 record.

## What to remove

All V1 classes and methods:
- `PackagingConfig` dataclass (replaced by Layer4Record in models)
- `PackagingConfigGenerator` class
- `_build_enhanced_prompt()` (replaced by PromptBuilder)
- `_create_packaging_config()` (replaced by Layer4Record.from_layer3)
- All Nemotron-specific code

## PackagingGenerator class

```python
class PackagingGenerator:
    def __init__(self, config: Layer4Config, api_client: Layer4Client,
                 prompt_builder: PromptBuilder):
        self.config = config
        self.api_client = api_client
        self.prompt_builder = prompt_builder
        self._system_prompt = prompt_builder.get_system_prompt()
```

### generate_for_record(record: Dict[str, Any]) -> Optional[Layer4Record]

Generate packaging for a single Layer 3 record. This is the main entry point.

Steps:
1. Build user prompt via `self.prompt_builder.build_user_prompt(record)`.
2. Call `self.api_client.generate_packaging(self._system_prompt, user_prompt)`.
3. Parse response into `PackagingResult.from_dict(response)`.
4. Construct `Layer4Record.from_layer3(record, packaging_result)`.
5. Return the Layer4Record.

On API error or JSON parse failure:
- Retry up to `self.config.max_retries` times with exponential backoff
  (`self.config.retry_delay * 2**attempt` seconds).
- Log each retry with the error message.
- Return `None` after all retries exhausted.

### regenerate_with_feedback(record: Dict[str, Any], failures: List[str]) -> Optional[Layer4Record]

Regenerate packaging after validation failure. Called by the orchestrator when the
validator rejects a result.

Steps:
1. Build correction prompt via `self.prompt_builder.build_correction_prompt(record, failures)`.
2. Call API with the correction prompt.
3. Parse and construct Layer4Record as above.
4. Return the Layer4Record, or None on failure.

Only one retry attempt (no exponential backoff loop). If the correction also fails,
return None and let the orchestrator handle the skip.

### _attempt_generation(user_prompt: str) -> PackagingResult

Internal method that handles a single API call + response parsing cycle. Used by both
`generate_for_record` and `regenerate_with_feedback`. Returns a PackagingResult on
success. Raises on failure (caller handles retry logic).

## Design rules

- One record in, one Layer4Record out. No multi-config generation.
- The system prompt is loaded once in `__init__` and reused for all records.
- Generator does NOT validate mass ranges. It produces results; the validator checks them.
- Generator does NOT write output. It returns Layer4Record objects to the orchestrator.
- Log at INFO level: product name and total packaging mass for each generated record.
- Log at WARNING level: retries and failures.
- No emojis.

## Files to modify

- `core/generator.py` -- complete rewrite

## Reference

- Layer 3 generator pattern: `layer_3/core/generator.py` (TransportGenerator)
- Design doc: `layer_4/DESIGN_V2.md` sections 7.4 (Processing Pipeline) and 10 (Response Parsing)
