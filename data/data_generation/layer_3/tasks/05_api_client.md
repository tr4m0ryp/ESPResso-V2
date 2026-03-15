# Task 05: API Client

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. Layer 3 uses Claude Sonnet via an OpenAI-compatible
API to generate transport scenarios. The V2 API client is simpler than V1:
one call per record returning a JSON array of transport legs, no 5-strategy
variant generation, no scenario ID suffix logic.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use the shared FunctionClient from `shared/api_client.py`
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_3/clients/api_client.py` -- Current V1 client
  to be rewritten. Note the 5-strategy generation logic that is removed
- `data/data_generation/shared/api_client.py` -- FunctionClient class with
  generate_text(), generate_complex_scenarios(), _extract_json_from_response()
- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Section 4.2 for the
  transport leg JSON structure

## Dependencies

- Task 01 (TransportLeg dataclass for return type context)

## The task

Rewrite `clients/api_client.py` in place.

### New Layer3Client class

```python
class Layer3Client:
    def __init__(self, config: Layer3Config):
        self.config = config
        self.client = FunctionClient(
            api_key=config.api_key,
            model_id=config.api_model,
            base_url=config.api_base_url
        )

    def generate_transport_legs(
        self,
        system_prompt: str,
        user_prompt: str
    ) -> List[Dict[str, Any]]:
        """
        Generate transport legs for a single record.

        Args:
            system_prompt: Static system prompt (cached by caller)
            user_prompt: Per-record user prompt

        Returns:
            List of leg dictionaries matching the transport_legs schema
        """
        # Use prompt_caching=True for the system prompt
        # Parse response as JSON array of leg objects
        # Retry with exponential backoff on failure
        # Return raw dicts (caller converts to TransportLeg)

    def test_connection(self) -> bool:
        """Test API connection."""

    def get_model_info(self) -> Dict[str, Any]:
        """Get model information."""
```

### Key changes from V1

- **Single call**: `generate_transport_legs()` replaces
  `generate_transport_scenarios()`. Returns raw leg dicts, not scenario dicts.
- **No strategy logic**: Remove all 5-strategy suffix handling, scenario ID
  generation, and strategy guidance injection.
- **System/user prompt split**: The caller provides both prompts separately
  (system prompt from PromptBuilder cache, user prompt per-record).
- **Prompt caching**: Pass system prompt with `prompt_caching=True` to
  FunctionClient so the API can cache it across calls.
- **Simpler validation**: Only check that the response is a non-empty list
  of dicts. Schema validation happens in the validator, not here.

### FunctionClient integration

The FunctionClient needs to support a system_prompt parameter. Check if
it already does. If not, modify the `_call_model` method to accept an
optional system_prompt parameter that replaces the hardcoded system
message. If modifying shared code is undesirable, build the messages
list manually and call `_make_api_call` directly.

## Acceptance criteria

1. `Layer3Client(config)` initializes without errors
2. `generate_transport_legs(system, user)` returns a list of dicts
3. No V1 methods remain (generate_transport_scenarios, strategy logic)
4. The system prompt is passed to the API as the system message
5. Retry logic with exponential backoff works
6. `test_connection()` and `get_model_info()` work
