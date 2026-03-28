# Task 005: LLM Client Wrapper

## Objective
Create a thin client wrapper around the shared FunctionClient that handles transport distance extraction API calls with retry logic, exponential backoff, and JSON parsing.

## Scope
**Files to create:**
- `data/data_generation/layer_6/enrichment/client.py`

**Files to read (not modify):**
- `data/data_generation/shared/api_client.py` -- FunctionClient to wrap
- `data/data_generation/layer_6/enrichment/config.py` -- EnrichmentConfig (from task 001)
- `data/data_generation/layer_5/clients/api_client.py` -- reference for layer client pattern
- `data/data_generation/layer_4/clients/api_client.py` -- reference for layer client pattern
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- Prompt building (task 003)
- Orchestration (task 009)
- Validation (task 006)

## Dependencies
- **Requires:** 001 (EnrichmentConfig)
- **Produces:** `EnrichmentClient` class used by task 009

## Technical Details

Create class `EnrichmentClient`:

```python
class EnrichmentClient:
    def __init__(self, config: EnrichmentConfig):
        # Initialize FunctionClient with config settings
        # api_key from os.environ[config.api_key_env_var]

    def extract_transport_distances(
        self,
        system_prompt: str,
        user_prompt: str
    ) -> List[Dict]:
        """Call LLM and parse JSON response.

        Returns list of dicts with keys:
        id, road_km, sea_km, rail_km, air_km, inland_waterway_km

        Raises after max_retries exhausted.
        """
```

### Retry Logic (D4)
- Max 5 retries per call
- Exponential backoff with jitter: `delay = min(2^attempt + random(0, 1), 60)`
- Log each retry with attempt number and error
- On final failure, raise exception (orchestrator handles fail-open)

### JSON Parsing
- Strip markdown code fences if present
- Strip thinking tags if present
- Try json.loads() on response content
- Follow the multi-fallback pattern from shared/api_client.py
- Validate that result is a list of dicts with expected keys

### API Call
- Use FunctionClient or direct HTTP POST to `{base_url}/chat/completions`
- Follow the pattern from existing layer clients (Layer 3, 4, 5)
- System prompt as system message, user prompt as user message
- Temperature from config (0.2)
- Max tokens from config (8000)

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file
- Use logging for retry/error messages

## Verification
- [ ] File imports successfully
- [ ] Class instantiates with default EnrichmentConfig
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If shared/api_client.py interface has changed from what's described
- If Layer client patterns differ significantly across layers
