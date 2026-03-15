Rewrite `clients/api_client.py` with a simple Layer4Client class wrapping the shared
FunctionClient.

## What to remove

Delete entirely:
- `clients/api_client_direct.py` -- V1 Nemotron direct client
- `clients/api_client_multikey.py` -- V1 multi-key pool

Rewrite:
- `clients/api_client.py` -- replace Nemotron4Client with Layer4Client

## Layer4Client class

```python
class Layer4Client:
    def __init__(self, config: Layer4Config):
        self.config = config
        self.client = FunctionClient(
            api_key=config.api_key,
            model_id=config.api_model,
            base_url=config.api_base_url
        )
        logger.info("Initialized Layer 4 client with model: %s", config.api_model)
```

### generate_packaging(system_prompt: str, user_prompt: str) -> Dict[str, Any]

Send system prompt + user prompt to the model and return the parsed JSON response.

Implementation:
1. Call `self.client.generate_text_with_system()` (or equivalent method on FunctionClient)
   passing `system_prompt`, `user_prompt`, `temperature=self.config.temperature`,
   `max_tokens=self.config.max_tokens`.
2. Parse the response content as JSON using `_parse_json_response()`.
3. Return the parsed dict with keys: `paper_cardboard_kg`, `plastic_kg`, `other_kg`,
   `reasoning`.

**Important**: Check whether the shared `FunctionClient` already supports a `system`
parameter. If it does, use it directly. If not, the simplest approach is to build the
messages list manually and call the chat completions endpoint:

```python
messages = [
    {"role": "system", "content": system_prompt},
    {"role": "user", "content": user_prompt}
]
payload = {
    "model": self.client.model_id,
    "messages": messages,
    "temperature": self.config.temperature,
    "max_tokens": self.config.max_tokens,
}
response = self.client.session.post(
    f"{self.client.base_url}/chat/completions",
    json=payload
)
```

This leverages FunctionClient's session (which already has auth headers configured) while
giving Layer 4 full control over the message structure.

### _parse_json_response(raw_text: str) -> Dict[str, Any]

Parse JSON from the model's response text. Handle three cases:

1. Clean JSON object (expected case).
2. JSON wrapped in markdown code fences (```json ... ```).
3. JSON with trailing/leading text outside the braces.

```python
def _parse_json_response(self, raw_text: str) -> Dict[str, Any]:
    text = raw_text.strip()

    # Strip markdown code fences
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]
        text = text.strip()

    # Extract JSON object
    start = text.index("{")
    end = text.rindex("}") + 1
    text = text[start:end]

    data = json.loads(text)

    required = {"paper_cardboard_kg", "plastic_kg", "other_kg", "reasoning"}
    missing = required - set(data.keys())
    if missing:
        raise ValueError(f"Missing fields in response: {missing}")

    return data
```

### test_connection() -> bool

Send a minimal test prompt ("Generate packaging for a cotton t-shirt, 0.2 kg, road
transport 100 km.") and verify the response parses successfully. Return True on success,
False on any exception. Log the error on failure.

### get_model_info() -> Dict[str, Any]

Return a dict with `model`, `base_url`, and `api_type` ("openai_compatible").

## Design rules

- Single client, single API key. No multi-key pool, no round-robin, no rate limiting.
- Retry logic is NOT in the client. The generator (task 06) handles retries.
- The client is a thin wrapper: send prompt, parse response, return dict. No validation
  of mass values (that is the validator's job, task 07).
- Use `logging.getLogger(__name__)` for all logging.
- No emojis.

## Files to create/modify

- `clients/api_client.py` -- complete rewrite

## Files to remove

- `clients/api_client_direct.py`
- `clients/api_client_multikey.py`

## Reference

- Shared FunctionClient: `shared/api_client.py` (FunctionClient class)
- Layer 3 client pattern: `layer_3/clients/api_client.py`
- Design doc: `layer_4/DESIGN_V2.md` section 7.3 (Client Architecture)
