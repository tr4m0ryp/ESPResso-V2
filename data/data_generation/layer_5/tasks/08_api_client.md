# Task 08: API Client Update

## Codebase context

ESPResso-V2 Layer 5 V2 modifies the API client to support 50-record coherence
batches instead of 10-record semantic batches. The reward scoring methods remain
but are only called for sampled records. The multi-key rate limiter and infinite
retry logic are unchanged.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/clients/api_client.py` -- Current V1 client to modify
- `data/data_generation/shared/api_client.py` -- Shared API client pattern

## Dependencies

- Task 02 (Layer5Config with updated max_tokens_instruct = 8000)

## The task

Modify `clients/api_client.py` to support V2 batch sizes and remove V1 artifacts.

### Keep unchanged

- `MultiKeyRateLimiter` class -- no changes needed
- `Layer5Client.__init__()` -- no changes needed
- `_get_session()`, `_call_chat_api()`, `_strip_thinking_tags()` -- no changes
- `generate_reward_score()`, `_build_reward_prompt()`, `_extract_reward_score()` -- keep for sampled scoring
- `test_connection()`, `health_check()`, `get_model_info()`, `get_rate_limiter_stats()` -- keep

### Modify

- `generate_batch_semantic_evaluation()`: Rename to `generate_batch_coherence_evaluation()`.
  Same signature and logic, just rename for clarity.
- `generate_semantic_evaluation()`: Remove entirely. V2 has no single-record
  semantic evaluation.
- `_get_instruct_system_prompt()`: Update text to reflect cross-layer coherence
  focus instead of per-field validation. New text:

```python
def _get_instruct_system_prompt(self) -> str:
    """Get system prompt for coherence evaluation model."""
    return """You are a cross-layer coherence evaluator for textile product data.

CRITICAL RULES:
1. DO NOT use thinking tags (<think>...</think>)
2. DO NOT validate individual fields (weights, distances, percentages)
3. Output ONLY the JSON result
4. Focus on cross-layer coherence and lifecycle plausibility
5. Be direct and concise

Your response must be immediately usable without parsing thinking tags."""
```

### Update docstrings

- Module docstring: change "10 records" references to "50 records"
- `MultiKeyRateLimiter` docstring: update example from "5 keys x 40 req/min"
  to be more generic
- `generate_batch_coherence_evaluation()` docstring: mention 50-record batches

### Remove model_reward references

Since V2 uses the same model for both coherence evaluation and reward scoring,
remove `model_reward` field. Use `model_instruct` for all calls. Update:
- `__init__`: remove `self.model_reward = config.api_model_reward`
- `generate_reward_score()`: use `self.model_instruct` instead of `self.model_reward`
- `get_model_info()`: remove `reward_model` key
- `_call_chat_api()`: remove the `if 'reward' in model_id.lower()` special case.
  Always use system + user message format.

## Acceptance criteria

1. `Layer5Client(config).generate_batch_coherence_evaluation(prompt, 0.3, 8000)` works
2. No method named `generate_semantic_evaluation` exists
3. No method named `generate_batch_semantic_evaluation` exists
4. `generate_reward_score()` still works (for sampled scoring)
5. `model_reward` no longer exists -- single model for all calls
6. System prompt references cross-layer coherence, not per-field validation
7. MultiKeyRateLimiter unchanged

## Files to modify

- `clients/api_client.py` -- modify as described

## Reference

- V1 client: `layer_5/clients/api_client.py` (current file)
- Shared client: `shared/api_client.py`
