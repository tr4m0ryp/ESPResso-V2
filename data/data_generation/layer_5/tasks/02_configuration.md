# Task 02: Configuration

## Codebase context

ESPResso-V2 Layer 5 is being redesigned as a cross-layer coherence checker.
The configuration must be updated to reflect the new 5-stage pipeline: passport
verification, cross-layer coherence (LLM, 50 records/batch), statistical quality,
sampled reward scoring (1-5% sample), and final decision. All per-layer
deterministic check config (weight ranges, transport ranges, packaging ratios,
valid categories) is removed since upstream layers handle their own validation.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Use Python dataclasses for configuration
- Environment variable overrides via `os.environ.get()`
- Logging via `logging.getLogger(__name__)`
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/config/config.py` -- Current V1 config to rewrite
- `data/data_generation/layer_3/config/config.py` -- Pattern for Layer3Config
- `data/data_generation/layer_4/config/config.py` -- Pattern for Layer4Config

## The task

Rewrite `config/config.py` with the V2 Layer5Config.

### Remove

All per-layer validation constants and methods:
- `WEIGHT_RANGES` dict
- `TRANSPORT_DISTANCE_RANGES` dict
- `PACKAGING_RATIO_RANGE` tuple
- `VALID_PACKAGING_CATEGORIES` list
- `DISTRIBUTION_THRESHOLDS` dict
- `get_weight_range_for_category()`
- `get_transport_range_for_type()`
- `is_plausibility_acceptable()` / `is_plausibility_reviewable()`
- `is_reward_acceptable()` / `is_reward_reviewable()`

### Keep (unchanged)

- `_paths: PipelinePaths` and all path properties (layer1-4 output, reference data, output dir, checkpoint dir)
- API configuration: `api_base_url`, `api_model_instruct`, `api_key_env_var`, `api_provider`
- Provider switching (nvidia/uva) logic in `__post_init__`
- `api_key`, `api_keys`, `rate_limit_per_key`, `total_rate_limit`, `parallel_workers`, `effective_rate_limit` properties
- `has_api_key()`, `ensure_directories()`

### Modify

- Remove `api_model_reward` -- V2 uses the same model for both coherence and reward
- Remove `temperature_reward` and `max_tokens_reward` -- use instruct settings for all LLM calls
- Update `max_tokens_instruct` default from 2000 to 8000 (50 records per batch needs more tokens)
- Rename comments referencing "10 records" to "50 records"

### Add new fields

```python
# Cross-layer coherence settings
coherence_batch_size: int  # Records per LLM call, default 50 (env: COHERENCE_BATCH_SIZE)
coherence_accept_threshold: float  # Default 0.85 (env: COHERENCE_ACCEPT_THRESHOLD)
coherence_review_threshold: float  # Default 0.70 (env: COHERENCE_REVIEW_THRESHOLD)

# Sampled reward scoring settings
reward_sample_rate: float  # Fraction of records to score, default 0.03 (3%) (env: REWARD_SAMPLE_RATE)
reward_accept_threshold: float  # Default 0.60 (env: REWARD_ACCEPT_THRESHOLD)

# Passport verification
passport_enabled: bool  # Default True (env: PASSPORT_ENABLED)

# Statistical quality settings (keep from V1, consolidate)
dedup_similarity_threshold: float  # Default 0.95 (env: DEDUP_SIMILARITY_THRESHOLD)
outlier_weight_sigma: float  # Default 3.0
outlier_ratio_sigma: float   # Default 2.0
outlier_transport_sigma: float  # Default 2.5
max_single_material_pct: float  # Default 0.30
```

Initialize all new fields in `__post_init__` from environment variables with the
specified defaults.

### Add new methods

```python
def should_sample_for_reward(self, record_index: int, total_records: int) -> bool:
    """Determine if a record should be sampled for reward scoring.
    Uses deterministic sampling based on record index for reproducibility.
    Sample every Nth record where N = 1/reward_sample_rate."""
    if self.reward_sample_rate <= 0:
        return False
    if self.reward_sample_rate >= 1.0:
        return True
    sample_interval = int(1.0 / self.reward_sample_rate)
    return record_index % sample_interval == 0

def is_coherence_acceptable(self, score: float) -> bool:
    """Check if coherence score meets acceptance threshold."""
    return score >= self.coherence_accept_threshold

def is_coherence_reviewable(self, score: float) -> bool:
    """Check if coherence score falls in review range."""
    return self.coherence_review_threshold <= score < self.coherence_accept_threshold
```

## Acceptance criteria

1. `Layer5Config()` instantiates without errors
2. `config.coherence_batch_size` returns 50 by default
3. `config.reward_sample_rate` returns 0.03 by default
4. `config.should_sample_for_reward(0, 1000)` returns True
5. `config.should_sample_for_reward(1, 1000)` returns False (interval = 33)
6. `config.max_tokens_instruct` defaults to 8000
7. No V1 validation constants remain (WEIGHT_RANGES, etc.)
8. All path properties still work
9. Provider switching (nvidia/uva) still works

## Files to modify

- `config/config.py` -- rewrite

## Reference

- V1 config: `layer_5/config/config.py` (current file)
- Layer 3 config pattern: `layer_3/config/config.py`
