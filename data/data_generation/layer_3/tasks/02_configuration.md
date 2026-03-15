# Task 02: Configuration

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. Layer 3 generates coordinate-based transport scenarios.
The V2 config must support the new per-leg generation approach, validation
thresholds, and semantic/statistical validation settings. V1 config fields
like `scenarios_per_record`, `supply_chain_type` ranges, `manufacturing_hubs`,
and `material_origins` are no longer needed.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for configuration
- Environment variables for runtime overrides (API keys, batch sizes)
- Path resolution via `shared/paths.py` PipelinePaths
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/config/config.py` -- Pattern for Layer5Config:
  PipelinePaths integration, API provider switching, environment variable
  overrides, validation thresholds
- `data/data_generation/layer_3/config/config.py` -- Current V1 config to
  be rewritten
- `data/data_generation/shared/paths.py` -- PipelinePaths class for
  centralized path resolution

## The task

Rewrite `config/config.py` in place. Replace the existing Layer3Config
with a V2 version.

### Remove these V1 fields

- `scenarios_per_record` (V2 generates 1 record with legs, not 5 scenarios)
- `distance_ranges` dict (V2 does not categorize by supply chain type)
- `material_origins` dict (geographic context moves to system prompt files)
- `manufacturing_hubs` dict (same -- moves to system prompt)
- `transport_emission_factors` dict (belongs to Layer 6, not Layer 3)

### Keep these fields (unchanged)

- PipelinePaths integration (`_paths`, `project_root`, path properties)
- API configuration (`api_base_url`, `api_model`, `api_key_env_var`, provider switching)
- API key management (`api_key`, `api_keys`, `has_api_key`, rate limit properties)
- `parallel_workers`, `effective_rate_limit`
- `batch_size`, `checkpoint_interval`, `max_retries`, `retry_delay`
- `ensure_directories()`
- `temperature`, `top_p`, `max_tokens`

### Add these V2 fields

**Validation thresholds (in `__post_init__`):**
```python
# Deterministic validation
min_leg_distance_km: float  # env LAYER3_MIN_LEG_DISTANCE, default 1
max_leg_distance_km: float  # env LAYER3_MAX_LEG_DISTANCE, default 25000
min_reasoning_length: int   # env LAYER3_MIN_REASONING_LEN, default 50
coordinate_decimal_places: int = 2  # for normalization
```

**Semantic validation config:**
```python
semantic_accept_threshold: float  # env LAYER3_SEMANTIC_ACCEPT, default 0.80
semantic_review_threshold: float  # env LAYER3_SEMANTIC_REVIEW, default 0.60
semantic_max_retries: int         # env LAYER3_SEMANTIC_RETRIES, default 2
```

**Statistical validation config:**
```python
location_diversity_threshold: float  # default 0.30 (flag if >30% same city)
distance_outlier_zscore: float       # default 3.0
mode_max_single_percentage: float    # default 0.80
```

**New path properties:**
```python
@property
def system_prompts_dir(self) -> Path:
    """Path to prompts/system/ directory."""
    return Path(__file__).parent.parent / "prompts" / "system"
```

### Allowed transport modes constant

```python
ALLOWED_TRANSPORT_MODES = frozenset({"road", "rail", "sea", "air", "inland_waterway"})
```

## Acceptance criteria

1. `Layer3Config()` instantiates without errors
2. No V1-only fields exist (scenarios_per_record, material_origins, etc.)
3. All validation thresholds are accessible and have sensible defaults
4. `config.system_prompts_dir` returns the correct path
5. API provider switching still works (nvidia vs uva)
6. `ALLOWED_TRANSPORT_MODES` is a frozenset with exactly 5 modes
