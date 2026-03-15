Rewrite `config/config.py` to replace the V1 Layer4Config with a simplified V2 configuration.

## What to remove

All V1-specific fields and logic:

- `api_key_env_vars` list (10 NVIDIA keys) -- replaced by single key
- `api_base_url` pointing to `integrate.api.nvidia.com` -- replaced by shared FunctionClient
- `api_model` referencing Nemotron -- replaced by shared FunctionClient default
- `api_keys` property and `MultiKeyClientPool` support
- `rate_limit_per_key`, `total_rate_limit` properties
- `parallel_workers` property
- `configs_per_record` setting (now always 1)
- `packaging_materials_path` property (no material database in V2)
- `get_packaging_intensity()` method
- `get_weight_range()` method

## Layer4Config dataclass

```python
@dataclass
class Layer4Config:
    _paths: PipelinePaths = field(default_factory=PipelinePaths, repr=False)

    @property
    def project_root(self) -> Path:
        return self._paths.root
```

### API settings

- `api_key_env_var`: str = `"ANTHROPIC_API_KEY"` -- environment variable name for the key
- `api_model`: str = `"claude-sonnet-4-6"` -- model identifier for FunctionClient
- `api_base_url`: str = `"http://localhost:3000/v1"` -- OpenAI-compatible endpoint
  (matches shared FunctionClient default)
- `temperature`: float = `0.3` -- lower than V1's 0.7 for consistency
- `max_tokens`: int = `300` -- JSON output is ~150-200 tokens

### Processing settings (from env or defaults)

Set in `__post_init__` using `object.__setattr__`:

- `batch_size`: int = `int(os.getenv('LAYER4_BATCH_SIZE', '50'))` -- records per checkpoint batch
- `checkpoint_interval`: int = `int(os.getenv('CHECKPOINT_INTERVAL', '5000'))` -- rows between checkpoints
- `max_retries`: int = `int(os.getenv('MAX_RETRIES', '3'))` -- API call retries
- `retry_delay`: float = `float(os.getenv('RETRY_DELAY', '2.0'))` -- base delay for exponential backoff

### Validation thresholds (from env or defaults)

Set in `__post_init__`:

- `min_packaging_ratio`: float = `float(os.getenv('LAYER4_MIN_PKG_RATIO', '0.005'))` --
  minimum total packaging mass / product weight (0.5%)
- `max_packaging_ratio`: float = `float(os.getenv('LAYER4_MAX_PKG_RATIO', '0.15'))` --
  maximum total packaging mass / product weight (15%)
- `min_reasoning_length`: int = `int(os.getenv('LAYER4_MIN_REASONING', '20'))` --
  minimum characters in reasoning field

### Path properties (delegate to PipelinePaths)

- `layer3_output_path -> Path`: return `self._paths.layer3_output`
- `output_dir -> Path`: return `self._paths.layer_output_dir(4)`
- `output_path -> Path`: return `self._paths.layer4_output`
- `checkpoint_dir -> Path`: return `self.output_dir / "checkpoints"`

### Properties

- `api_key -> str`: read from `os.environ[self.api_key_env_var]`, raise ValueError if
  missing or placeholder
- `has_api_key() -> bool`: return True if api_key is available and valid

### Methods

- `ensure_directories() -> None`: create `output_dir` and `checkpoint_dir` with
  `parents=True, exist_ok=True`

## __post_init__

Load `.env` file from project root (same pattern as V1). Then set env-overridable fields
via `object.__setattr__`.

## Files to modify

- `config/config.py` -- complete rewrite

## Reference

- Layer 3 V2 config pattern: `layer_3/config/config.py`
- Shared paths: `shared/paths.py` (PipelinePaths)
- Design doc: `layer_4/DESIGN_V2.md` section 8 (Configuration)
