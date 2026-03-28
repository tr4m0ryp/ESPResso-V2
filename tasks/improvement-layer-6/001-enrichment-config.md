# Task 001: Enrichment Configuration

## Objective
Create the configuration dataclass for the Layer 6 LLM enrichment phase. This defines paths, batch settings, retry logic, and API parameters for the transport distance extraction step.

## Scope
**Files to create:**
- `data/data_generation/layer_6/enrichment/__init__.py`
- `data/data_generation/layer_6/enrichment/config.py`

**Files to read (not modify):**
- `data/data_generation/layer_6/config/config.py` -- existing Layer 6 config pattern
- `data/data_generation/layer_5/config/config.py` -- reference for API config pattern
- `data/data_generation/layer_4/config/config.py` -- reference for API config pattern
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- Modifying existing config.py
- Any LLM client or API logic

## Dependencies
- **Requires:** none
- **Produces:** `EnrichmentConfig` dataclass used by tasks 005, 006, 009

## Technical Details
Create a dataclass `EnrichmentConfig` with these fields:

```python
@dataclass
class EnrichmentConfig:
    # Input paths
    layer5_path: str  # layer_5_validated_dataset.csv
    layer4_path: str  # layer_4_complete_dataset.parquet

    # Output paths
    output_dir: str   # where enriched dataset + temp files go
    output_filename: str  # pre_layer6_enriched.parquet

    # API settings (D4, D5)
    api_base_url: str  # http://localhost:3000/v1
    api_model: str     # claude-sonnet-4-5-20241022
    api_key_env_var: str  # UVA_API_KEY
    temperature: float  # 0.2
    max_tokens: int     # 8000

    # Batch settings (D6, D11)
    batch_size: int     # 20 records per LLM call
    checkpoint_interval: int  # 5000 records

    # Retry settings (D4)
    max_retries: int    # 5

    # Validation (D8)
    distance_tolerance: float  # 0.01 (1%)
```

Follow the pattern from existing Layer6Config. Use `field(default=...)` for all defaults. Include a `validate()` method that checks input paths exist and creates output_dir.

The `__init__.py` should be minimal -- just version and status markers matching the Layer 6 pattern.

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file

## Verification
- [ ] File imports successfully: `python3 -c "from data.data_generation.layer_6.enrichment.config import EnrichmentConfig"`
- [ ] Default config creates with valid defaults
- [ ] Files stay under 300 lines
- [ ] No files modified outside Scope

## Stop Conditions
- If existing config patterns differ significantly from what's described here
- If import path conventions don't match the project structure
