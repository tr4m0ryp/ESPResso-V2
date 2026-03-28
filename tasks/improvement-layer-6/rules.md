# Shared Rules -- Layer 6 Transport Improvement

## Project Conventions
- Python 3.10+, no type: ignore comments
- Maximum 300 lines per source file. Split proactively at ~200 lines.
- No emojis in any file, output, or commit message.
- No .md files except CLAUDE.md and task/notes files.
- Model logic in plain .py files, never in notebooks.

## File Organization
- New enrichment code goes in `data/data_generation/layer_6/enrichment/`
- New scripts go in `data/data_generation/scripts/`
- Config extends existing `data/data_generation/layer_6/config/`
- Follow existing Layer 6 import patterns: `from data.data_generation.layer_6.X import Y`

## Dependencies Available
- pandas, pyarrow (for parquet I/O)
- json (stdlib, for transport_legs parsing)
- requests (for API calls via shared client)
- pathlib (for path handling)
- logging (use module-level logger: `logger = logging.getLogger(__name__)`)

## Shared Infrastructure
- `data/data_generation/shared/api_client.py` -- FunctionClient class for LLM calls
- `data/data_generation/shared/parallel_processor.py` -- ParallelProcessor for concurrency
- All layers use UVA AI API Cloudflare at `http://localhost:3000/v1`
- API key env var: `UVA_API_KEY`

## Data Schema -- Enrichment Output
The LLM enrichment step adds these columns per record:
- `road_km` (float) -- total km by road across all legs
- `sea_km` (float) -- total km by sea
- `rail_km` (float) -- total km by rail
- `air_km` (float) -- total km by air
- `inland_waterway_km` (float) -- total km by inland waterway

## Data Schema -- Final Output Column Renames (D10)
- Old: `transport_mode_probabilities` -> New: `transport_mode_distances_km`
- Old: `weighted_ef_g_co2e_tkm` -> New: `effective_ef_g_co2e_tkm`
- New column: `transport_mode_fractions` (dict of mode -> fraction of total distance)

## Transport Emission Factors (unchanged)
- road: 74.0 g CO2e/tkm
- rail: 22.0 g CO2e/tkm
- inland_waterway: 31.0 g CO2e/tkm
- sea: 10.3 g CO2e/tkm
- air: 782.0 g CO2e/tkm

## New Transport Formula
```
CF_transport = SUM over modes:
    (W_total / 1000) * mode_distance_km * (EF_mode / 1000)
```

## Key Data Facts
- Input: `layer_5_validated_dataset.csv` (50,480 rows, 27 cols)
- Layer 5 does NOT have transport_legs. Must JOIN to Layer 4 via pp-XXXXXX id.
- Layer 5 record_id format: `cl-X-Y_pp-XXXXXX` -- extract pp-XXXXXX for join.
- Layer 4: `layer_4_complete_dataset.parquet` (53,926 rows) has transport_legs column.
- transport_legs is a JSON string containing array of leg objects.
- Each leg has: transport_modes (list), distance_km (float), reasoning (str), plus coordinates/locations.
- 75.9% of legs are single-mode, 24.1% multi-mode.
- Dominant multi-modal pattern: road+sea+road (84% of multi-modal legs).
- Average 14.3 legs per record, max 38.

## Testing
- No formal test framework required for enrichment scripts.
- Verify by running with a small sample (5-10 records) and checking output.
- Calculation changes should produce valid carbon footprints (all components >= 0).
