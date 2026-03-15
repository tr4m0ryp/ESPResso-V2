# Layer 4 v2: Packaging Configuration Generator -- Design Document

## 1. Motivation

The current Layer 4 implementation suffers from critical bottlenecks that explain poor
downstream model predictions for `cf_packaging_kg_co2e`:

1. **API Failures**: The NVIDIA Nemotron API failed entirely during generation (15,878 errors,
   0 successful calls), causing the pipeline to fall back to ~2,604 cached template configs
   reused across 101,966 rows (97.4% duplication).
2. **Material Granularity Mismatch**: The prompt allows 17 specific packaging materials, but
   only 4 emission factor categories exist. The model must learn an unnecessary material-to-
   category mapping that adds noise without information.
3. **Multi-Key Rate Limiting Complexity**: 10 NVIDIA API keys, 80-150 parallel workers, and
   round-robin key assignment add operational complexity that is no longer needed.
4. **Redundant Multi-Config Generation**: 2 packaging configs per record doubles the dataset
   without adding meaningful variation.

## 2. Design Principles

1. **Predict at the emission factor level**: Output mass in kg per emission factor category,
   not per specific material. This eliminates the material-to-category mapping entirely.
2. **One config per record**: Each Layer 3 record produces exactly one packaging output.
   The transport leg already constrains the packaging decision -- a second config is noise.
3. **Transport leg as primary reasoning signal**: The combination of `transport_modes`,
   `total_transport_distance_km`, `supply_chain_type`, and `origin_region` drives packaging
   decisions. The prompt must make this relationship explicit.
4. **Single-client simplicity**: Claude Sonnet via the Anthropic API. No multi-key pool,
   no rate limit management, no parallel worker orchestration.

## 3. Category Reduction: 4 to 3

The research paper defines 4 packaging emission factor categories:

| Category         | EF (kgCO2e/kg) | Status in v2       |
|------------------|-----------------|---------------------|
| Paper/Cardboard  | 1.3             | Kept                |
| Plastic          | 3.5             | Kept                |
| Glass            | 1.1             | Merged into Other   |
| Other/Unspecified| 2.5             | Kept (absorbs Glass)|

**Rationale**: Glass packaging (glass fibre reinforcement, decorative glass elements) is
virtually never used in textile supply chains. Keeping it as a separate category creates a
near-zero-variance feature that adds no predictive value. Products that exceptionally use
glass-like materials are better classified as "Other".

### v2 Packaging Categories

| Category        | EF (kgCO2e/kg) | Typical Materials                                    |
|-----------------|-----------------|------------------------------------------------------|
| Paper/Cardboard | 1.3             | Cardboard boxes, tissue paper, kraft paper, labels    |
| Plastic         | 3.5             | Polybags, shrink wrap, garment covers, hangers        |
| Other           | 2.5             | Composite materials, silica gel, mixed, glass-based   |

### Layer 6 Compatibility

Layer 6 `components.calculate_packaging()` receives `categories` and `masses_kg` as parallel
lists and looks up each category in `packaging_ef`. The function already handles case-
insensitive matching and falls back to `Other/Unspecified` for unknown categories.

**Required Layer 6 change**: Add `"Other"` as a recognized key in `PACKAGING_EMISSION_FACTORS`
(mapping to 2.5), or ensure the output uses `"Other/Unspecified"` as the category string.
The simplest approach is to output `"Other/Unspecified"` from Layer 4 to maintain backward
compatibility with the existing Layer 6 code.

### Model Feature Pipeline Compatibility

The model feature extractor (`model_a/features/packaging.py`) generates multi-hot and per-
category mass features based on `PACKAGING_CATEGORIES` in `model_a/config.py`. Currently:

```python
PACKAGING_CATEGORIES = ["paper/cardboard", "plastic", "glass", "other"]
```

**Required model change**: Remove `"glass"` from the vocabulary. Update to:

```python
PACKAGING_CATEGORIES = ["paper/cardboard", "plastic", "other"]
```

This removes `pkg_cat_glass` and `pkg_mass_glass` features (which were near-zero anyway)
and keeps `pkg_cat_other` and `pkg_mass_other` (which now absorbs glass cases).

## 4. Input Schema (from Layer 3)

Layer 4 reads the Layer 3 Parquet output. The 17 columns passed through are:

| Column                       | Type          | Used for Packaging Reasoning |
|------------------------------|---------------|------------------------------|
| `category_id`                | str           | --                           |
| `category_name`              | str           | Yes: product type context    |
| `subcategory_id`             | str           | --                           |
| `subcategory_name`           | str           | Yes: product specificity     |
| `materials`                  | JSON list     | Yes: fragility hints         |
| `material_weights_kg`        | JSON list     | --                           |
| `material_percentages`       | JSON list     | --                           |
| `total_weight_kg`            | float         | Yes: packaging proportional  |
| `preprocessing_path_id`      | str           | --                           |
| `preprocessing_steps`        | JSON list     | --                           |
| `step_material_mapping`      | JSON dict     | --                           |
| `transport_scenario_id`      | str           | --                           |
| `total_transport_distance_km`| float         | Yes: protection level        |
| `supply_chain_type`          | str           | Yes: haul classification     |
| `origin_region`              | str           | Yes: regional practices      |
| `transport_modes`            | JSON list     | Yes: primary reasoning input |
| `transport_reasoning`        | str           | --                           |

The columns marked "Yes" are the ones explicitly surfaced in the prompt to Claude Sonnet.

## 5. Output Schema

Layer 4 v2 outputs a Parquet file with the 17 Layer 3 columns plus 3 new Layer 4 columns:

| Column                  | Type       | Description                                        |
|-------------------------|------------|----------------------------------------------------|
| `packaging_categories`  | JSON list  | Always `["Paper/Cardboard", "Plastic", "Other/Unspecified"]` |
| `packaging_masses_kg`   | JSON list  | `[mass_paper, mass_plastic, mass_other]` in kg     |
| `packaging_reasoning`   | str        | 1-3 sentence explanation of packaging choices      |

**Total columns**: 20 (17 passthrough + 3 new).

### Output Format Details

- `packaging_categories` is always the full list of 3 categories in fixed order. This
  eliminates alignment ambiguity and simplifies downstream parsing.
- `packaging_masses_kg` contains the predicted mass for each category. A category not used
  for this product has mass `0.0`.
- At least one category must have a non-zero mass.
- No `total_packaging_mass_kg` column. Layer 6 computes the total as needed via
  `sum(mass_i * EF_i)`.
- No `packaging_config_id` column. The transport_scenario_id already uniquely identifies
  the record since the expansion factor is now 1:1.
- No `packaging_items` column. The 17 specific materials are no longer relevant.
- No `generation_timestamp` column. Not needed for downstream processing.

## 6. Prompt Design

### 6.1 System Prompt

The system prompt establishes the role, defines the 3 categories, and explains the
transport-leg-driven reasoning framework.

```
You are a packaging logistics expert for the textile and apparel industry. Your task is to
predict realistic packaging configurations for textile products based on their
characteristics and supply chain transport leg.

=== PACKAGING CATEGORIES ===

You must predict the mass (in kg) for exactly three packaging material categories:

1. Paper/Cardboard (EF: 1.3 kgCO2e/kg)
   Includes: cardboard boxes, corrugated cardboard, tissue paper, kraft paper, paper
   wrapping, paper tags, labels, cardboard inserts, paper stuffing.

2. Plastic (EF: 3.5 kgCO2e/kg)
   Includes: polybags (PE/PP), shrink wrap, garment covers, plastic hangers, plastic
   clips, bubble wrap, plastic tags, zip-lock bags.

3. Other (EF: 2.5 kgCO2e/kg)
   Includes: composite materials, mixed-material packaging, silica gel packets, foam
   inserts, rubber bands, metal clips, any material that does not clearly fall into
   Paper/Cardboard or Plastic.

=== TRANSPORT LEG REASONING ===

The transport leg is the primary driver of packaging decisions. Use the transport modes,
distance, and supply chain type to reason about protection requirements:

TRANSPORT MODES AND THEIR PACKAGING IMPLICATIONS:
- Sea freight: Products face humidity, salt air, long durations (weeks). Requires moisture
  barriers (plastic polybags), sturdy outer protection (corrugated cardboard). Higher
  plastic and paper/cardboard mass.
- Air freight: Weight is costly. Packaging must be minimal but protective. Favor lightweight
  plastic (polybags) over heavy cardboard. Lower total packaging mass.
- Road transport: Vibration and handling impacts. Standard packaging with cardboard
  protection. Balanced paper/cardboard and plastic.
- Rail transport: Similar to road but longer distances, less handling. Standard protection.
- Inland waterway: Humidity exposure like sea but shorter duration. Moderate moisture
  protection.

MULTI-MODAL TRANSPORT (most common):
- Sea + Road (e.g., Bangladesh to Europe): Heavy cardboard outer + polybag inner for
  moisture protection during sea leg. This is the most common combination.
- Air + Road (e.g., urgent orders): Lightweight polybag-dominant packaging.
- Road + Rail: Standard balanced packaging.

DISTANCE-BASED SCALING:
- Short-haul (<500 km, typically road-only): Minimal packaging. Polybag + basic label.
  Total packaging: 1-3% of product weight.
- Medium-haul (500-2000 km, road/rail): Standard packaging. Polybag + cardboard insert.
  Total packaging: 3-6% of product weight.
- Long-haul (>2000 km, sea/air + road): Enhanced packaging. Corrugated cardboard + polybag
  + tissue paper. Total packaging: 5-10% of product weight.

SUPPLY CHAIN TYPE:
- short_haul: Domestic/regional. Minimal packaging.
- medium_haul: Continental. Standard packaging.
- long_haul: Intercontinental. Maximum protection.

=== PRODUCT-SPECIFIC PACKAGING NORMS ===

The product type, weight, and materials influence packaging choices:

- Lightweight foldable items (t-shirts, shirts, underwear, 0.1-0.3 kg):
  Primarily polybag + thin cardboard insert. Paper/Cardboard: 0.005-0.015 kg,
  Plastic: 0.003-0.010 kg, Other: 0.000-0.002 kg.

- Medium-weight items (jeans, trousers, dresses, 0.3-0.8 kg):
  Polybag + cardboard hanger or insert + paper label. Paper/Cardboard: 0.010-0.030 kg,
  Plastic: 0.005-0.015 kg, Other: 0.000-0.003 kg.

- Heavy/structured items (coats, jackets, suits, 0.8-2.0 kg):
  Garment cover or box + tissue paper + polybag. Paper/Cardboard: 0.020-0.060 kg,
  Plastic: 0.010-0.030 kg, Other: 0.000-0.005 kg.

- Footwear (shoes, boots, 0.5-2.0 kg):
  Shoebox (cardboard) + tissue paper + polybag + silica gel. Paper/Cardboard: 0.050-0.120 kg,
  Plastic: 0.005-0.015 kg, Other: 0.002-0.008 kg (silica gel).

- Accessories (scarves, belts, hats, 0.05-0.3 kg):
  Small box or pouch + tissue. Paper/Cardboard: 0.005-0.020 kg,
  Plastic: 0.002-0.008 kg, Other: 0.000-0.002 kg.

=== OUTPUT FORMAT ===

Respond with ONLY a JSON object. No markdown, no explanation outside the JSON.

{
  "paper_cardboard_kg": <float>,
  "plastic_kg": <float>,
  "other_kg": <float>,
  "reasoning": "<1-3 sentences explaining why this packaging is appropriate>"
}

Rules:
- All masses must be >= 0.0
- At least one mass must be > 0.0
- Masses must be realistic for the product type and transport leg
- Use at most 4 decimal places for mass values
- The reasoning must reference the transport leg and product type
```

### 6.2 User Prompt Template

The user prompt is constructed per record from the Layer 3 data:

```
Predict the packaging for this textile product:

Product: {subcategory_name} ({category_name})
Product weight: {total_weight_kg} kg
Materials: {materials}

Transport leg:
- Modes: {transport_modes}
- Total distance: {total_transport_distance_km} km
- Supply chain type: {supply_chain_type}
- Origin region: {origin_region}
```

### 6.3 Prompt Design Rationale

1. **Transport leg is the first reasoning step**: The prompt explicitly defines how each
   transport mode affects packaging requirements. This gives the model a causal chain:
   transport mode -> environmental exposure -> protection needs -> material category masses.

2. **Concrete mass ranges per product type**: Instead of a generic "2-10% of product weight"
   rule, the prompt provides per-category mass ranges for each product type. This grounds
   the model's predictions in realistic industry data.

3. **Fixed output order**: The JSON output always contains exactly 3 fields
   (`paper_cardboard_kg`, `plastic_kg`, `other_kg`). No list alignment issues. No missing
   categories. No need to parse variable-length arrays.

4. **No material-level granularity**: The model does not need to decide between "Polybag (PE)"
   and "Shrink wrap" -- both are Plastic. This eliminates a source of variance that has
   zero impact on the emission factor calculation.

## 7. Architecture

### 7.1 File Structure

```
layer_4/
    __init__.py
    main.py                    # Entry point, CLI, batch processing loop
    config/
        config.py              # Single API key, no rate limits, simplified settings
    clients/
        api_client.py          # Claude Sonnet client via Anthropic API
    core/
        generator.py           # Per-record generation logic, response parsing
        validator.py           # Output validation (mass ranges, consistency)
    io/
        input_reader.py        # Layer 3 Parquet reader with field extraction
        writer.py              # Output Parquet writer
    prompts/
        prompts.py             # System prompt, user prompt template builder
```

### 7.2 Removed Components (vs. v1)

| v1 Component                   | Reason for Removal                               |
|--------------------------------|--------------------------------------------------|
| `api_client_direct.py`         | Replaced by single Claude Sonnet client           |
| `api_client_multikey.py`       | No multi-key needed without rate limits            |
| `io/packaging_data.py`         | No material database needed (3 categories only)    |
| `io/progress_tracker.py`       | Inline progress logging sufficient                 |
| `core/orchestrator.py`         | Simplified pipeline fits in main.py                |

### 7.3 Client Architecture

**Model**: Claude Sonnet (`claude-sonnet-4-6`)
**API**: Anthropic Messages API
**Authentication**: Single `ANTHROPIC_API_KEY` environment variable
**Rate limiting**: None required (self-hosted/direct access, no external rate limits)
**Retry**: Simple exponential backoff (3 retries, 2s/4s/8s delays) on transient errors

The client sends the system prompt once per session and the user prompt per record. The
response is parsed as JSON directly from the message content.

```python
# Pseudocode for the client
class SonnetClient:
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = "claude-sonnet-4-6"

    def generate_packaging(self, system_prompt: str, user_prompt: str,
                           temperature: float = 0.3) -> dict:
        response = self.client.messages.create(
            model=self.model,
            max_tokens=300,
            temperature=temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}]
        )
        return json.loads(response.content[0].text)
```

**Temperature**: 0.3 (lower than v1's 0.7). Since we want deterministic, transport-leg-
driven predictions rather than creative variation, a lower temperature produces more
consistent and reproducible results.

**Max tokens**: 300. The JSON output is ~150-200 tokens. 300 provides headroom without
allowing the model to generate excessive text.

### 7.4 Processing Pipeline

```
1. Read Layer 3 Parquet
       |
2. For each record:
   a. Extract relevant fields (product context + transport leg)
   b. Build user prompt from template
   c. Call Claude Sonnet with system prompt + user prompt
   d. Parse JSON response
   e. Validate masses (non-negative, within range, at least one > 0)
   f. If validation fails: log warning, retry once with feedback
   g. Construct output row (17 Layer 3 cols + 3 Layer 4 cols)
       |
3. Write complete Parquet output
```

**Batch processing**: Records are processed sequentially. With no rate limits and ~0.5-1s
per API call, throughput is ~3600-7200 records/hour. For the expected ~67,858 Layer 3
records, this is approximately 10-19 hours.

**Checkpointing**: Write intermediate Parquet files every N records (configurable, default
5000) to enable resume on interruption. The checkpoint contains the index of the last
successfully processed record.

**Batching optimization** (optional): If throughput needs improvement, batch multiple records
into a single prompt by asking the model to predict packaging for N products at once. This
trades per-record reasoning quality for speed. Recommended batch size: 5-10 records per
call.

## 8. Configuration

```python
@dataclass
class Layer4Config:
    # API
    api_key_env_var: str = "ANTHROPIC_API_KEY"
    model: str = "claude-sonnet-4-6"
    temperature: float = 0.3
    max_tokens: int = 300

    # Processing
    batch_size: int = 1           # Records per API call (1 = sequential)
    checkpoint_interval: int = 5000
    max_retries: int = 3
    retry_delay: float = 2.0

    # Validation
    max_packaging_ratio: float = 0.15   # Max packaging/product weight ratio
    min_packaging_ratio: float = 0.005  # Min packaging/product weight ratio (0.5%)

    # Paths (from PipelinePaths)
    # layer3_output, layer4_output, output_dir
```

## 9. Validation

### 9.1 Per-Record Validation

After parsing the JSON response, validate:

| Check                              | Condition                           | Action on Fail   |
|------------------------------------|-------------------------------------|------------------|
| All 3 mass fields present          | keys exist in JSON                  | Retry with error |
| All masses >= 0.0                  | non-negative                        | Retry with error |
| At least one mass > 0.0            | sum > 0                             | Retry with error |
| Total mass reasonable              | 0.5% - 15% of product weight        | Log warning      |
| Reasoning present                  | non-empty string                    | Accept without   |
| Mass precision                     | <= 4 decimal places                 | Round silently   |

### 9.2 Dataset-Level Validation (post-generation)

After the full dataset is generated, run aggregate checks:

| Check                              | Threshold                          | Action           |
|------------------------------------|------------------------------------|------------------|
| Duplicate packaging configs        | < 5% identical mass triplets       | Flag for review  |
| Category usage distribution        | Paper/Cardboard > 80% of records   | Expected         |
| Category usage distribution        | Plastic > 70% of records           | Expected         |
| Category usage distribution        | Other < 30% of records             | Expected         |
| Mean packaging ratio               | 3-8% of product weight             | Flag if outside  |
| Correlation: distance vs mass      | Positive correlation expected       | Flag if negative |
| Zero-mass records                  | < 0.1%                             | Flag for review  |

## 10. Response Parsing

The generator must handle Claude Sonnet's response robustly:

```python
def parse_response(self, raw_text: str) -> dict:
    """Parse the JSON response from Claude Sonnet.

    Handles:
    1. Clean JSON (expected case)
    2. JSON wrapped in markdown code blocks
    3. JSON with trailing text after the closing brace
    """
    text = raw_text.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1]       # remove opening fence
        text = text.rsplit("```", 1)[0]     # remove closing fence
        text = text.strip()

    # Find the JSON object boundaries
    start = text.index("{")
    end = text.rindex("}") + 1
    text = text[start:end]

    data = json.loads(text)

    # Validate expected keys
    required = {"paper_cardboard_kg", "plastic_kg", "other_kg", "reasoning"}
    if not required.issubset(data.keys()):
        missing = required - data.keys()
        raise ValueError(f"Missing fields: {missing}")

    return {
        "paper_cardboard_kg": round(float(data["paper_cardboard_kg"]), 4),
        "plastic_kg": round(float(data["plastic_kg"]), 4),
        "other_kg": round(float(data["other_kg"]), 4),
        "reasoning": str(data["reasoning"]),
    }
```

### Conversion to Layer 6 Format

The parsed response is converted to the list format that Layer 6 expects:

```python
packaging_categories = ["Paper/Cardboard", "Plastic", "Other/Unspecified"]
packaging_masses_kg = [
    parsed["paper_cardboard_kg"],
    parsed["plastic_kg"],
    parsed["other_kg"],
]
```

Note: `"Other/Unspecified"` is used (not `"Other"`) to match the existing key in Layer 6's
`PACKAGING_EMISSION_FACTORS` dictionary without requiring any Layer 6 code changes.

## 11. Downstream Impact

### 11.1 Layer 5 (Validation Layer)

Layer 5 validates Layer 4 output before passing to Layer 6. With the simplified schema (3
mass values instead of variable-length item lists), validation becomes straightforward:
range checks and ratio checks. No JSON array alignment validation needed.

### 11.2 Layer 6 (Carbon Calculation)

Layer 6's `calculate_packaging()` function works unchanged. It receives:
- `categories = ["Paper/Cardboard", "Plastic", "Other/Unspecified"]`
- `masses_kg = [0.015, 0.008, 0.002]`

And computes: `CF = 0.015 * 1.3 + 0.008 * 3.5 + 0.002 * 2.5 = 0.0525 kgCO2e`

No Layer 6 code changes required.

### 11.3 Model Feature Pipeline

The feature extractor generates per-category features. Required changes:

1. Remove `"glass"` from `PACKAGING_CATEGORIES` in `model_a/config.py`
2. Features `pkg_cat_glass` and `pkg_mass_glass` are dropped
3. Features `pkg_cat_other` and `pkg_mass_other` now include former glass cases
4. Aggregate features (`total_packaging_mass_kg`, `packaging_category_count`, etc.)
   remain unchanged in logic

### 11.4 C Calculation Module

The C code in `data_calculation/include/packaging/packaging.h` defines a `PackagingCategory`
enum with `PACKAGING_GLASS`. If this module is used independently:
- Keep `PACKAGING_GLASS` in the enum for backward compatibility
- In practice, no records will use it since Layer 4 no longer generates it

## 12. Expected Improvements

| Metric                          | v1 (Current)       | v2 (Projected)        |
|---------------------------------|--------------------|-----------------------|
| Unique packaging configs        | 2,604 / 101,966    | ~67,858 / 67,858      |
| Config duplication rate          | 97.4%              | < 5%                  |
| API success rate                 | 0%                 | > 99%                 |
| Mass consistency errors          | 9%                 | 0% (fixed-order output)|
| Expansion factor                 | 2x                 | 1x                    |
| Output columns                   | 24                 | 20                    |
| Records in dataset               | ~101,966           | ~67,858               |
| Packaging-to-product correlation | Near zero          | Strong positive        |
| Transport-to-packaging signal    | Not captured       | Primary driver         |

## 13. Migration Checklist

- [ ] Implement new `clients/api_client.py` with Anthropic SDK
- [ ] Implement new `prompts/prompts.py` with system and user prompt templates
- [ ] Implement new `core/generator.py` with JSON parsing and validation
- [ ] Implement new `core/validator.py` with per-record and dataset-level checks
- [ ] Implement new `io/input_reader.py` for Layer 3 Parquet reading
- [ ] Implement new `io/writer.py` for Parquet output
- [ ] Implement new `config/config.py` with simplified settings
- [ ] Implement new `main.py` with sequential processing and checkpointing
- [ ] Update `model_a/config.py`: remove `"glass"` from `PACKAGING_CATEGORIES`
- [ ] Update `model_a/features/packaging.py`: remove glass-specific features
- [ ] Regenerate Layer 4 dataset
- [ ] Run dataset-level validation checks
- [ ] Regenerate Layer 6 training dataset from new Layer 4 output
- [ ] Retrain model and compare `cf_packaging_kg_co2e` prediction accuracy
- [ ] Remove old Layer 4 files: `api_client_direct.py`, `api_client_multikey.py`,
      `packaging_data.py`, `progress_tracker.py`, `orchestrator.py`
