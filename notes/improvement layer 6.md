# improvement layer 6 -- Design Notes
# Started: 2026-03-28

## Objective

Replace Layer 6's generalized multinomial logit transport model with actual per-leg transport data from Layer 3. The existing pipeline generates detailed per-leg transport modes and distances (in the transport_legs JSON column), but Layer 6 ignores this and recomputes mode probabilities from total distance alone. Goal: use the actuals for maximum training data accuracy.

## Agreed Decisions

### D3: Two-phase Layer 6 architecture
**Decision:** Layer 6 becomes two phases: (1) LLM enrichment pass that reads Layer 5 data, extracts structured per-segment transport distances from reasoning fields via Claude Sonnet 4.5, and saves a "pre-Layer 6" dataset with the new column; (2) Deterministic calculation pass that reads the enriched dataset and computes carbon footprints using actual per-segment mode/distance pairs.
**Reasoning:** Clean separation of concerns -- the LLM pass is expensive and only needs to run once. The calculation pass remains deterministic and re-runnable. The pre-Layer 6 dataset is a checkpoint artifact that can be inspected and reused.
**Rejected:** Inline LLM calls during calculation (no separation, harder to debug and resume).

### D4: LLM configuration and resilience settings
**Decision:**
- Model: Claude Sonnet 4.5 (claude-sonnet-4-5-20241022)
- Max retries: 5 per request (fail-open -- skip record on exhaustion, log it)
- No rate limits on API tier -- can push throughput
- Batched processing (batch size TBD -- see analysis below)
- Checkpointing: periodic saves of enriched rows (frequency TBD -- see analysis below)
- No validation pipeline -- system prompt designed to produce correct structured output directly
- Backoff: exponential with jitter (standard for API resilience)
**Reasoning:** Sonnet 4.5 is cost-efficient for structured extraction tasks. No rate limits means we optimize for throughput. Fail-open on retry exhaustion ensures one bad record doesn't block the entire run. Validation skipped in favor of a well-designed system prompt with strict JSON schema.
**Rejected:** Claude Opus 4.6 (overkill for extraction). Fail-closed on retry (blocks pipeline for edge cases).

### D6: Per-record aggregated extraction, not per-leg
**Decision:** The LLM receives the entire transport_legs array for a record (all legs + all reasoning fields) and returns one aggregated summary: total km per transport mode for the entire product. Output schema per record: `{"road": X, "sea": Y, "rail": Z, "air": W, "inland_waterway": V}`. Multiple records batched per API call (~20 records/call).
**Reasoning:** Per-leg extraction would require 500K-800K individual extractions (10K-16K API calls even batched). Per-record aggregation reduces to ~5,000 API calls for 100K records at 20 records/batch. The model reads all reasoning fields at once and sums internally. Output is exactly what the calculation formula needs -- no further aggregation step.
**Rejected:** Per-leg individual extraction -- 3-10x more API calls for the same result. The per-leg detail is not needed by the carbon formula; only the per-mode totals matter.

### D8: Validation tolerance and failed record handling
**Decision:** 1% tolerance on SUM(mode_distances) vs total_distance_km. Records that fail validation are collected and retried in batches at the end (not individually). This saves tokens compared to per-record retry.
**Reasoning:** Tight tolerance catches real extraction errors. Batched retry at the end is more token-efficient than individual retries during processing. Failed records from the main pass can be grouped into a single retry batch.
**Rejected:** 5% tolerance (too loose). Per-record retry (wastes tokens on individual calls).

### D7: Lightweight validation on LLM output
**Decision:** After LLM returns per-mode distance totals, validate that SUM(mode distances) is approximately equal to the record's total_distance_km. Flag records where the discrepancy exceeds a tolerance threshold (TBD). This replaces the earlier "no validation" stance from D4.
**Reasoning:** Aggregating across multiple legs and multi-modal segments introduces summation error risk. A simple numeric check catches obvious LLM mistakes (missed legs, hallucinated distances) without needing a full validation pipeline. Cheap to run -- pure arithmetic, no API calls.
**Rejected:** No validation at all (D4 originally said skip it, but aggregation changes the risk profile). Full LLM-based validation pipeline (overkill for a numeric check).

### D5: LLM provider -- UVA AI API Cloudflare
**Decision:** Use the existing UVA AI API Cloudflare integration via shared/api_client.py FunctionClient. Base URL: http://localhost:3000/v1, OpenAI-compatible chat completions endpoint. API key via UVA_API_KEY env var. Model: claude-sonnet-4-5-20241022 (to be configured in Layer6Config).
**Reasoning:** All other layers (1-5) use this same provider and client architecture. Consistent infrastructure, no new dependencies needed.
**Rejected:** Direct Anthropic SDK call (breaks existing abstraction pattern).

### D2: LLM-based reasoning field parsing for multi-modal distance splitting
**Decision:** Use LLM calls to extract structured per-segment (mode, distance_km) pairs from the natural language reasoning field in each transport leg. This runs as an enrichment step in Layer 6 before carbon calculation, adding a new structured column to the dataset.
**Reasoning:** The reasoning field contains per-segment distances in natural language (e.g., "Trucked 420 km... shipped 2100 km... 330 km by road"). An LLM can parse this reliably across varying phrasings. Regex would be brittle. This gives the highest accuracy for multi-modal legs.
**Rejected:** Primary mode assignment (Approach 1) -- loses first/last mile detail. Regex parsing -- too brittle for varied LLM-generated text. Proportional split (Approach 2) -- already rejected as R2.

### D10: Rename output columns to reflect actual transport data
**Decision:** Replace probability-based column names with names reflecting actual measured transport data:
- `transport_mode_probabilities` -> `transport_mode_distances_km` (dict of mode -> actual km)
- `weighted_ef_g_co2e_tkm` -> `effective_ef_g_co2e_tkm` (actual weighted average from real mode mix)
- New column: `transport_mode_fractions` (dict of mode -> fraction of total distance, for model features)
**Reasoning:** "probability" implies statistical estimation. The new data represents actual transport mode usage. Column names should reflect this.

### D11: Checkpoint every 5,000 records
**Decision:** Write temp checkpoint files every 5,000 records during LLM enrichment pass. Use temp file + merge pattern from Layer 5. Resume by scanning completed record IDs in temp files.
**Reasoning:** 50,480 records / 5,000 = ~10 checkpoint files. ~125 API calls between saves. Balances I/O overhead vs crash recovery.

### D1: Use actuals over calibrated model
**Decision:** Replace the multinomial logit model with actual per-leg transport mode and distance data from the transport_legs column, rather than calibrating the existing model against observed data.
**Reasoning:** The user's priority is maximum accuracy in the training data. The actual modes and distances are already present in the data -- using them directly eliminates the approximation error inherent in any statistical model. A calibrated model would still be an approximation.
**Rejected:** Strategy A (calibrate logit model against observed mode choices) -- still a generalization, not exact.

## Open Questions

- [x] What proportion of legs are multi-modal vs single-mode? -- 75.9% single, 24.1% multi (722K total legs across 50,480 records)
- [x] Multi-modal distance splitting strategy -- decided: LLM parsing of reasoning field (D2)
- [x] What happens to the output schema? -- Rename columns to reflect actual data, not probabilities (D10)
- [x] Should the reasoning field be parsed for per-segment distances (Approach 3), or is primary mode assignment (Approach 1) acceptable? -- decided: Approach 3 via LLM (D2)
- [x] Which LLM provider/model? -- Claude Sonnet 4.5 (D4)
- [x] Rate limiting, retry, and backoff? -- 5 retries, exponential+jitter, no rate limit cap (D4)
- [x] Checkpointing strategy? -- every 5,000 records using temp file + merge pattern (D11)
- [x] Batch size? -- ~20 records per LLM call (D6)
- [x] What structured output schema should the LLM return? -- per-mode km totals: {"road": X, "sea": Y, ...} (D6)
- [x] Fallback for single-mode legs? -- no special case, all legs processed together per record (D6)
- [x] Validation tolerance: 1% (D8)
- [x] Failed record handling: collect and retry in batches at the end (D8)
- [x] Cost estimate: ~$280 at Anthropic list pricing (~55M input tokens + ~7.6M output tokens across ~2,524 calls). UVA proxy pricing may differ.
- [x] Which Layer 5 output to use as input: accepted + review (~50,480 records) (D9 -- user confirmed from prior conversation)
- [x] Which file is the A+R dataset? -- `layer_5_validated_dataset.csv` = 50,480 rows (accepted 34,787 + needs_review 15,693). Confirmed via LFS pull.
- [x] Does Layer 5 data have transport_legs? -- NO. Column `transport_items` is just material names. Must JOIN to Layer 4 via pp-XXXXXX id extracted from record_id to get transport_legs. All 50,480 match.
- [x] Should we reuse the existing shared/api_client.py (localhost:3000 proxy) or call Anthropic API directly? -- UVA AI API Cloudflare via shared/api_client.py (D5)

## Rejected Alternatives

### R1: Calibrate existing multinomial logit model
Fit alpha/beta/d_ref parameters against observed (distance, mode) pairs from transport_legs. Rejected because it remains a statistical approximation when exact data is available. Would be useful for generalizing to unseen data, but for training data generation, actuals are preferred.

### R2: Proportional split by mode count
For multi-modal legs like ["road", "sea", "road"] with 2850 km, split distance equally by mode occurrence (road 2/3, sea 1/3). Rejected during analysis as clearly inaccurate -- overweights first/last mile road segments relative to trunk haul.

## Technical Constraints

### Data structure limitations
- transport_legs is a JSON string in parquet, needs json.loads() per record
- Each leg has ONE distance_km for the ENTIRE leg, not per-segment
- Multi-modal legs (e.g., ["road", "sea", "road"]) have no structured per-segment distance breakdown
- The reasoning field contains natural language with per-segment distances but requires NLP/regex parsing
- ~100K records in Layer 4 dataset, variable number of legs per record

### Existing transport emission factors (unchanged)
- road: 74.0 g CO2e/tkm
- rail: 22.0 g CO2e/tkm
- inland_waterway: 31.0 g CO2e/tkm
- sea: 10.3 g CO2e/tkm
- air: 782.0 g CO2e/tkm

### Files that will need modification
- `data/data_generation/layer_6/core/components.py` -- calculate_transport() function
- `data/data_generation/layer_6/core/transport_model.py` -- may be replaced or heavily refactored
- `data/data_generation/layer_6/core/calculator.py` -- needs to pass transport_legs to transport calculation
- `data/data_generation/layer_6/core/_processing.py` -- needs to extract transport_legs from input records
- `data/data_generation/layer_6/config/config.py` -- TRANSPORT_MODE_PARAMS may become unused

### Existing analysis script
- `data/data_generation/layer_6/analysis/extract_transport_data.py` already has:
  - `extract_legs()` function for parsing transport_legs JSON
  - `infer_primary_mode()` function for single-mode assignment from multi-modal legs
  - MODE_RANK hierarchy: sea > air > rail > inland_waterway > road

## Implementation Hints

### Two-phase architecture
**Phase 1 -- LLM enrichment:** Read Layer 5 data, extract transport_legs + reasoning, call Claude Sonnet 4.5 to produce structured per-segment (mode, distance_km) pairs. Save as pre-Layer 6 dataset (parquet).
**Phase 2 -- Deterministic calculation:** Read enriched dataset, compute per-segment transport CF using actual modes and distances. Purely deterministic, re-runnable.

### New transport formula (per-leg, per-segment)
```
CF_transport = SUM over all legs, all segments:
    (W_total / 1000) * D_segment * (EF_mode / 1000)
```

### Single-mode legs (likely majority)
No ambiguity: full distance_km * EF_mode. LLM call still needed to confirm but trivial extraction.

### Existing infrastructure to reuse
- `shared/api_client.py` -- OpenAI-compatible REST client (base URL: localhost:3000/v1)
- `shared/parallel_processor.py` -- ParallelProcessor with rate limiting, pause/resume
- `layer_5/io/writer_incremental.py` -- temp file + merge checkpoint pattern
- JSON extraction from content + reasoning_content fields (multi-fallback)

### Batch strategy (revised -- per-record aggregation)
**~20 records per LLM call:**
- Each record's full transport_legs JSON (all legs + reasoning) sent to the model
- Model returns one aggregated {mode: total_km} dict per record
- 100K records / 20 per call = ~5,000 API calls total
- Checkpoint every 5,000 records (write temp parquet/CSV to {output_dir}/temp_files/)
- Resume by loading completed record IDs from temp files, skip already-processed
- Temperature: 0.3 (deterministic extraction, not creative)

### Checkpoint strategy analysis
**Why 5,000 records:**
- ~100K records total = ~20 checkpoint files
- At ~50 legs/call, ~10 records/call = ~500 API calls between checkpoints
- Balances I/O overhead vs data loss risk
- Consistent with Layer 3 checkpoint frequency

### Output schema considerations
Current output fields that will change:
- transport_mode_probabilities (dict of mode -> probability) -- replaced with actual mode distance fractions
- weighted_ef_g_co2e_tkm (single weighted EF) -- replaced with actual weighted average from segments
- New column: transport_segments (structured JSON with per-segment mode + distance)

### System Prompt Design (D12 -- pending approval)

**System prompt:**

```
You are a transport logistics data extraction engine. Your task is to read textile supply chain transport leg data and extract the total distance traveled by each transport mode.

TASK
For each record, you receive a JSON array of transport legs. Each leg has:
- transport_modes: ordered list of modes used (e.g., ["road", "sea", "road"])
- distance_km: total distance for that leg
- reasoning: narrative describing the journey with per-segment distances

EXTRACTION RULES
1. For SINGLE-MODE legs (transport_modes has one entry): assign the full distance_km to that mode.
2. For MULTI-MODE legs (transport_modes has multiple entries): read the reasoning field and extract the distance for each segment. The reasoning always describes each segment with its distance (e.g., "Trucked 430 km to port. Shipped 2180 km. Final 340 km by road.").
3. Sum all distances per mode across ALL legs in the record.
4. The five valid modes are: road, sea, rail, air, inland_waterway. Return 0.0 for any mode not used.
5. Round all distances to 1 decimal place.

OUTPUT FORMAT
Return a JSON array with one object per record, in the order received. Each object:
{
  "id": "<the record id provided>",
  "road_km": <float>,
  "sea_km": <float>,
  "rail_km": <float>,
  "air_km": <float>,
  "inland_waterway_km": <float>
}

CRITICAL RULES
- Extract distances ONLY from the reasoning text. Do not estimate or infer.
- If the reasoning does not specify per-segment distances for a multi-mode leg, divide the leg distance proportionally by the number of modes (fallback only).
- Output ONLY the JSON array. No explanation, no markdown fences, no preamble.
```

**User prompt template (per batch):**

```
Extract transport mode distances for the following {n} records.

--- Record 1 (id: {record_id}) ---
total_distance_km: {total_km}
transport_legs:
{legs_json_stripped}

--- Record 2 (id: {record_id}) ---
...
```

**Stripping strategy:** Send only transport_modes, distance_km, and reasoning per leg. Drop coordinates, locations, from_step, to_step, leg_index -- these are irrelevant for distance extraction and waste tokens.

**Temperature:** 0.2 (near-deterministic extraction)

**Max tokens:** 8,000. Expected output ~3,000 tokens (20 records x ~150 tokens each). The 8K ceiling provides ~2.5x headroom to prevent truncation without encouraging verbose output. The system prompt's "Output ONLY the JSON array" instruction keeps responses tight.

**Batch sizing rationale:** 20 records x ~14 legs x ~80 tokens per leg reasoning = ~22,400 input tokens per call. Well within context limits. Output ~3,000 tokens. Total ~25K tokens per call.

## Implementation Log

### 2026-03-28: Implementation complete

**10 tasks executed across 5 groups (4 parallel + 1 sequential + integration + cleanup).**

Files created (enrichment pipeline):
- `data/data_generation/layer_6/enrichment/__init__.py` (5 lines)
- `data/data_generation/layer_6/enrichment/config.py` (118 lines) -- EnrichmentConfig dataclass
- `data/data_generation/layer_6/enrichment/data_joiner.py` (256 lines) -- Layer 5 + Layer 4 join
- `data/data_generation/layer_6/enrichment/prompt_builder.py` (165 lines) -- system + batch prompts
- `data/data_generation/layer_6/enrichment/client.py` (262 lines) -- LLM client with retry
- `data/data_generation/layer_6/enrichment/validator.py` (251 lines) -- 1% tolerance validation
- `data/data_generation/layer_6/enrichment/checkpoint.py` (197 lines) -- checkpoint manager
- `data/data_generation/layer_6/enrichment/orchestrator.py` (265 lines) -- main enrichment pipeline
- `data/data_generation/layer_6/enrichment/smoke_test.py` (286 lines) -- integration tests
- `data/data_generation/scripts/run_layer6_enrichment.py` (159 lines) -- CLI entry point

Files modified (calculation engine):
- `data/data_generation/layer_6/config/config.py` -- added enriched paths, use_enriched_transport flag, D10 column constants
- `data/data_generation/layer_6/core/components.py` -- added calculate_transport_from_actuals(), renamed old to calculate_transport_logit()
- `data/data_generation/layer_6/core/transport_model.py` -- added note about enriched path
- `data/data_generation/layer_6/core/calculator.py` -- branches on use_enriched_transport, unpacks 4-tuple from new transport function
- `data/data_generation/layer_6/core/_processing.py` -- reads enriched input, outputs D10 column names
- `data/data_generation/layer_6/core/databases.py` -- added transport_mode_fractions to CalculationResult

Bugs found and fixed:
- calculator.py unpacked 2 values from a 4-tuple return (calculate_transport_from_actuals). Fixed to unpack all 4.
- calculator.py referenced old function name calculate_transport() instead of renamed calculate_transport_logit(). Fixed.

De-sloppify fixes:
- Moved inline imports (ast, shutil, traceback) to module-level in 4 files
- Removed dead variable in data_joiner.py

Smoke test: 10/10 passing.
