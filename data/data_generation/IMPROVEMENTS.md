# Data Generation Architecture -- Improvement Plan

This document describes planned improvements to the 6-layer synthetic data
generation pipeline. Each improvement targets a specific quality problem
identified in the current architecture.

These improvements will be implemented when the full pipeline is rebuilt.

---

## Improvement 1: Per-Layer LLM Reality Validation

### Problem

In the current architecture, data quality is only checked at two points:

1. **Structural validators** inside each layer (array lengths, positive values,
   weight sums). These catch malformed data but cannot judge whether a
   composition is realistic.
2. **Layer 5** (the dedicated validation layer), which runs semantic checks
   using an LLM after all generation is complete.

The problem is that errors compound across layers. A single unrealistic
composition in Layer 1 gets expanded through Layers 2, 3, and 4 into dozens
of derived records (processing paths, transport scenarios, packaging configs).
By the time Layer 5 evaluates them, the pipeline has already spent API calls
and compute generating variants of bad data. Layer 5 then has to evaluate
every derived record individually, and its own LLM-based scoring has blind
spots (arbitrary thresholds, fragile response parsing, batch-mode defaults).

The result: unrealistic data propagates forward and multiplies, and the single
validation gate at Layer 5 is insufficient to catch everything.

### Solution

Add an LLM-based reality check at the output of each generation layer (Layers
1 through 4). Each check uses a focused prompt that asks the model a narrow
question about the output it just produced.

This does NOT replace Layer 5. Layer 5 remains as the final cross-cutting
validation gate. The per-layer checks are lightweight filters that catch
obvious problems early, before they multiply downstream.

### Design

Each layer gets a `RealtimeRealityChecker` component that runs after the
layer's structural validator but before the output is written. The checker
receives the generated record and returns a pass/fail decision with a short
justification.

**Layer 1 reality check -- Material-Product Coherence:**

The checker receives the product category, subcategory, and the generated
material composition. It asks:

```
You are a textile manufacturing expert. Evaluate whether this material
composition is realistic for a commercially viable fashion product.

Product: {category_name} > {subcategory_name}
Materials: {materials_with_percentages}
Total weight: {total_weight_kg} kg

Answer with a JSON object:
{
  "realistic": true/false,
  "reason": "One sentence explaining why"
}

A composition is REALISTIC if:
- The materials are appropriate for this specific product type
- The blend ratios follow established industry practices
- The total weight is plausible for this product category
- The product could actually be manufactured and sold commercially
```

Records that fail are discarded and regenerated (up to a configurable retry
limit). Records that pass proceed to Layer 2.

**Layer 2 reality check -- Processing Path Feasibility:**

The checker receives the material composition and the generated processing
path. It verifies:

- Processing steps are in a feasible sequential order (spinning before weaving,
  dyeing after fabric formation, etc.)
- Each step is applicable to the materials in the composition
- The overall path represents a real manufacturing workflow

**Layer 3 reality check -- Supply Chain Plausibility:**

The checker receives the product, materials, and the generated transport
scenario. It verifies:

- The origin region is a plausible source for these materials
- The transport distance is geographically consistent with the origin and a
  European destination
- The transport modes make sense for the distance and product type
- The supply chain strategy (cost/speed/eco/regional/risk) is internally
  consistent

**Layer 4 reality check -- Packaging Appropriateness:**

The checker receives the product category, weight, and the generated packaging
configuration. It verifies:

- Packaging materials are appropriate for this product type
- Packaging mass is proportional to product weight (typically 5-15%)
- The packaging configuration represents how this product would actually be
  packaged for retail

### Implementation Details

- **Model:** Claude Sonnet 4.6 (same model used for generation, so no
  additional API setup required)
- **Validation Flow (Two-Pass Strategy):**
  1. **Pass 1 - Batch Validation:** Validate all generated records in a single
     batch call. Return pass/fail with one-sentence justification for each record.
  2. **Pass 1 - Failure Collection:** Collect all failed records and their
     justifications into a single batch.
  3. **Guided Regeneration:** Send failed records + justifications to the
     generator with a specific prompt instructing it to address the exact
     issues identified. The generator regenerates ONLY the failed records,
     informed by the validation feedback.
  4. **Pass 2 - Re-validation:** Validate the regenerated records in a single
     batch call.
  5. **Final Disposition:** Records that pass in Pass 2 are retained. Records
     that fail in Pass 2 are permanently discarded (no further regeneration).
- **Latency:** Two API calls per layer per batch cycle (one validation, one
  re-validation), plus one generation call for failed records. Batching 10+
  records per call.
- **Cost:** Roughly 2-3 validation calls per batch of generated records, plus
  one generation call per batch of failures. For 14,000 Layer 1 records in
  batches of 100: ~140 Pass 1 validations, ~30-50 Pass 2 re-validations (only
  failed records), ~30-50 guided regenerations. Marginal compared to initial
  generation.
- **Threshold:** Binary pass/fail. The model either validates the record as
  realistic or identifies specific issues that prevent validation. No threshold
  tuning needed.
- **Feedback Loop:** The justification from Pass 1 failure becomes actionable
  input to the regeneration prompt, closing the feedback loop. The generator
  has concrete guidance on what to fix rather than blind retry.

### Expected Impact

- Catches unrealistic compositions at the source, before they multiply through
  downstream layers
- Guided regeneration improves the quality of retried records by providing
  specific feedback on what was wrong, rather than blind retry
- Two-pass validation prevents wasted downstream processing on stubborn edge
  cases (records that fail Pass 2 are discarded, not infinitely retried)
- Eliminates the error compounding problem where one bad Layer 1 record
  generates 50+ bad downstream records
- Provides per-record justifications that can be logged for debugging and
  quality auditing
- Feedback-driven generation improves model performance on difficult material
  combinations by giving it explicit guidance on what constraints to satisfy

---

## Improvement 2: Realistic Diversity Through Prompt Design

### Problem

The current pipeline generates compositions that are structurally valid but
lack diversity. The batch prompt (`prompts.py:120-179`) asks the LLM to
produce "UNIQUE and VARIED" compositions, but this is just instructional text
with no enforcement mechanism.

In practice, the LLM converges to the most common material combinations for
each product category. For T-shirts, nearly all generations are cotton/polyester
/elastane blends with minor ratio variations. For coats, wool/polyester
dominates. Real fashion products are far more varied -- T-shirts exist in
linen, hemp, bamboo viscose, modal, tencel, and many other materials.

The `generate_varied_compositions` method (`generator.py:297-326`) attempts
diversity by varying temperature by +/-0.1 around the base (0.7), giving a
range of 0.6-0.8. This is too narrow to produce meaningfully different outputs.

There is zero deduplication -- not within a single batch call, not across
batches for the same subcategory, and not across different subcategories. The
orchestrator (`orchestrator.py:248-271`) validates each composition
independently and never compares it to previously generated compositions.

### Solution

Redesign the generation prompt to produce the full diversity spectrum in a
single API call per subcategory. The key insight is that diversity and realism
are not in tension -- real fashion markets ARE diverse. By structuring the
prompt with explicit section headers, the model generates all material strategy
strata in one response while maintaining cross-section awareness.

**Single-Call Stratified Generation:**

Instead of asking for N "unique and varied" products (or making separate calls
per strategy), issue one call per subcategory with explicit section anchors
that force the model to fill each stratum:

```
Generate {n} material compositions for {product_category} > {subcategory}.
Organize your output into EXACTLY the 5 sections below.
Generate EXACTLY {n//5} products per section ({n} total).

=== SECTION 1: CONVENTIONAL ({n//5} products) ===
Primary materials: cotton, polyester, standard blends.
Generate compositions using mainstream, widely available material combinations.

=== SECTION 2: NATURAL/SUSTAINABLE ({n//5} products) ===
Primary materials: linen, hemp, organic cotton, tencel, modal, bamboo viscose.
Generate compositions prioritizing natural or sustainably sourced materials.

=== SECTION 3: PREMIUM ({n//5} products) ===
Primary materials: silk, cashmere, merino wool, cupro, alpaca.
Generate compositions using high-end materials found in luxury retail.

=== SECTION 4: PERFORMANCE ({n//5} products) ===
Primary materials: nylon, elastane-heavy blends, technical synthetics.
Generate compositions optimized for function (stretch, moisture-wicking, durability).

=== SECTION 5: BLENDED/INNOVATIVE ({n//5} products) ===
Generate compositions using unusual but commercially real material combinations
that do not fit neatly into the above categories.

AVAILABLE MATERIALS (full database):
{all_75_materials_formatted}

CONSTRAINTS:
- 2-5 materials per product
- Weight range: {weight_min}-{weight_max} kg
- Percentages sum to 100%
- Every composition must represent a product that could be sold in retail

ANTI-DUPLICATION:
Before returning your response, review ALL {n} products across all sections.
No two products may share the same set of materials (even with different
percentages). If you find duplicates, replace one with a different combination.
```

**Why single-call is superior to multi-call:**

- **1 API call instead of 5** per subcategory. For 14,000 Layer 1 records in
  batches of 100, this cuts generation calls from ~700 to ~140.
- **Cross-section awareness.** The model sees all sections in one context
  window, so it actively avoids duplication across strata. A cotton/elastane
  blend in CONVENTIONAL will not reappear in PERFORMANCE.
- **No per-strategy temperature tuning.** Diversity is enforced by prompt
  structure (a strong lever), not temperature variance (a weak lever). A single
  moderate temperature of 0.75 is sufficient for all strata.
- **Larger batch sizes.** Claude Sonnet can output 50-100 structured products
  per call. The section headers keep the output organized so the model does
  not lose track of structure at high counts.

**Post-generation count verification:**

After parsing the response, verify that the expected number of records was
actually returned. The model may occasionally produce fewer products than
requested (skipping some in a section) or more (over-generating in one
section to compensate for another).

```python
def verify_batch_count(parsed_records, expected_count, subcategory_id):
    """Verify the model returned exactly the expected number of records."""
    actual = len(parsed_records)
    if actual == expected_count:
        return parsed_records

    if actual > expected_count:
        # Trim excess records from the end (last section is most likely
        # to over-generate since the model is filling remaining quota)
        log.warning(
            f"{subcategory_id}: expected {expected_count}, got {actual}. "
            f"Trimming {actual - expected_count} excess records."
        )
        return parsed_records[:expected_count]

    # Under-generation: compute the shortfall and request a targeted
    # follow-up call for ONLY the missing count
    shortfall = expected_count - actual
    log.warning(
        f"{subcategory_id}: expected {expected_count}, got {actual}. "
        f"Requesting {shortfall} additional records."
    )
    return parsed_records  # caller handles the shortfall fill
```

The caller tracks the shortfall and issues a single targeted follow-up call:

```python
if shortfall > 0:
    # Determine which sections are under-filled
    section_counts = count_per_section(parsed_records)
    under_filled = {
        section: (expected_per_section - count)
        for section, count in section_counts.items()
        if count < expected_per_section
    }
    # Request only the missing products, passing existing fingerprints
    # to prevent duplicates in the fill call
    fill_records = generate_fill_batch(
        subcategory, under_filled, existing_fingerprints
    )
    parsed_records.extend(fill_records)
```

At most one follow-up call is made. If the fill call also under-generates, the
shortfall is logged and the batch proceeds with fewer records rather than
entering a retry loop.

**Post-generation deduplication:**

After count verification, compute a composition fingerprint for each record:

```python
def composition_fingerprint(comp):
    """Create a normalized fingerprint for deduplication."""
    sorted_materials = sorted(zip(comp.materials, comp.material_percentages))
    return tuple((m, round(p, -1)) for m, p in sorted_materials)
```

Deduplication operates at two levels:

1. **Intra-batch:** After parsing a single call's response, fingerprint all
   records and remove duplicates within the batch. The in-prompt anti-
   duplication instruction should prevent most of these, but the fingerprint
   check is the enforcement mechanism.

2. **Cross-batch:** Maintain a set of seen fingerprints across all batches
   for a subcategory. If a record from batch N matches a fingerprint from
   batch M, discard it. Percentages are rounded to the nearest 10 to catch
   near-duplicates (85% cotton vs 87% cotton are effectively the same).

Discarded duplicates contribute to the shortfall count and are filled via
the same targeted follow-up mechanism described above. The fill call receives
the full set of existing fingerprints in its prompt so the model knows which
combinations to avoid:

```
The following material combinations have ALREADY been generated.
Do NOT repeat any of them:
{existing_fingerprints_formatted}
```

**Temperature strategy:**

Use a single temperature of 0.75 for all calls. The diversity is enforced
by the prompt's section structure, not by temperature variance. Temperature
is a weak diversity lever (0.6 vs 0.8 produces marginal differences in
material selection). Explicit section constraints are a strong lever that
guarantees coverage of all material strategy buckets regardless of
temperature setting.

### Implementation Details

- **Batch size:** 50-100 products per call, depending on subcategory. The
  5-section structure keeps the output organized at these sizes.
- **Parsing:** The section headers (`=== SECTION N: ... ===`) serve as
  parsing anchors. Split the response on these markers and parse each
  section independently. If a section header is missing or malformed, fall
  back to sequential parsing.
- **Count verification flow:**
  1. Parse response into records
  2. Count total records and per-section counts
  3. If total matches expected: proceed to deduplication
  4. If over-count: trim from the end
  5. If under-count: log shortfall, proceed to deduplication, then fill
- **Deduplication flow:**
  1. Fingerprint all records in the batch
  2. Remove intra-batch duplicates
  3. Remove cross-batch duplicates (against seen fingerprints set)
  4. Add remaining fingerprints to seen set
  5. If duplicates were removed: add to shortfall count for fill call
- **Fill call budget:** At most 1 follow-up call per batch. If the fill call
  itself under-generates or produces duplicates, accept the reduced count and
  move on. This prevents infinite retry loops on difficult subcategories.
- **Logging:** Log per-batch statistics: records requested, received, trimmed,
  deduplicated, filled. This provides visibility into which subcategories are
  problematic and whether the prompt structure is effective.

### Expected Impact

- Full diversity spectrum generated in a single API call per subcategory,
  cutting generation costs by roughly 5x compared to per-strategy calls
- Cross-section awareness prevents the model from repeating compositions
  across material strategy strata
- Count verification catches under/over-generation with a single targeted
  fill call (no retry loops)
- Two-level deduplication (intra-batch + cross-batch) eliminates near-
  duplicate compositions with fingerprint enforcement
- Prompt-driven diversity (section headers) is a stronger guarantee than
  temperature-driven diversity, producing reliable coverage of all material
  strategy buckets

---

## Improvement 3: Remove Material Pre-Filtering, Supply Full Database

### Problem

The current architecture uses `SUBCATEGORY_MATERIAL_HINTS` (`materials.py:
163-249`) to pre-filter which materials the LLM is allowed to see. For each
subcategory, a hard-coded mapping selects 2-4 material categories, and only
materials from those categories appear in the prompt.

This creates two problems:

1. **Artificial limitation of material combinations.** For example, Outdoor
   Jackets (`cl-9-7`) are mapped to `["synthetic_fibers", "technical_materials"]`
   only. The LLM never sees `rubber_foam` (used in waterproof membranes and
   seam tape), `natural_fibers` (cotton canvas outdoor jackets exist), or
   `metals` (zippers, hardware). The model cannot generate these real product
   compositions because the materials are hidden from it.

2. **Uninformed fallback.** Subcategories without an explicit mapping in
   `SUBCATEGORY_MATERIAL_HINTS` fall back through parent category -> main
   category -> default `["natural_fibers", "synthetic_fibers"]`. Any
   subcategory not explicitly listed (and many are not) gets only natural and
   synthetic fibers, missing leather, rubber, metals, down, cork, and all
   other categories.

The pre-filtering was originally necessary because earlier models had limited
context windows and could not handle the full material list in-prompt.

### Solution

Remove the `SUBCATEGORY_MATERIAL_HINTS` mapping and the `MaterialCategoryMapper`
pre-filtering logic entirely. Supply the full 75-material database to the LLM
in every prompt, for every subcategory.

**Why this works now:**

The pipeline is being rebuilt on Claude Sonnet 4.6, which has a context window
large enough to handle the full material list without issues. The 75 materials
formatted with emission factors total roughly 2,000 tokens -- well within
budget even for batch prompts generating 25+ products.

**Why pre-filtering is counterproductive:**

The LLM (Claude Sonnet 4.6) has strong domain knowledge about which materials
are appropriate for which product categories. Limiting its choices to a hand-
coded subset forces it to work with an artificially constrained material space
and prevents it from making correct but unexpected material selections.

For example, a leather jacket with cotton lining is a common real product. But
if the LLM only sees `["leather_hides", "synthetic_fibers"]`, it cannot
generate this composition. Supplying the full database lets the model use its
own knowledge to select appropriate materials.

**What to remove:**

- `MaterialCategoryMapper.SUBCATEGORY_MATERIAL_HINTS` dict (lines 163-249)
- `MaterialCategoryMapper.get_categories_for_subcategory()` method
- `MaterialCategoryMapper.get_filtered_materials_for_subcategory()` method
- The two-stage generation path in `generator.py` (Stage A category selection
  is no longer needed when all materials are available)
- The `SINGLE_STAGE_TOKEN_THRESHOLD` logic in `generator.py:72`

**What to keep:**

- `MaterialCategoryMapper.EXCLUSION_PATTERNS` -- still needed to remove non-
  textile materials from the EcoInvent database before they reach the prompt.
  However, the exclusion list should be reviewed and cleaned up (fix the
  "seed-cotton" vs "seed cotton" mismatch, remove the trailing comma in
  "packing,", and consider whether "nonwoven" should remain a blanket
  exclusion).
- `MaterialDatabase` -- still loads and provides access to materials
- `MaterialCategoryMapper.CATEGORY_PATTERNS` -- still useful for grouping
  materials in the prompt by type (natural fibers, synthetic fibers, etc.) for
  readability, even though no categories are excluded

**New prompt format:**

```
AVAILABLE MATERIALS (grouped by type for reference):

Natural Fibers:
- fibre, cotton (EF: 5.89 kg CO2eq/kg)
- fibre, cotton, organic (EF: 3.80 kg CO2eq/kg)
- fibre, flax (EF: 2.29 kg CO2eq/kg)
- wool, conventional, at farm gate (EF: 22.80 kg CO2eq/kg)
[... all natural fibers]

Synthetic Fibers:
- fibre, polyester (EF: 6.98 kg CO2eq/kg)
- nylon 6 (EF: 9.20 kg CO2eq/kg)
[... all synthetic fibers]

Leather & Hides:
[... all hides]

Rubber & Foam:
[... all rubber/foam]

Metals:
[... all metals]

[... remaining categories]

Select 2-5 materials from ANY category above that are appropriate for this
specific product. You are not restricted to any category -- choose whatever
materials a real manufacturer would use for this product.
```

This gives the model full visibility while still organizing the materials
into readable groups. The model's own knowledge of textile manufacturing
determines which materials are appropriate, not a hard-coded mapping.

### Expected Impact

- Eliminates artificial material restrictions that prevent valid compositions
- Removes a maintenance burden (no more updating SUBCATEGORY_MATERIAL_HINTS
  when new subcategories are added)
- Enables the LLM to generate compositions with mixed-category materials
  (leather jacket with cotton lining, sneaker with rubber sole + cotton upper
  + metal eyelets) that were previously impossible
- Simplifies the generator code by removing the two-stage path

---

## Improvement 4: Fix Exclusion Pattern Matching

### Problem

The `EXCLUSION_PATTERNS` list in `materials.py:96-121` uses substring matching
to remove non-textile materials from the database before generation. This
approach has concrete failures with the current 75-material database:

**Materials wrongly excluded (false positives):**

| Material | Excluded By | Problem |
|----------|-------------|---------|
| `textile, nonwoven polyester` (row 65) | `"nonwoven"` | Nonwoven polyester is used as interlining in suits, coats, and structured garments |
| `textile, nonwoven polypropylene` (row 66) | `"nonwoven"` | Same -- legitimate interlining/lining material |

**Materials wrongly included (false negatives):**

| Material | Should Match | Problem |
|----------|-------------|---------|
| `seed cotton, conventional, Bangladesh` (row 20) | `"seed-cotton"` | Hyphen in pattern vs space in name -- no match |
| `seed cotton, conventional, India (Gujarat)` (row 21) | `"seed-cotton"` | Same mismatch |
| `seed cotton, conventional, global average` (row 22) | `"seed-cotton"` | Same mismatch |

**Fragile patterns:**

| Pattern | Issue |
|---------|-------|
| `"packing,"` (trailing comma) | Only matches `"packing,"` literally, misses `"packing"` or `"packing material"` |
| `"fibreboard"` | Misses American spelling `"fiber board"` or hyphenated `"fibre-board"` |

### Solution

Fix the specific known issues in `EXCLUSION_PATTERNS`:

```python
EXCLUSION_PATTERNS: List[str] = [
    # Raw agricultural materials (not processed fibres)
    "seed cotton",       # FIXED: was "seed-cotton" (hyphen didn't match)
    "decorticated",
    # Waste and by-products
    "bottom ash", "residues", "waste", "sludge", "sewage",
    # Processing steps (not materials)
    "bleaching", "mercerizing", "sanforizing", "dyeing", "finishing",
    # Construction materials
    "fibreboard", "fiber board",  # FIXED: added American spelling
    "fibre cement", "gypsum", "insulation",
    "corrugated", "facing tile", "roof slate", "duct",
    "cement bonded", "wood wool",
    # Engineering plastics (not textile fibres)
    "glass-filled", "injection moulded", "reinforced plastic",
    "carbon fibre reinforced", "glass fibre",
    # Packaging -- FIXED: removed trailing comma
    "packing",
    # Non-textile polymers
    "biopolymer", "bio-polymer",  # FIXED: added hyphenated variant
    "starch biopolymer",
    # Miscellaneous non-textiles
    "medium density", "mswi", "ww from",
    # Market processes (not actual materials)
    "market for",
]
```

Remove the blanket `"nonwoven"` exclusion. Nonwoven polyester and polypropylene
are legitimate textile materials (interlinings, linings, interfacing). The LLM
is capable of deciding when nonwovens are appropriate for a given product type
-- this is exactly the kind of decision the per-layer reality check
(Improvement 1) is designed to validate.

### Expected Impact

- 3 seed cotton variants correctly excluded (were slipping through)
- 2 nonwoven textiles correctly included (were wrongly excluded)
- Fragile patterns fixed to handle spelling variations
- Net effect: cleaner material pool for generation with fewer edge case
  failures
