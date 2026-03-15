# Layer 3: Transport Scenario Generator -- Complete Design

This document captures every design decision for the complete redesign
of Layer 3 in ESPResso-V2.

---

## 1. Purpose and Scope

Layer 3 generates structured, coordinate-based transport scenarios for textile
products. Given a product's material composition and processing steps (from
Layers 1 and 2), Layer 3 determines:

- A realistic geographic location for each processing step of each material
- The transport route between consecutive processing locations
- The transport modes used for each segment of each route
- The distance traveled per segment (reasoned by the LLM)
- A reasoning narrative describing the full journey

**Scope**: Cradle-to-gate. The journey covers raw material sourcing through
all processing steps, assembly, and final delivery to the user's warehouse.
It does NOT cover retail distribution or end-of-life.

---

## 2. What Changed and Why

### Problems with V1 Layer 3

The V1 implementation used an LLM (Nemotron) to generate a single
`total_transport_distance_km` number and a flat `transport_modes` list per
record. This produced:

- 403 unique origin_region string variants (fragmented, unlabeled)
- 16,700 rows (24.6%) with supply_chain_type/distance mismatches
- 490 rows with placeholder template text instead of real data
- 6,362 rows with non-standard scenario IDs that bypassed validation
- The generated transport_modes list was completely ignored by Layer 6,
  which recomputed mode probabilities from distance alone using a
  multinomial logit model
- The parallel processing codepath bypassed all quality control

### What V2 Layer 3 does differently

- Generates per-leg transport detail instead of a single collapsed distance
- Uses WGS84 coordinates instead of free-text origin strings
- Claude Sonnet handles the entire reasoning process: location assignment,
  route planning, transport mode selection, and distance estimation per leg
- Distances per leg are produced by Sonnet through reasoning (accounting
  for actual transport routes, not just straight-line distance)
- Only `total_distance_km` is computed by the pipeline (sum of leg distances)
- Sonnet chooses its own realistic locations based on reasoning, guided by
  a geographic reference in the system prompt to prevent coordinate
  hallucination

---

## 3. Input Schema

Layer 3 reads the output of Layer 2. Each input record contains:

| Column                 | Type       | Description                          |
|------------------------|------------|--------------------------------------|
| category_id            | str        | Product category ID (from L1)        |
| category_name          | str        | Product category name (from L1)      |
| subcategory_id         | str        | Subcategory ID (from L1)             |
| subcategory_name       | str        | Subcategory name (from L1)           |
| materials              | str (JSON) | Array of material names (from L1)    |
| material_weights_kg    | str (JSON) | Array of per-material weights (L1)   |
| material_percentages   | str (JSON) | Array of per-material percentages    |
| total_weight_kg        | float      | Total product weight in kg (L1)      |
| preprocessing_path_id  | str        | Unique preprocessing path ID (L2)    |
| preprocessing_steps    | str (JSON) | Array of all processing step names   |
| step_material_mapping  | str (JSON) | Dict: material -> [step1, step2...]  |

### Key input characteristics

- Most records have 2 materials (94% of current data), some have 1 or 3-4
- Each material has 1-8 processing steps (from the 31 unique steps in the
  current dataset)
- Materials with 0 steps are a Layer 2 generation defect. Layer 2's prompt
  must be fixed to ensure every material has at minimum its core processing
  chain. For trims/hardware, at least a "manufacturing" step is required so
  Layer 3 can assign a source location.

---

## 4. Output Schema

### 4.1 Flat columns (13 total)

**Carried forward from earlier layers (11 columns, unchanged):**

| Column                 | Type       | Source |
|------------------------|------------|--------|
| category_id            | str        | L1     |
| category_name          | str        | L1     |
| subcategory_id         | str        | L1     |
| subcategory_name       | str        | L1     |
| materials              | str (JSON) | L1     |
| material_weights_kg    | str (JSON) | L1     |
| material_percentages   | str (JSON) | L1     |
| total_weight_kg        | float      | L1     |
| preprocessing_path_id  | str        | L2     |
| preprocessing_steps    | str (JSON) | L2     |
| step_material_mapping  | str (JSON) | L2     |

**Added by Layer 3 (2 columns):**

| Column                    | Type       | Description                                    |
|---------------------------|------------|------------------------------------------------|
| transport_legs            | str (JSON) | Full array of leg objects (see 4.2)            |
| total_distance_km         | float      | Sum of all leg distance_km values (computed by pipeline) |

**Total: 13 columns.** No per-mode aggregated columns (road_km, sea_km, etc.)
-- that is feature engineering, handled in the model preprocessing pipeline
or Layer 6, not in the Layer 3 output.

### 4.2 Transport leg JSON structure

The `transport_legs` column contains a JSON array where each element
represents one processing-step-to-processing-step transport leg:

```json
[
  {
    "leg_index": 0,
    "material": "textile, silk",
    "from_step": "raw_material",
    "to_step": "spinning",
    "from_location": "Otsu, Japan",
    "to_location": "Fukui, Japan",
    "from_lat": 35.00,
    "from_lon": 135.87,
    "to_lat": 36.06,
    "to_lon": 136.22,
    "distance_km": 132.4,
    "transport_modes": ["road"],
    "reasoning": "Direct road transport 132km via Hokuriku Expressway. Both locations on Honshu island with good road infrastructure, no port or rail transfer needed."
  },
  {
    "leg_index": 1,
    "material": "textile, silk",
    "from_step": "spinning",
    "to_step": "dyeing",
    "from_location": "Fukui, Japan",
    "to_location": "Shaoxing, China",
    "from_lat": 36.06,
    "from_lon": 136.22,
    "to_lat": 30.00,
    "to_lon": 120.58,
    "distance_km": 1842.7,
    "transport_modes": ["road", "sea", "road"],
    "reasoning": "Trucked 85km from Fukui spinning mill to Port of Tsuruga. Container shipped 1580km across the East China Sea to Port of Shanghai (3-4 day transit). Final 178km by truck from Shanghai Yangshan terminal to Shaoxing dyeing district via G60 expressway."
  }
]
```

**Field definitions:**

| Field            | Type     | Description                                        |
|------------------|----------|----------------------------------------------------|
| leg_index        | int      | Sequential index starting at 0                     |
| material         | str      | Which material is being transported on this leg    |
| from_step        | str      | Processing step at the origin location             |
| to_step          | str      | Processing step at the destination location        |
| from_location    | str      | City, Country of origin                            |
| to_location      | str      | City, Country of destination                       |
| from_lat         | float    | WGS84 latitude of origin (decimal degrees)         |
| from_lon         | float    | WGS84 longitude of origin (decimal degrees)        |
| to_lat           | float    | WGS84 latitude of destination (decimal degrees)    |
| to_lon           | float    | WGS84 longitude of destination (decimal degrees)   |
| distance_km      | float    | Total distance for this leg (reasoned by Sonnet)   |
| transport_modes  | [str]    | Ordered array of modes used (first-mile to last)   |
| reasoning        | str      | Narrative describing every segment of the journey  |

**No material_weight_kg in legs.** The weight per material is already in the
parent record's `material_weights_kg` from L1 and can be joined via the
`material` field. Including it would be redundant.

### 4.3 total_distance_km

This is the ONLY value computed by the pipeline (not by Sonnet). It is
the sum of all `distance_km` values from the `transport_legs` array.

---

## 5. Generation Design

This section describes how the LLM generates transport data and how the
pipeline assembles it.

### 5.1 Location and distance reasoning

Distances per leg are NOT computed deterministically by the pipeline.
Sonnet reasons about the actual transport route and estimates the distance
for each leg, accounting for:

- The actual transport path (roads are not straight lines)
- Port-to-port maritime routes (routing around landmasses)
- First-mile and last-mile road segments to/from ports and airports
- The fact that different transport modes travel different actual distances
  for the same origin-destination pair

This is the reason we use Sonnet: it can reason about realistic transport
distances that are higher than the straight-line distance between two
coordinates. A sea route from Japan to China goes through specific
shipping lanes; a road from a factory to a port follows actual highways.

The pipeline computes only ONE value:

```
total_distance_km = sum(leg["distance_km"] for leg in transport_legs)
```

Everything else (per-leg distance, transport modes, coordinates, reasoning)
comes directly from Sonnet's output.

### 5.2 Transport mode determination

Transport modes are determined by Sonnet for each leg, based on geographic
reasoning. There are NO hardcoded rule-based mode selection rules in the
codebase.

Sonnet reasons about:
- Whether locations are on the same landmass or separated by ocean
- The available infrastructure (ports, airports, rail networks)
- The distance and practical transport options
- What makes sense for the specific product and route

The 5 transport modes from the ESPResso-V2 C layer (transport.h):
- road (HGV/truck)
- rail (freight train)
- sea (container ship)
- air (freight aircraft)
- inland_waterway (barge)

Each leg's `transport_modes` is an ordered array reflecting the actual
multi-modal journey. A typical intercontinental leg looks like:
`["road", "sea", "road"]` (truck to port, ship, truck from port).

The `reasoning` field describes each segment including the distance per
segment, so the full journey is traceable and the total leg distance
reflects all segments combined.

### 5.3 Assembly and convergence

Multiple materials in a product travel independently through their own
processing chains, then converge at an assembly location.

**Pattern 1 -- Single convergence (most garments, ~80%):**
All materials arrive at one CMT (Cut, Make, Trim) factory. This is the
industry standard for basic garments (T-shirts, trousers, dresses, etc.).

**Pattern 2 -- Progressive convergence (complex products, ~20%):**
Some materials converge at an intermediate sub-assembly point before final
assembly. This applies to footwear (upper + sole), technical outerwear
(shell + insulation), and tailored garments (body + lining).

The LLM identifies the final assembly step (the last step that involves
multiple materials, e.g., "garment assembly", "stitching", "bonding").
All material chains must route to this location.

For complex products where the step_material_mapping implies sub-assembly
(e.g., separate "quilting" and "bonding" steps for different material
groups), the LLM may place one intermediate convergence point.

After assembly, all materials travel as a single unit to the warehouse.

**Warehouse leg:** The final leg of every product journey is from the
assembly location to the warehouse. The warehouse location depends on
the target market (EU or US). The LLM determines the appropriate warehouse
based on the context and routes the product accordingly, including
intermediate port stops.

### 5.4 Geographic reference

The system prompt includes a geographic reference containing major
manufacturing regions, ports, and cities with their verified WGS84
coordinates. This serves as an anchor to prevent coordinate hallucination.

The reference includes:
- Major textile manufacturing cities (with coordinates)
- Major container ports (with coordinates)
- Major cargo airports (with coordinates)
- Common warehouse/distribution locations (EU and US)
- Key industrial regions per processing type

Sonnet is NOT restricted to locations in the reference table. It can
choose any location it deems realistic. The reference serves two purposes:

1. When Sonnet picks a location that IS in the reference, it uses the
   verified coordinates from the table
2. When Sonnet picks a location NOT in the reference, it uses its own
   geographic knowledge, but the surrounding reference data helps it
   calibrate (e.g., knowing that Shanghai is at 31.23, 121.47 helps
   it place nearby Shaoxing at approximately 30.00, 120.58)

---

## 6. Prompt Architecture

### 6.1 Directory structure

```
layer_3/
  prompts/
    system/
      00_role.txt               -- LLM persona and task overview
      01_task_definition.txt    -- Exact task, input/output description
      02_geographic_reference.txt -- Reference locations with coordinates
      03_routing_guidance.txt   -- Context for transport mode reasoning
      04_convergence_rules.txt  -- Material assembly/convergence logic
      05_warehouse_rules.txt    -- Final warehouse leg rules
      06_data_spread.txt        -- Guidance for location variety
      07_output_format.txt      -- Exact JSON schema with worked examples
    builder.py                  -- Assembles system + user prompt per record
```

### 6.2 Component purposes

**00_role.txt**: Sets Claude Sonnet as a textile supply chain logistics
expert who assigns locations to processing steps and determines transport
routes with distances.

**01_task_definition.txt**: Defines the exact task: for each material,
determine a realistic location (city, country, lat, lon) for each
processing step, then reason about the transport route between consecutive
steps including transport modes, distances, and intermediate waypoints,
then route to warehouse.

**02_geographic_reference.txt**: Reference locations with verified WGS84
coordinates, organised by function (manufacturing hubs, ports, airports,
warehouses). Sonnet uses this to anchor its coordinate knowledge and
prevent hallucination.

**03_routing_guidance.txt**: Context for transport mode reasoning. NOT
hardcoded rules, but contextual knowledge: typical infrastructure in
different regions, common freight routes, how multi-modal journeys work
in textile logistics.

**04_convergence_rules.txt**: How materials converge at assembly. Covers
single-convergence (CMT) and progressive-convergence (sub-assembly)
patterns. Instructions for identifying the assembly step.

**05_warehouse_rules.txt**: Final leg rules. EU warehouses and US
warehouses. Context for warehouse selection.

**06_data_spread.txt**: Guidance for variety in location selection.
Encourages Sonnet to explore different regions for similar products
rather than always defaulting to the most obvious choice.

**07_output_format.txt**: The exact JSON schema for the transport_legs
array, with a complete worked example showing a multi-material product
with convergence, multi-modal legs, and warehouse delivery.

### 6.3 Prompt assembly

The system prompt is the concatenation of all 00-07 files and is STATIC
across all records (cacheable for API cost savings with Claude Sonnet).

The user message is PER-RECORD and contains:
- Product details (category, subcategory, weight)
- Materials with weights
- step_material_mapping from Layer 2
- Target warehouse (EU or US)
- Record seed number (for data spread variation)

### 6.4 LLM choice

Claude Sonnet is the target LLM for Layer 3 generation. It handles:
- Location assignment (geographic reasoning)
- Coordinate determination (lat/lon for each location)
- Transport route planning (waypoint and mode selection)
- Distance estimation per leg (reasoned, not straight-line)
- Reasoning narrative generation

The pipeline handles only:
- Summing leg distances into `total_distance_km`
- Validating coordinate ranges and distance bounds
- Writing the output

---

## 7. Coordinate System

### 7.1 Storage format: WGS84 latitude/longitude

All coordinates in the Layer 3 output are stored as standard WGS84 decimal
degrees. This is the format the LLM returns.

- Every geocoding API and GIS tool uses this format
- It is human-readable and verifiable
- It is the canonical form from which all other encodings can be computed

### 7.2 How Sonnet provides coordinates

Sonnet determines realistic locations for each processing step based on
its own reasoning about the supply chain. It then converts each location
name to lat/lon coordinates.

To prevent coordinate hallucination, the system prompt includes a
geographic reference containing major manufacturing regions, ports, and
cities with their verified coordinates (see section 5.4).

For downstream model consumption (sin/cos encoding and other
transformations), see `/layer_6/COORDINATE_CONVERSION.md`.

---

## 8. Data Distribution and Variety

### 8.1 How diversity is achieved

With the new per-leg approach, natural distribution comes from Sonnet's
reasoning about realistic locations for each specific product and material.
Different materials have different sourcing regions, different processing
steps happen in different industrial clusters, and different products
have different supply chain structures.

This inherent variety in the input data (14,339 unique preprocessing paths
with different material combinations and step sequences) naturally produces
diverse location assignments without artificial strategy-based biasing.

### 8.2 Additional spread mechanisms

To prevent Sonnet from defaulting to the same locations too frequently:

- The system prompt includes guidance to vary locations across records
- A per-record seed or variant indicator in the user message encourages
  Sonnet to consider different regions for similar products
- The geographic reference in the system prompt shows multiple valid
  locations for each type of processing step, encouraging exploration

---

## 9. Validation and Verification

### 9.1 Overview and result structure

Every generated record passes through a four-stage validation pipeline.
Each stage produces a structured result:

```python
@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str]        # Critical -- blocks output
    warnings: List[str]      # Quality flags -- allows output
    corrected_record: Optional[Layer3Record]
```

Records with errors are rejected or sent for regeneration. Records with
only warnings pass through with the warnings logged. Corrective fixes
are applied automatically where safe.

### 9.2 Deterministic validation (12 checks)

These are code-based checks that run on every record without LLM calls.
Each check produces errors (blocking) or warnings (non-blocking).

1. **Schema completeness** -- All required fields present with correct
   types per leg: leg_index (int), material (str), from_step (str),
   to_step (str), from_location (str), to_location (str), from_lat
   (float), from_lon (float), to_lat (float), to_lon (float),
   distance_km (float), transport_modes (list), reasoning (str).

2. **Coordinate range** -- lat in [-90, 90], lon in [-180, 180] for
   every coordinate in every leg.

3. **Land validation** -- Coordinates not in ocean for land-based
   locations. Uses a lightweight land/sea mask to flag coordinates
   that fall in water when the location name implies land.

4. **Distance bounds** -- Each leg distance_km is positive, minimum
   1 km, maximum 25,000 km. Catches zero-distance legs and
   circumnavigation-scale errors.

5. **Material coverage** -- Every material from the input record's
   `materials` array appears in at least one leg. Catches cases where
   Sonnet forgot a material entirely.

6. **Step coverage** -- Every step in `step_material_mapping` appears
   as either from_step or to_step in the legs for the corresponding
   material. Catches skipped processing steps.

7. **Leg continuity** -- Per-material chain: to_location of leg N
   equals from_location of leg N+1. Catches teleportation gaps where
   a material jumps between unconnected locations.

8. **Transport modes** -- All modes in the allowed set {road, rail,
   sea, air, inland_waterway}. Array is non-empty per leg.

9. **Reasoning quality** -- Non-empty, minimum 50 characters. Catches
   placeholder or stub reasoning.

10. **Convergence** -- All material chains converge at the assembly
    step location. The assembly step's location must be identical
    across all materials that participate in it.

11. **Warehouse terminus** -- The final leg ends at the warehouse
    location. Every material chain must terminate at the same
    warehouse destination.

12. **Leg indexing** -- Sequential from 0, no gaps, no duplicates
    within a material chain.

### 9.3 Corrective validation (auto-fixes)

Where safe, the pipeline corrects minor issues rather than rejecting
the record:

- **Distance recomputation**: Recompute total_distance_km from leg
  distances. If the stored value mismatches, correct it and log a
  warning.
- **Coordinate normalization**: Round coordinates to 2 decimal places
  for consistency.
- **Reasoning cleanup**: Strip leading/trailing whitespace, collapse
  multiple spaces, remove control characters.
- **Leg re-indexing**: If leg indices have gaps (e.g., 0, 1, 3),
  re-index sequentially and log the correction.

### 9.4 Semantic validation (LLM-based)

Uses a second LLM call to evaluate plausibility. This catches errors
that are structurally valid but geographically or logistically wrong.

- **Location plausibility**: Is this processing step realistic for
  this city? (e.g., silk spinning in a landlocked desert city is
  suspicious)
- **Route plausibility**: Does the transport route make geographic
  sense? (e.g., shipping from China to Vietnam via the Atlantic)
- **Mode plausibility**: Are modes appropriate for distance and
  geography? (e.g., inland_waterway for a route with no navigable
  rivers)

Returns a `SemanticValidationResult` with per-check scores and an
overall recommendation: accept / review / reject.

**Two-pass flow**: validate -> regenerate failures with feedback ->
re-validate. Records that fail both passes are permanently discarded
with justification logged.

### 9.5 Statistical validation (batch-level)

Runs after all records in a batch are generated. Detects batch-level
quality issues that single-record checks cannot catch.

- **Location diversity**: Flag if >30% of records use the same city
  for any given processing step type. Catches Sonnet defaulting to
  a single location.
- **Distance outliers**: Z-score > 3 across the batch for
  total_distance_km. Catches records that are wildly different from
  the batch norm.
- **Mode distribution**: Reasonable mix of transport modes across the
  batch. Flag if any single mode exceeds 80% or any mode is absent.
- **Duplicate detection**: Hash-based on the ordered sequence of
  (material, from_location, to_location) tuples per record. Catches
  identical or near-identical transport plans.

### 9.6 Validation pipeline flow

```
Record -> Deterministic -> Corrective -> Semantic -> Statistical
              |               |             |            |
          errors=reject   auto-fix     LLM check    batch-level
              |               |             |            |
          pass=continue   log warning   two-pass     flag outliers
```

Records that pass deterministic checks proceed to corrective fixes,
then semantic validation. Statistical checks run on the full batch
after all records are processed. The final output includes only
records that passed all stages.

---

## 10. Pipeline Flow

```
1. Read Layer 2 record
   |
2. Build per-record user prompt (product + materials + steps + warehouse)
   |
3. Send to Claude Sonnet with static system prompt
   |
4. Parse LLM response: extract leg JSON array
   |
5. Deterministic validation:
   - Schema completeness, coordinate ranges, distance bounds
   - Material and step coverage
   - Leg continuity, convergence, warehouse terminus
   - Transport modes, reasoning quality, leg indexing
   |
6. Corrective validation:
   - Recompute total_distance_km, normalize coordinates
   - Clean reasoning text, re-index legs if needed
   |
7. Semantic validation (LLM-based, two-pass):
   - Location, route, and mode plausibility
   - Regenerate failures with feedback, re-validate
   |
8. Compute total_distance_km = sum of all leg distance_km values
   |
9. Write output row (11 carried-forward columns + 2 new columns)
   |
10. Statistical validation (after full batch):
    - Location diversity, distance outliers, mode distribution
    - Duplicate detection
```

Steps 3-4 are the LLM generation call. Steps 5-7 are validation.
Steps 8-9 are assembly and output. Step 10 is batch-level analysis.

---

## 11. What Layer 3 Does NOT Do

- Does NOT compute per-mode aggregate columns (road_km, sea_km, etc.)
  -- that is feature engineering for the model
- Does NOT compute sin/cos coordinate encoding -- that is done in Layer 6
- Does NOT assign supply_chain_type labels -- replaced by actual
  coordinate and distance data
- Does NOT generate free-text origin_region strings -- replaced by
  structured (city, country, lat, lon) tuples
- Does NOT hardcode transport mode selection rules -- Sonnet reasons
  about modes per segment
- Does NOT compute distances from coordinates -- Sonnet reasons about
  actual transport distances per leg

---

## 12. Downstream Impact

### Layer 4 (Packaging)

Layer 4 receives the 13-column Layer 3 output and adds packaging
configurations. No change to the Layer 4 interface beyond the new
column names.

### Layer 6 (Carbon Footprint Calculation)

Layer 6 parses the `transport_legs` JSON and computes cf_transport per leg:

```
cf_transport_leg = (weight_kg / 1000) * distance_km * (EF_mode / 1000)
```

Using the actual transport mode per segment (from the legs array), NOT
the multinomial logit model. The logit model is no longer needed because
the mode is known per leg.

Layer 6 also handles coordinate encoding for model features (see
`/layer_6/COORDINATE_CONVERSION.md`).

### Model Training

The model receives raw lat/lon coordinates, total_distance_km as a summary
feature, and per-mode distance aggregates computed during feature engineering.
Derived features (num_legs, num_countries, crosses_water, sin/cos encoding)
are computed in the feature engineering pipeline, not in Layer 3.
