# Task 03: System Prompt Files

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. Layer 3 uses Claude Sonnet to generate per-leg
transport scenarios. The system prompt is split into 8 text files that are
concatenated at runtime. This static system prompt is cached by the API
for cost savings. The user prompt is per-record.

## Design rules

- Prompt files are plain text (.txt), no code
- Files are numbered 00-07 for deterministic ordering
- Content is drawn from LAYER3_DESIGN.md sections 5-8
- No emojis anywhere
- The system prompt must be self-contained: Sonnet should be able to
  produce correct output given only the system prompt + user message

## Reference files to study

- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Sections 5 (Generation
  Design), 6 (Prompt Architecture), 7 (Coordinate System), 8 (Data
  Distribution) contain the content for these prompts
- `data/data_generation/layer_3/prompts/prompts.py` -- Current V1 prompt
  builder with REALISM_DEFINITION, shows the kind of context provided

## The task

Create 8 text files in `prompts/system/`:

### 00_role.txt (~15 lines)

Set Claude Sonnet's persona: a textile supply chain logistics expert with
deep knowledge of global freight routes, manufacturing clusters, and
transport infrastructure. The task is to assign realistic geographic
locations to processing steps and determine transport routes with
distances for textile products.

### 01_task_definition.txt (~30 lines)

Define the exact task:
- Input: product category, materials with weights, step_material_mapping,
  warehouse location
- For each material, determine a realistic location (City, Country, lat,
  lon) for each processing step
- Determine the transport route between consecutive steps: transport
  modes, distance, reasoning narrative
- All material chains must converge at the assembly step
- Final leg goes to the warehouse
- Output: JSON array of transport legs

### 02_geographic_reference.txt (~100 lines)

Reference locations with verified WGS84 coordinates. Organize by function:

**Major textile manufacturing cities** (15-20 entries):
Include cities like Dhaka (23.81, 90.41), Ho Chi Minh City (10.82, 106.63),
Shanghai (31.23, 121.47), Guangzhou (23.13, 113.26), Istanbul (41.01, 28.98),
Tirupur (11.11, 77.34), Suzhou (31.30, 120.62), etc.

**Major container ports** (10-15 entries):
Include Shanghai Yangshan, Singapore, Rotterdam, Los Angeles, Hamburg, etc.

**Major cargo airports** (5-8 entries):
Include Hong Kong, Dubai, Frankfurt, Memphis, etc.

**Warehouse/distribution locations** (4-6 entries):
EU: Rotterdam (51.92, 4.48), Hamburg (53.55, 9.99), Antwerp (51.22, 4.40)
US: Los Angeles (33.94, -118.41), New York/New Jersey (40.68, -74.17)

Note: These are reference coordinates only. Sonnet may use any location.

### 03_routing_guidance.txt (~30 lines)

Contextual knowledge for transport mode reasoning (NOT hardcoded rules):
- Typical infrastructure by region (Asian road/port networks, European
  rail networks, etc.)
- Common freight route patterns (Asia-Europe via Suez, Asia-US West Coast
  transpacific, intra-Asia short sea)
- Multi-modal journey structure: first mile (road) -> trunk (sea/rail/air)
  -> last mile (road)
- Distance-mode relationships (general guidance, not rules): <500km
  typically road, 500-2000km road/rail, >2000km includes sea or air

### 04_convergence_rules.txt (~25 lines)

Material convergence logic:
- Single convergence pattern (~80%): all materials arrive at one CMT
  factory for assembly
- Progressive convergence (~20%): sub-assembly before final assembly
  (footwear, technical outerwear)
- How to identify the assembly step from step_material_mapping
- After assembly, materials travel as one unit to warehouse

### 05_warehouse_rules.txt (~15 lines)

Final warehouse leg:
- The user prompt specifies the target warehouse (EU or US)
- EU warehouses: Rotterdam, Hamburg, Antwerp area
- US warehouses: Los Angeles, New York/New Jersey area
- Include intermediate port stops if needed
- The warehouse is always the final destination

### 06_data_spread.txt (~15 lines)

Location variety guidance:
- Vary locations across records for similar products
- Use the seed number in the user prompt to influence variety
- Do not default to the same city for every record
- Consider secondary manufacturing regions, not just the largest hubs
- Different regions for different materials (cotton from India vs
  polyester from Taiwan)

### 07_output_format.txt (~40 lines)

Exact JSON schema and a complete worked example:
- Show the transport_legs JSON array schema with all 13 fields per leg
- Include a full worked example: a 2-material product (e.g., cotton +
  polyester t-shirt) with 5-6 legs showing material chains, convergence,
  multi-modal legs, and warehouse delivery
- Emphasize: output ONLY the JSON array, no explanation text

## Acceptance criteria

1. All 8 files exist in `prompts/system/` with correct numbering
2. Geographic reference has at least 30 locations with coordinates
3. Output format includes a complete worked example with 5+ legs
4. No file exceeds 120 lines
5. Total system prompt (all 8 files concatenated) is under 400 lines
6. No emojis in any file
7. Content is consistent with LAYER3_DESIGN.md sections 5-8
