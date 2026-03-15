# Data Generation Pipeline
## 6-Layer Synthetic Data Generation for ESPResso

**Project:** ESPResso Carbon Footprint Model
**Date:** 2025-02-04
**Version:** 2.0

---

## 1. Problem Overview

### 1.1 Objective
Generate 1,000,000+ realistic synthetic product records for training the ESPResso carbon footprint model. Records must span valid combinations of:
- Materials (87 base materials)
- Processing steps (33 steps, 1,200+ valid combinations)
- Transport scenarios (5 modes, distance-dependent probabilities)
- Packaging configurations (4 categories)

### 1.2 Pipeline Architecture
Six sequential layers transform simple product categories into fully-specified carbon footprints:

```
Layer 1: Product Composition     → Materials, weights, percentages
Layer 2: Processing Paths        → Valid manufacturing sequences
Layer 3: Transport Scenarios     → Distances, supply chain geography
Layer 4: Packaging Config        → Protection requirements, materials
Layer 5: Validation              → Deterministic + semantic + statistical checks
Layer 6: Carbon Calculation      → Deterministic kg CO2e computation
```

### 1.3 Scale Targets
- **Layer 1 Output:** ~45,000 products
- **Layer 2 Expansion:** ~462,600 variants (processing combinations)
- **Layer 3 Expansion:** ~2.3M scenarios (transport variants)
- **Layer 4 Expansion:** ~4.6M configs (packaging variants)
- **Layer 5 Filter:** ~850,000 validated records
- **Layer 6 Output:** 850,000 training records with carbon footprints

---

## 2. Layer Specifications

### 2.1 Layer 1: Product Composition Generator

**Purpose:** Generate realistic material compositions for each product subcategory.

**Input:**
- `datasets/final/taxonomy_category.csv` - Category/subcategory definitions
- `datasets/final/base_materials.csv` - 87 available materials

**Output:**
- `datasets/generated/layer_1/` - Product compositions
- Schema: `category_id, category_name, subcategory_id, subcategory_name, materials[], material_weights[], material_percentages[], total_weight_kg`

**Implementation:**
- **Model:** Claude Sonnet 4.6 (via UVA)
- **Client:** `layer_1/clients/api_client.py`
- **Core:** `layer_1/core/orchestrator.py`, `generator.py`
- **Data Models:** `layer_1/models/materials.py`, `taxonomy.py`
- **Prompts:** `layer_1/prompts/prompts.py`
- **Output:** `layer_1/io/output.py`

**Key Features:**
- Category-aware material selection (e.g., Down Coats → polyester shell, duck down fill)
- Realistic weight distributions per subcategory
- Material percentage validation (sums to 100%)

---

### 2.2 Layer 2: Processing Path Generator

**Purpose:** Enumerate all valid preprocessing pathways for each material composition.

**Input:**
- Layer 1 output CSV
- `datasets/final/material_processing_combinations.csv` - 1,200+ valid pairs

**Output:**
- Multiple records per Layer 1 input (one per valid pathway)
- Schema adds: `preprocessing_path_id, preprocessing_steps[]`

**Implementation:**
- **Model:** Qwen3 235B (128K+ context window required)
- **Core:** `layer_2/core/orchestrator.py`, `generator.py`
- **Data Models:** `layer_2/models/processing_data.py`
- **Input Reader:** `layer_2/io/layer1_reader.py`

**Challenge:** Material-process lookup requires ~180K tokens context window.

---

### 2.3 Layer 3: Transport Scenario Generator

**Purpose:** Generate geographically realistic supply chain distances.

**Input:**
- Layer 2 output
- Geographic heuristics (material origin → processing → assembly)

**Output:**
- 5 distance variants per record
- Schema adds: `total_transport_distance_km, supply_chain_type`

**Implementation:**
- **Model:** Claude Sonnet 4.6 (via UVA)
- **Client:** `layer_3/clients/api_client.py`, `nemotron_ultra_client.py`
- **Core:** `layer_3/core/orchestrator.py`, `generator.py`, `controller.py`
- **Controller:** Extra validation layer for scenario quality control

---

### 2.4 Layer 4: Packaging Configuration Generator

**Purpose:** Generate context-appropriate packaging based on product characteristics.

**Input:**
- Layer 3 output
- Product weight, fragility indicators, transport distance

**Output:**
- 2 packaging configs per record
- Schema adds: `packaging_materials[], packaging_masses_kg[]`

**Implementation:**
- **Model:** Claude Sonnet 4.6 (via UVA)
- **Client:** `layer_4/clients/api_client.py`
- **Core:** `layer_4/core/orchestrator.py`, `generator.py`
- **Entry Point:** `layer_4/main.py`
- **Checkpointing:** Resume capability for long runs

---

### 2.5 Layer 5: Validation Layer

**Purpose:** Quality gate - classify records as accepted/review/rejected.

**Implementation:**
- **Core:** `layer_5/core/orchestrator.py`, `orchestrator_batch.py`
- **Validators:**
  - `deterministic_validator.py` - Schema and data type validation
  - `semantic_validator.py` - AI-driven coherence evaluation
  - `statistical_validator.py` - Distribution and outlier checks

**Validation Pipeline:**
1. **Deterministic Validation**
   - Material existence in database
   - Mass balance checks (weights sum to total)
   - Processing step validity
   - Distance range enforcement (500-25,000 km)

2. **Semantic Validation**
   - **Model:** Claude Sonnet 4.6 (via UVA)
   - 5 dimensions: Material-Product, Processing-Material, Transport-SupplyChain, Packaging-Product, Overall Realism
   - Scoring: 0.85+ (accept), 0.70-0.85 (review), <0.70 (reject)

3. **Reward Scoring**
   - **Model:** Claude Sonnet 4.6 (via UVA)
   - Quality threshold: 0.60+

4. **Statistical Validation**
   - Deduplication (MD5 hashing)
   - Distribution monitoring
   - Outlier detection (3-sigma)

**Output Files:**
- `layer_5_validated.csv` - Accepted records (~850K)
- `layer_5_review_queue.csv` - Requires manual inspection
- `layer_5_rejected.csv` - Failed validation

---

### 2.6 Layer 6: Carbon Calculation Layer

**Purpose:** Deterministic carbon footprint computation.

**Implementation:**
- **Language:** C (performance-critical)
- **Entry:** `layer_6/main.c`
- **Core Logic:** `layer_6/layer6_calculation.c`, `layer6_calculation.h`
- **Build:** `layer_6/Makefile` → `layer6_calculate`
- **Python Wrapper:** `layer_6/` contains Python integration modules

**Formula:**
```
CF_total = (CF_raw + CF_processing + CF_transport + CF_packaging) × 1.02
```

**Performance:** ~10,000 records/second

**Output Schema:** Full record + 8 carbon footprint fields

---

## 3. Execution

### 3.1 Sequential Execution
```bash
# Run individual layers
python data_generation/scripts/run_layer_1.py
python data_generation/scripts/run_layer_2.py
python data_generation/scripts/run_layer_3.py
python data_generation/scripts/run_layer_4.py

# Layer 5 validation
python data_generation/layer_5/main.py

# Layer 6 carbon calculation
make -C data_generation/layer_6
./data_generation/layer_6/layer6_calculate
```

### 3.2 Optimized Scripts
```bash
# Layer 3 with parallel processing
bash data_generation/scripts/run_layer3_optimized.sh

# Layer 4 optimized
bash data_generation/scripts/run_layer4_optimized.sh

# Layer 4 verification
bash data_generation/scripts/verify_layer4.sh

# Layer 6 Python wrapper
python data_generation/scripts/run_layer_6.py
```

---

## 4. Dependencies

### 4.1 External APIs
- UVA AI API
- Requires `UVA_API_KEY` in `.env` (or NVIDIA keys for legacy mode)

### 4.2 Internal Dependencies
- `datasets/final/` - Material database, taxonomy, processing combinations
- `datasets/generated/` - Layer-to-layer data flow

### 4.3 Shared Components
- `shared/api_client.py` - NVIDIA API client (function calling interface)
- `shared/parallel_processor.py` - Parallel execution utilities

---

## 5. See Also
- `datasets/README.md` - Data storage structure
- `model/README.md` - ESPResso model documentation
