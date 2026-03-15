# Data Calculation Module
## Legacy C-Based Carbon Calculator

**Project:** ESPResso Carbon Footprint Model
**Status:** DEPRECATED
**Date:** 2025-01-17
**Replacement:** `data_generation/layer_6/`

---

## 1. Overview

### 1.1 Status
This module has been superseded by `data_generation/layer_6/`. It remains in the repository for reference purposes only.

### 1.2 Original Purpose
Calculate deterministic carbon footprints using C implementation for performance. Processes raw material data through transport, processing, and packaging calculations.

### 1.3 Migration Path
**Recommended:** Use `data_generation/layer_6/` instead
- Improved integration with 6-layer pipeline
- Same calculation logic, better I/O handling
- Direct CSV processing from Layer 5 output

---

## 2. Architecture

### 2.1 Directory Structure
```
data_calculation/
├── src/                    # C source files
│   ├── main.c             # Entry point
│   ├── transport/         # Transport calculation logic
│   ├── processing/        # Material processing calculations
│   ├── packaging/         # Packaging footprint calculations
│   └── utils/             # CSV parsing utilities
├── include/               # Header files (.h)
├── bin/                   # Compiled binaries (gitignored)
└── Makefile              # Build configuration
```

### 2.2 Build Process
```bash
# Compile
make

# Run (legacy - do not use for new work)
./bin/carbon_calculator input.csv output.csv
```

---

## 3. Components (Legacy)

### 3.1 Transport Calculator (`src/transport/`)
- **Purpose:** Multinomial logit model for mode selection
- **Key Files:** `transport.c`, `emission_factors.c`
- **Headers:** `include/transport/transport.h`, `emission_factors.h`
- **Formula:** Distance-weighted emission factors

### 3.2 Processing Calculator (`src/processing/`)
- **Purpose:** Material-process combination emissions
- **Key Files:** `material_processing.c`
- **Headers:** `include/processing/material_processing.h`

### 3.3 Packaging Calculator (`src/packaging/`)
- **Purpose:** Category-based packaging footprints
- **Key Files:** `packaging.c`
- **Headers:** `include/packaging/packaging.h`

### 3.4 Raw Materials (`src/raw_materials/`)
- **Purpose:** Raw material emission factor calculations
- **Key Files:** `raw_materials.c`
- **Headers:** `include/raw_materials/raw_materials.h`

### 3.5 Adjustments (`src/adjustments/`)
- **Purpose:** Internal transport and waste adjustments (2% additive)
- **Key Files:** `adjustments.c`
- **Headers:** `include/adjustments/adjustments.h`

---

## 4. Deprecation Notice

**Do not use for new development.** This module is retained for:
- Historical reference
- Comparison validation against Layer 6
- Potential edge case debugging

**Active Development:** See `data_generation/layer_6/README.md`
