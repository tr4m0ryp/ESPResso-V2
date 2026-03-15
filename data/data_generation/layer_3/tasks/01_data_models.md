# Task 01: Data Models

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. It has 6 layers, each adding data to the pipeline.
Layer 3 generates coordinate-based transport scenarios: for each product
record from Layer 2, it assigns geographic locations to processing steps
and determines transport routes with distances between them. The output
is a JSON array of transport legs per record plus a computed total distance.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for all data structures
- Use standard library types (List, Dict, Optional) from typing
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path: `from data.data_generation.layer_3.models.models import ...`
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/models/models.py` -- Pattern for dataclass
  definitions, validation result structures, and serialization helpers
- `data/data_generation/layer_3/core/generator.py` -- Current V1 models
  (TransportScenario, Layer3Record) that this task replaces
- `data/data_generation/layer_3/io/layer2_reader.py` -- Layer2Record
  dataclass showing the input schema

## The task

Create `models/models.py` and `models/__init__.py` under layer_3.

### models/__init__.py

Re-export all public dataclasses from models.py.

### models/models.py

Define these dataclasses:

**TransportLeg:**
```python
@dataclass
class TransportLeg:
    leg_index: int
    material: str
    from_step: str
    to_step: str
    from_location: str
    to_location: str
    from_lat: float
    from_lon: float
    to_lat: float
    to_lon: float
    distance_km: float
    transport_modes: List[str]
    reasoning: str
```

Include a `to_dict() -> Dict[str, Any]` method and a
`from_dict(data: Dict[str, Any]) -> TransportLeg` classmethod.

**Layer3Record:**
```python
@dataclass
class Layer3Record:
    # Carried forward from L1/L2 (11 fields)
    category_id: str
    category_name: str
    subcategory_id: str
    subcategory_name: str
    materials: List[str]
    material_weights_kg: List[float]
    material_percentages: List[float]
    total_weight_kg: float
    preprocessing_path_id: str
    preprocessing_steps: List[str]
    step_material_mapping: Dict[str, List[str]]
    # Added by Layer 3 (2 fields)
    transport_legs: List[TransportLeg]
    total_distance_km: float
```

Include `to_dict()` (serializes transport_legs as JSON string, other
list/dict fields as JSON strings), `from_dict()` classmethod, and a
`compute_total_distance() -> float` method that sums leg distances.

**ValidationResult:**
```python
@dataclass
class ValidationResult:
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    corrections_applied: List[str] = field(default_factory=list)
    corrected_record: Optional['Layer3Record'] = None
```

**SemanticValidationResult:**
```python
@dataclass
class SemanticValidationResult:
    location_plausibility_score: float
    route_plausibility_score: float
    mode_plausibility_score: float
    issues_found: List[str] = field(default_factory=list)
    recommendation: str = "review"  # "accept", "review", "reject"
```

**StatisticalValidationResult:**
```python
@dataclass
class StatisticalValidationResult:
    is_duplicate: bool = False
    duplicate_similarity: float = 0.0
    is_outlier: bool = False
    outlier_type: Optional[str] = None
    location_diversity_ok: bool = True
    mode_distribution_ok: bool = True
    distribution_issues: List[str] = field(default_factory=list)
```

## Acceptance criteria

1. `from data.data_generation.layer_3.models import TransportLeg, Layer3Record, ValidationResult` works
2. `TransportLeg.from_dict(leg.to_dict())` round-trips correctly
3. `Layer3Record.from_dict(record.to_dict())` round-trips correctly
4. `Layer3Record.compute_total_distance()` returns the sum of leg distances
5. All dataclasses use `field(default_factory=list)` for mutable defaults
6. No V1 fields remain (no transport_scenario_id, supply_chain_type, origin_region)
