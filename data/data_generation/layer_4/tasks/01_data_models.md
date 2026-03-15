Create data models (dataclasses) for the V2 Layer 4 pipeline in `models/models.py`.

Create the `models/` directory with `__init__.py` re-exporting all public models.

## PackagingResult

Intermediate result from the LLM before conversion to output format. 4 fields:

- `paper_cardboard_kg`: float -- mass of paper/cardboard packaging in kg
- `plastic_kg`: float -- mass of plastic packaging in kg
- `other_kg`: float -- mass of other/unspecified packaging in kg
- `reasoning`: str -- 1-3 sentence explanation of packaging choices

Methods:

- `to_output_lists() -> Tuple[List[str], List[float]]`: Convert to the parallel-list
  format used by Layer 6. Returns:
  - `categories = ["Paper/Cardboard", "Plastic", "Other/Unspecified"]` (always this
    fixed order)
  - `masses = [self.paper_cardboard_kg, self.plastic_kg, self.other_kg]`
  Note: use `"Other/Unspecified"` (not `"Other"`) to match Layer 6's
  `PACKAGING_EMISSION_FACTORS` key.

- `total_mass_kg() -> float`: Return `paper_cardboard_kg + plastic_kg + other_kg`.

- `@classmethod from_dict(cls, data: Dict[str, Any]) -> "PackagingResult"`: Parse from a
  dict with keys `paper_cardboard_kg`, `plastic_kg`, `other_kg`, `reasoning`. Cast numeric
  fields to float, round to 4 decimal places. Raise `ValueError` if any required key is
  missing.

## Layer4Record

Represents a complete Layer 4 output record. Inherits all Layer 3 fields and adds packaging
output. 16 fields total.

Carried forward from L1/L2 (11 fields, same types as Layer3Record):
- `category_id`: str
- `category_name`: str
- `subcategory_id`: str
- `subcategory_name`: str
- `materials`: List[str]
- `material_weights_kg`: List[float]
- `material_percentages`: List[float]
- `total_weight_kg`: float
- `preprocessing_path_id`: str
- `preprocessing_steps`: List[str]
- `step_material_mapping`: Dict[str, List[str]]

Carried forward from Layer 3 (2 fields):
- `transport_legs`: List[Dict[str, Any]] -- serialized transport legs (kept as list of dicts,
  not TransportLeg objects, to avoid cross-layer model coupling)
- `total_distance_km`: float

Added by Layer 4 (3 fields):
- `packaging_categories`: List[str] -- always `["Paper/Cardboard", "Plastic", "Other/Unspecified"]`
- `packaging_masses_kg`: List[float] -- `[paper_mass, plastic_mass, other_mass]` in kg
- `packaging_reasoning`: str -- explanation of packaging choices

Methods:

- `to_dict() -> Dict[str, Any]`: Convert to dictionary for output. Serialize `materials`,
  `material_weights_kg`, `material_percentages`, `preprocessing_steps`,
  `step_material_mapping`, `transport_legs`, `packaging_categories`, `packaging_masses_kg`
  as JSON strings. Keep scalar fields as-is.

- `@classmethod from_layer3(cls, record: Dict[str, Any], result: PackagingResult) -> "Layer4Record"`:
  Construct from a Layer 3 record dict and a PackagingResult. Parse JSON-encoded list/dict
  fields from the Layer 3 record using `_parse_json_field()`. Call `result.to_output_lists()`
  to populate `packaging_categories` and `packaging_masses_kg`.

## ValidationResult

Result of per-record validation. 3 fields:

- `is_valid`: bool -- True if all hard checks pass
- `errors`: List[str] -- hard failures (default empty list)
- `warnings`: List[str] -- soft issues (default empty list)

## Design rules

- Use Python dataclasses with `from dataclasses import dataclass, field`.
- Use `field(default_factory=list)` for mutable defaults.
- Use standard library `typing` for type hints.
- Use `logging.getLogger(__name__)` for the module logger.
- Use full package path imports: `from data.data_generation.layer_4.models.models import ...`
- No emojis anywhere.
- Do NOT import Layer 3 model classes (TransportLeg, Layer3Record). Layer 4 receives
  Layer 3 data as dicts/DataFrames, not typed objects, to keep layers decoupled.

## Files to create

- `models/__init__.py` -- re-export PackagingResult, Layer4Record, ValidationResult
- `models/models.py` -- all three dataclasses

## Files to remove

None. This is a new directory.

## Reference

- Layer 3 models pattern: `layer_3/models/models.py`
- Design doc: `layer_4/DESIGN_V2.md` sections 5 (Output Schema) and 10 (Response Parsing)
