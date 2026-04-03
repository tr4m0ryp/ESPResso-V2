"""
Data models for Layer 4: Packaging Material Estimation.

Defines the packaging result structure, complete Layer 4 record,
and validation result model for the packaging estimation pipeline.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


def _parse_json_field(value: Any) -> Any:
    """Parse a field that may be a JSON string or already a Python object."""
    if isinstance(value, (list, dict)):
        return value
    return json.loads(str(value))


@dataclass
class PackagingResult:
    """Intermediate result from the LLM for packaging material estimation."""

    paper_cardboard_kg: float
    plastic_kg: float
    other_kg: float
    reasoning: str

    def to_output_lists(self) -> Tuple[List[str], List[float]]:
        """Convert to parallel-list format used by Layer 6.

        Returns a fixed-order tuple of (categories, masses) where categories
        matches the PACKAGING_EMISSION_FACTORS keys in Layer 6.
        """
        categories = ["Paper/Cardboard", "Plastic", "Other/Unspecified"]
        masses = [self.paper_cardboard_kg, self.plastic_kg, self.other_kg]
        return categories, masses

    def total_mass_kg(self) -> float:
        """Return the total packaging mass in kg."""
        return self.paper_cardboard_kg + self.plastic_kg + self.other_kg

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PackagingResult":
        """Parse a PackagingResult from a dictionary.

        Raises ValueError if any required key is missing.
        """
        required_keys = ["paper_cardboard_kg", "plastic_kg", "other_kg", "reasoning"]
        for key in required_keys:
            if key not in data:
                raise ValueError(
                    f"Missing required key '{key}' in PackagingResult data"
                )
        return cls(
            paper_cardboard_kg=round(float(data["paper_cardboard_kg"]), 4),
            plastic_kg=round(float(data["plastic_kg"]), 4),
            other_kg=round(float(data["other_kg"]), 4),
            reasoning=str(data["reasoning"]),
        )


@dataclass
class Layer4Record:
    """Represents a complete Layer 4 output record with packaging estimates."""

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

    # Carried forward from Layer 3 (2 fields)
    transport_legs: List[Dict[str, Any]] = field(default_factory=list)
    total_distance_km: float = 0.0

    # Added by Layer 4 (3 fields)
    packaging_categories: List[str] = field(default_factory=list)
    packaging_masses_kg: List[float] = field(default_factory=list)
    packaging_reasoning: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary for output.

        List and dict fields are serialized as JSON strings to match
        the established cross-layer serialization pattern.
        """
        return {
            "category_id": self.category_id,
            "category_name": self.category_name,
            "subcategory_id": self.subcategory_id,
            "subcategory_name": self.subcategory_name,
            "materials": json.dumps(self.materials),
            "material_weights_kg": json.dumps(self.material_weights_kg),
            "material_percentages": json.dumps(self.material_percentages),
            "total_weight_kg": self.total_weight_kg,
            "preprocessing_path_id": self.preprocessing_path_id,
            "preprocessing_steps": json.dumps(self.preprocessing_steps),
            "step_material_mapping": json.dumps(self.step_material_mapping),
            "transport_legs": json.dumps(self.transport_legs),
            "total_distance_km": self.total_distance_km,
            "packaging_categories": json.dumps(self.packaging_categories),
            "packaging_masses_kg": json.dumps(self.packaging_masses_kg),
            "packaging_reasoning": self.packaging_reasoning,
        }

    @classmethod
    def from_layer3(
        cls, record: Dict[str, Any], result: "PackagingResult"
    ) -> "Layer4Record":
        """Construct a Layer4Record from a Layer 3 record dict and a PackagingResult.

        Parses JSON-encoded list/dict fields from the Layer 3 record and
        populates packaging fields from the PackagingResult.
        """
        packaging_categories, packaging_masses_kg = result.to_output_lists()

        # Parse transport_legs: may be a JSON string or already a list of dicts
        raw_legs = record.get("transport_legs", [])
        if isinstance(raw_legs, str):
            raw_legs = json.loads(raw_legs)

        return cls(
            category_id=str(record["category_id"]),
            category_name=str(record["category_name"]),
            subcategory_id=str(record["subcategory_id"]),
            subcategory_name=str(record["subcategory_name"]),
            materials=_parse_json_field(record["materials"]),
            material_weights_kg=[
                float(w) for w in _parse_json_field(record["material_weights_kg"])
            ],
            material_percentages=[
                float(p) for p in _parse_json_field(record["material_percentages"])
            ],
            total_weight_kg=float(record["total_weight_kg"]),
            preprocessing_path_id=str(record["preprocessing_path_id"]),
            preprocessing_steps=_parse_json_field(record["preprocessing_steps"]),
            step_material_mapping=_parse_json_field(record["step_material_mapping"]),
            transport_legs=list(raw_legs),
            total_distance_km=float(record.get("total_distance_km", 0.0)),
            packaging_categories=packaging_categories,
            packaging_masses_kg=packaging_masses_kg,
            packaging_reasoning=result.reasoning,
        )


@dataclass
class ValidationResult:
    """Result of per-record validation for Layer 4 output."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
