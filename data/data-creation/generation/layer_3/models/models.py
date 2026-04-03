"""
Data models for Layer 3: Transport Scenario Generation.

Defines the transport leg structure, complete Layer 3 record,
and validation result models for the coordinate-based transport pipeline.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


@dataclass
class TransportLeg:
    """Represents a single transport leg between two processing steps."""

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

    def to_dict(self) -> Dict[str, Any]:
        """Convert transport leg to dictionary."""
        return {
            "leg_index": self.leg_index,
            "material": self.material,
            "from_step": self.from_step,
            "to_step": self.to_step,
            "from_location": self.from_location,
            "to_location": self.to_location,
            "from_lat": self.from_lat,
            "from_lon": self.from_lon,
            "to_lat": self.to_lat,
            "to_lon": self.to_lon,
            "distance_km": self.distance_km,
            "transport_modes": self.transport_modes,
            "reasoning": self.reasoning,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TransportLeg":
        """Create a TransportLeg from a dictionary."""
        return cls(
            leg_index=int(data["leg_index"]),
            material=str(data["material"]),
            from_step=str(data["from_step"]),
            to_step=str(data["to_step"]),
            from_location=str(data["from_location"]),
            to_location=str(data["to_location"]),
            from_lat=float(data["from_lat"]),
            from_lon=float(data["from_lon"]),
            to_lat=float(data["to_lat"]),
            to_lon=float(data["to_lon"]),
            distance_km=float(data["distance_km"]),
            transport_modes=list(data["transport_modes"]),
            reasoning=str(data["reasoning"]),
        )


@dataclass
class Layer3Record:
    """Represents a complete Layer 3 output record with transport legs."""

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
    transport_legs: List[TransportLeg] = field(default_factory=list)
    total_distance_km: float = 0.0

    def compute_total_distance(self) -> float:
        """Compute total distance by summing all leg distances."""
        return sum(leg.distance_km for leg in self.transport_legs)

    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary for CSV output.

        List/dict fields and transport_legs are serialized as JSON strings
        to match the existing Layer 2 serialization pattern.
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
            "transport_legs": json.dumps(
                [leg.to_dict() for leg in self.transport_legs]
            ),
            "total_distance_km": self.total_distance_km,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Layer3Record":
        """Create a Layer3Record from a dictionary.

        Handles both raw Python objects and JSON-encoded strings
        for list/dict fields (as produced by to_dict).
        """
        def _parse_json_field(value: Any) -> Any:
            if isinstance(value, (list, dict)):
                return value
            return json.loads(str(value))

        materials = _parse_json_field(data["materials"])
        material_weights_kg = _parse_json_field(data["material_weights_kg"])
        material_percentages = _parse_json_field(data["material_percentages"])
        preprocessing_steps = _parse_json_field(data["preprocessing_steps"])
        step_material_mapping = _parse_json_field(
            data["step_material_mapping"]
        )

        # Parse transport_legs: may be a JSON string or a list of dicts
        raw_legs = data.get("transport_legs", [])
        if isinstance(raw_legs, str):
            raw_legs = json.loads(raw_legs)
        transport_legs = [TransportLeg.from_dict(leg) for leg in raw_legs]

        return cls(
            category_id=str(data["category_id"]),
            category_name=str(data["category_name"]),
            subcategory_id=str(data["subcategory_id"]),
            subcategory_name=str(data["subcategory_name"]),
            materials=materials,
            material_weights_kg=[float(w) for w in material_weights_kg],
            material_percentages=[float(p) for p in material_percentages],
            total_weight_kg=float(data["total_weight_kg"]),
            preprocessing_path_id=str(data["preprocessing_path_id"]),
            preprocessing_steps=preprocessing_steps,
            step_material_mapping=step_material_mapping,
            transport_legs=transport_legs,
            total_distance_km=float(data.get("total_distance_km", 0.0)),
        )


@dataclass
class ValidationResult:
    """Result of deterministic validation checks on a Layer 3 record."""

    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    corrections_applied: List[str] = field(default_factory=list)
    corrected_record: Optional["Layer3Record"] = None


@dataclass
class SemanticValidationResult:
    """Result of semantic consistency validation via LLM."""

    location_plausibility_score: float
    route_plausibility_score: float
    mode_plausibility_score: float
    issues_found: List[str] = field(default_factory=list)
    recommendation: str = "review"  # "accept", "review", "reject"


@dataclass
class StatisticalValidationResult:
    """Result of statistical analysis and deduplication."""

    is_duplicate: bool = False
    duplicate_similarity: float = 0.0
    is_outlier: bool = False
    outlier_type: Optional[str] = None
    location_diversity_ok: bool = True
    mode_distribution_ok: bool = True
    distribution_issues: List[str] = field(default_factory=list)
