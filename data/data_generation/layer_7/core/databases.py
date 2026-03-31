"""Reference database wrappers for Layer 7 water footprint calculation.

Provides dataclasses holding water consumption lookup tables loaded
from CSV reference files. Includes material, processing step,
packaging, and AWARE characterization factor databases.

Primary classes:
    WaterMaterialDatabase -- Raw material water consumption lookup.
    WaterProcessingDatabase -- Processing step water consumption lookup.
    WaterPackagingDatabase -- Packaging water consumption lookup.
    AWAREDatabase -- AWARE characterization factor lookup by country.
    WaterCalculationResult -- Per-record calculation output container.

Dependencies:
    Layer 6 material_aliases for name resolution (shared across layers).
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from data.data_generation.layer_6.core.material_aliases import (
    resolve_material_name,
)

logger = logging.getLogger(__name__)


@dataclass
class WaterMaterialDatabase:
    """Database of material water consumption factors.

    Maps material_name -> water_consumption_m3_per_kg.
    All lookups pass through alias resolution first.
    """

    materials: Dict[str, float] = field(default_factory=dict)

    def get_water_factor(
        self, material_name: str
    ) -> Optional[float]:
        """Get water consumption factor for a material.

        Resolution order: exact, case-insensitive, substring.

        Args:
            material_name: Name of the material (raw or canonical).

        Returns:
            Water consumption in m3/kg, or None if not found.
        """
        resolved = resolve_material_name(material_name)
        return _fuzzy_lookup(resolved, self.materials)


@dataclass
class WaterProcessingDatabase:
    """Database of processing step water consumption factors.

    Stores both step-level and material-process combination lookups.
    """

    steps: Dict[str, float] = field(default_factory=dict)
    combinations: Dict[Tuple[str, str], float] = field(
        default_factory=dict
    )

    def get_step_water_factor(
        self, process_name: str
    ) -> Optional[float]:
        """Get water consumption for a processing step.

        Args:
            process_name: Name of the processing step.

        Returns:
            Water consumption in m3/kg, or None if not found.
        """
        return _fuzzy_lookup(process_name, self.steps)

    def get_combination_water_factor(
        self, material_name: str, process_name: str
    ) -> Optional[float]:
        """Get water consumption for a material-process combination.

        Args:
            material_name: Canonical material name.
            process_name: Processing step name.

        Returns:
            Water consumption in m3/kg, or None if not found.
        """
        resolved = resolve_material_name(material_name)

        key = (resolved, process_name)
        if key in self.combinations:
            return self.combinations[key]

        # Case-insensitive match
        mat_lower = resolved.lower().strip()
        proc_lower = process_name.lower().strip()
        for (mat, proc), wf in self.combinations.items():
            if (mat.lower().strip() == mat_lower
                    and proc.lower().strip() == proc_lower):
                return wf

        return None


@dataclass
class WaterPackagingDatabase:
    """Database of packaging water consumption factors.

    Maps packaging category -> water_consumption_m3_per_kg.
    """

    categories: Dict[str, float] = field(default_factory=dict)

    def get_packaging_water_factor(
        self, category: str
    ) -> Optional[float]:
        """Get water consumption for a packaging category.

        Args:
            category: Packaging category name.

        Returns:
            Water consumption in m3/kg, or None if not found.
        """
        return _fuzzy_lookup(category, self.categories)


@dataclass
class AWAREDatabase:
    """AWARE characterization factor database.

    Maps country_name -> AWARE CF (annual). Separate databases for
    agricultural and non-agricultural use.
    """

    factors: Dict[str, float] = field(default_factory=dict)
    fallback: float = 43.1

    def get_factor(self, country_name: str) -> float:
        """Get AWARE CF for a country, with fallback to global value.

        Args:
            country_name: Country name (after alias resolution).

        Returns:
            AWARE characterization factor (annual).
        """
        if country_name in self.factors:
            return self.factors[country_name]

        # Case-insensitive match
        lower = country_name.lower().strip()
        for name, cf in self.factors.items():
            if name.lower().strip() == lower:
                return cf

        return self.fallback


@dataclass
class WaterCalculationResult:
    """Result of water footprint calculation for a single record."""

    wf_raw_materials_m3_world_eq: float = 0.0
    wf_processing_m3_world_eq: float = 0.0
    wf_packaging_m3_world_eq: float = 0.0
    wf_total_m3_world_eq: float = 0.0
    calculation_notes: List[str] = field(default_factory=list)
    is_valid: bool = True


def _fuzzy_lookup(
    key: str, lookup: Dict[str, float]
) -> Optional[float]:
    """Fuzzy string lookup: exact, case-insensitive, substring.

    Args:
        key: Lookup key string.
        lookup: Dictionary to search.

    Returns:
        Matched value, or None if no match found.
    """
    if key in lookup:
        return lookup[key]

    key_lower = key.lower().strip()
    for name, val in lookup.items():
        if name.lower().strip() == key_lower:
            return val

    for name, val in lookup.items():
        name_lower = name.lower()
        if key_lower in name_lower or name_lower in key_lower:
            return val

    return None
