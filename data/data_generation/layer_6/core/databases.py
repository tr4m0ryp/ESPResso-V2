"""Reference database wrappers for Layer 6 carbon footprint calculation.

Provides MaterialDatabase and ProcessingDatabase dataclasses that hold
emission factor lookup tables loaded from Parquet reference files.
Both databases integrate material alias resolution so that
LLM-generated material names are mapped to canonical reference entries
before any matching attempt.

Primary classes:
    MaterialDatabase -- Raw material emission factor lookup.
    ProcessingDatabase -- Material-process combination EF lookup.
    CalculationResult -- Per-record calculation output container.

Dependencies:
    material_aliases.resolve_material_name for name resolution.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from data.data_generation.layer_6.core.material_aliases import (
    resolve_material_name
)

logger = logging.getLogger(__name__)


@dataclass
class MaterialDatabase:
    """Database of material emission factors.

    Wraps a dictionary of material_name -> EF (kgCO2e/kg).
    All lookups pass through alias resolution first.
    """

    materials: Dict[str, float] = field(default_factory=dict)
    material_refs: Dict[str, str] = field(default_factory=dict)

    def get_emission_factor(
        self,
        material_name: str
    ) -> Optional[float]:
        """Get emission factor for a material with alias resolution.

        Resolution order:
        1. Resolve name via alias map.
        2. Exact match on resolved name.
        3. Case-insensitive match.
        4. Substring match (fallback).

        Args:
            material_name: Name of the material (raw or canonical).

        Returns:
            Emission factor in kgCO2e/kg, or None if not found.
        """
        resolved = resolve_material_name(material_name)

        # Exact match on resolved name
        if resolved in self.materials:
            return self.materials[resolved]

        # Case-insensitive match
        resolved_lower = resolved.lower().strip()
        for name, ef in self.materials.items():
            if name.lower().strip() == resolved_lower:
                return ef

        # Substring match (last resort)
        for name, ef in self.materials.items():
            name_lower = name.lower()
            if resolved_lower in name_lower or name_lower in resolved_lower:
                return ef

        return None


@dataclass
class ProcessingDatabase:
    """Database of material-processing combination emission factors.

    All lookups pass through alias resolution for the material name.
    """

    combinations: Dict[Tuple[str, str], float] = field(
        default_factory=dict
    )

    def get_combined_ef(
        self,
        material_name: str,
        process_name: str
    ) -> Optional[float]:
        """Get combined EF for a material-process combination.

        Resolution order:
        1. Resolve material name via alias map.
        2. Exact match on (resolved_material, process).
        3. Case-insensitive match.
        4. Partial material match with exact process.

        Args:
            material_name: Name of the material (raw or canonical).
            process_name: Name of the processing step.

        Returns:
            Combined EF in kgCO2e/kg, or None if not found.
        """
        resolved = resolve_material_name(material_name)

        # Exact match
        key = (resolved, process_name)
        if key in self.combinations:
            return self.combinations[key]

        # Case-insensitive match
        mat_lower = resolved.lower().strip()
        proc_lower = process_name.lower().strip()

        for (mat, proc), ef in self.combinations.items():
            if (mat.lower().strip() == mat_lower
                    and proc.lower().strip() == proc_lower):
                return ef

        # Partial material match
        for (mat, proc), ef in self.combinations.items():
            mat_l = mat.lower().strip()
            proc_l = proc.lower().strip()
            if (mat_lower in mat_l or mat_l in mat_lower):
                if proc_l == proc_lower or proc_lower in proc_l:
                    return ef

        return None


@dataclass
class CalculationResult:
    """Result of carbon footprint calculation for a single record."""

    cf_raw_materials_kg_co2e: float = 0.0
    cf_transport_kg_co2e: float = 0.0
    cf_processing_kg_co2e: float = 0.0
    cf_packaging_kg_co2e: float = 0.0
    cf_modelled_kg_co2e: float = 0.0
    cf_adjustment_kg_co2e: float = 0.0
    cf_total_kg_co2e: float = 0.0
    transport_mode_probabilities: Dict[str, float] = field(
        default_factory=dict
    )
    weighted_ef_g_co2e_tkm: float = 0.0
    calculation_notes: List[str] = field(default_factory=list)
    is_valid: bool = True
