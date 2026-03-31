"""Component water footprint calculators for Layer 7.

Contains individual calculation methods for each water footprint
component: raw materials (with AWARE agri), processing (with AWARE
nonagri), and packaging (no AWARE). No transport component.

Formulas (D4):
    WF_raw  = sum(w_i * WU_material_i * AWARE_agri_i)
    WF_proc = sum(w_m * WU_process_p  * AWARE_nonagri_p)
    WF_pack = sum(m_j * WU_packaging_j)
    WF_total = WF_raw + WF_proc + WF_pack

Primary functions:
    calculate_raw_materials_water -- WF from material water consumption.
    calculate_processing_water -- WF from processing water consumption.
    calculate_packaging_water -- WF from packaging water consumption.
    calculate_total_water -- Sum of all components.

Dependencies:
    databases module for WaterMaterialDatabase, etc.
    country_resolver for AWARE factor extraction.
"""

from typing import Any, Dict, List, Optional, Tuple

from data.data_generation.layer_7.core.databases import (
    WaterMaterialDatabase,
    WaterProcessingDatabase,
    WaterPackagingDatabase,
    AWAREDatabase,
)
from data.data_generation.layer_7.core.country_resolver import (
    extract_material_origins,
    extract_step_locations,
)
from data.data_generation.layer_6.core.material_aliases import (
    resolve_material_name,
)


def calculate_raw_materials_water(
    materials: List[str],
    weights_kg: List[float],
    water_db: WaterMaterialDatabase,
    aware_agri_db: AWAREDatabase,
    transport_legs_raw: Any,
    country_aliases: Optional[Dict[str, str]] = None,
    stats: Optional[Dict[str, int]] = None,
) -> Tuple[float, List[str]]:
    """Calculate raw materials water footprint with AWARE weighting.

    WF_raw = sum(weight_i * WU_material_i * AWARE_agri_i)

    The AWARE agri factor is looked up per-material using the origin
    country extracted from the first transport leg.

    Args:
        materials: List of material names.
        weights_kg: List of material weights in kg.
        water_db: Material water consumption database.
        aware_agri_db: AWARE agricultural factors database.
        transport_legs_raw: Raw transport_legs for country extraction.
        country_aliases: Country alias dictionary.
        stats: Mutable dict with 'matches' and 'misses' counters.

    Returns:
        Tuple of (water footprint in m3 world-eq, list of notes).
    """
    wf_raw = 0.0
    notes = []

    if stats is None:
        stats = {'matches': 0, 'misses': 0}

    if len(materials) != len(weights_kg):
        notes.append("Material/weight count mismatch")
        return 0.0, notes

    # Build origin lookup keyed by material name
    origins = extract_material_origins(
        transport_legs_raw, materials, country_aliases
    )

    for material, weight in zip(materials, weights_kg):
        resolved = resolve_material_name(material)
        wu = water_db.get_water_factor(resolved)

        # Look up origin country by material name
        origin = origins.get(material, {})
        country = origin.get('country', '')
        aware_cf = (
            aware_agri_db.get_factor(country)
            if country else aware_agri_db.fallback
        )

        if wu is not None:
            wf_raw += weight * wu * aware_cf
            stats['matches'] += 1
        else:
            # Default water consumption: 0.01 m3/kg (conservative)
            default_wu = 0.01
            wf_raw += weight * default_wu * aware_cf
            notes.append(
                f"Unknown material '{material}' "
                f"(resolved: '{resolved}'), used default WU"
            )
            stats['misses'] += 1

    return wf_raw, notes


def calculate_processing_water(
    materials: List[str],
    weights_kg: List[float],
    processing_steps: List[str],
    water_db: WaterProcessingDatabase,
    aware_nonagri_db: AWAREDatabase,
    transport_legs_raw: Any,
    country_aliases: Optional[Dict[str, str]] = None,
) -> Tuple[float, List[str]]:
    """Calculate processing water footprint with AWARE weighting.

    WF_proc = sum(weight_m * WU_process_p * AWARE_nonagri_p)

    For each material-step pair, looks up the combination water
    factor first, then falls back to the step-level factor.

    Args:
        materials: List of material names.
        weights_kg: List of material weights in kg.
        processing_steps: List of processing step names.
        water_db: Processing water consumption database.
        aware_nonagri_db: AWARE non-agricultural factors database.
        transport_legs_raw: Raw transport_legs for country extraction.
        country_aliases: Country alias dictionary.

    Returns:
        Tuple of (water footprint in m3 world-eq, list of notes).
    """
    wf_proc = 0.0
    notes = []

    if len(materials) != len(weights_kg):
        notes.append("Material/weight count mismatch for processing")
        return 0.0, notes

    # Build per-(material, step) location lookup
    step_locs = extract_step_locations(
        transport_legs_raw, country_aliases
    )
    # Material origins as fallback when step location is missing
    origins = extract_material_origins(
        transport_legs_raw, materials, country_aliases
    )

    for material, weight in zip(materials, weights_kg):
        resolved = resolve_material_name(material)

        for step in processing_steps:
            # Per-step factory location, fall back to material origin
            loc = step_locs.get((material, step))
            if loc:
                country = loc.get('country', '')
            else:
                country = origins.get(
                    material, {}
                ).get('country', '')
            aware_cf = (
                aware_nonagri_db.get_factor(country)
                if country else aware_nonagri_db.fallback
            )

            # Tier 1: material-process combination
            wu = water_db.get_combination_water_factor(
                resolved, step
            )
            if wu is not None:
                wf_proc += weight * wu * aware_cf
                continue

            # Tier 2: step-level factor
            wu = water_db.get_step_water_factor(step)
            if wu is not None:
                wf_proc += weight * wu * aware_cf
                continue

            # Tier 3: default (0.005 m3/kg, conservative)
            default_wu = 0.005
            wf_proc += weight * default_wu * aware_cf

    return wf_proc, notes


def calculate_packaging_water(
    categories: List[str],
    masses_kg: List[float],
    packaging_db: WaterPackagingDatabase,
) -> Tuple[float, List[str]]:
    """Calculate packaging water footprint (no AWARE weighting).

    WF_pack = sum(mass_j * WU_packaging_j)

    Packaging water footprint is not weighted by AWARE factors
    because packaging production location is not tracked.

    Args:
        categories: List of packaging category names.
        masses_kg: List of packaging masses in kg.
        packaging_db: Packaging water consumption database.

    Returns:
        Tuple of (water footprint in m3 world-eq, list of notes).
    """
    wf_pack = 0.0
    notes = []

    if len(categories) != len(masses_kg):
        notes.append("Packaging category/mass count mismatch")
        return 0.0, notes

    for category, mass in zip(categories, masses_kg):
        wu = packaging_db.get_packaging_water_factor(category)

        if wu is None:
            # Default packaging water consumption
            wu = 0.001
            notes.append(
                f"Unknown packaging category '{category}', "
                f"used default WU"
            )

        wf_pack += mass * wu

    return wf_pack, notes


def calculate_total_water(
    wf_raw: float,
    wf_processing: float,
    wf_packaging: float,
) -> float:
    """Calculate total water footprint (no adjustments).

    WF_total = WF_raw + WF_processing + WF_packaging

    No 1.02 adjustment factor (unlike Layer 6).
    No transport component (WF_transport = 0).

    Args:
        wf_raw: Raw materials water footprint (m3 world-eq).
        wf_processing: Processing water footprint (m3 world-eq).
        wf_packaging: Packaging water footprint (m3 world-eq).

    Returns:
        Total water footprint in m3 world-eq.
    """
    return wf_raw + wf_processing + wf_packaging
