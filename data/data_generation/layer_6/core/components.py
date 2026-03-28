"""Component carbon footprint calculators for Layer 6.

Contains the individual calculation methods for each carbon footprint
component: raw materials, transport, processing, and packaging. These
are used by CarbonFootprintCalculator as a mixin-style delegation.

Primary functions:
    calculate_raw_materials -- CF from material emission factors.
    calculate_transport_from_actuals -- CF from actual per-mode distances.
    calculate_transport_logit -- CF via multinomial logit model (legacy).
    calculate_processing -- CF from material-process combinations.
    calculate_packaging -- CF from packaging category EFs.

Dependencies:
    databases module for MaterialDatabase, ProcessingDatabase.
    material_aliases for name resolution.
    transport_model for mode selection.
"""

from typing import Dict, List, Optional, Tuple

from data.data_generation.layer_6.core.databases import (
    MaterialDatabase, ProcessingDatabase
)
from data.data_generation.layer_6.core.material_aliases import (
    resolve_material_name
)
from data.data_generation.layer_6.core.transport_model import (
    TransportModeModel
)
from data.data_generation.layer_6.core._family_data import (
    MATERIAL_FAMILY_MAP,
    FAMILY_APPLICABLE_EXISTING,
)


def calculate_raw_materials(
    materials: List[str],
    weights_kg: List[float],
    material_db: MaterialDatabase,
    stats: Dict[str, int]
) -> Tuple[float, List[str]]:
    """Calculate raw materials carbon footprint.

    CF_raw = sum(w_i * EF_i)

    Args:
        materials: List of material names.
        weights_kg: List of material weights in kg.
        material_db: Material emission factor database.
        stats: Mutable dict with 'matches' and 'misses' counters.

    Returns:
        Tuple of (footprint in kgCO2e, list of notes/warnings).
    """
    cf_raw = 0.0
    notes = []

    if len(materials) != len(weights_kg):
        notes.append("Material/weight count mismatch")
        return 0.0, notes

    for material, weight in zip(materials, weights_kg):
        resolved = resolve_material_name(material)
        ef = material_db.get_emission_factor(resolved)

        if ef is not None:
            cf_raw += weight * ef
            stats['matches'] += 1
        else:
            default_ef = 5.0
            cf_raw += weight * default_ef
            notes.append(
                f"Unknown material '{material}' "
                f"(resolved: '{resolved}'), used default EF"
            )
            stats['misses'] += 1

    return cf_raw, notes


def calculate_transport_logit(
    weight_kg: float,
    distance_km: float,
    transport_model: TransportModeModel
) -> Tuple[float, Dict[str, float], float]:
    """Calculate transport CF via multinomial logit model (legacy).

    CF_transport = (W/1000) * D * (EF_weighted/1000)

    Args:
        weight_kg: Total product weight in kg.
        distance_km: Transport distance in km.
        transport_model: Transport mode selection model.

    Returns:
        Tuple of (footprint, mode_probabilities, weighted_ef).
    """
    result = transport_model.calculate_transport_footprint(
        weight_kg, distance_km
    )
    return (
        result['footprint_kg_co2e'],
        result['mode_probabilities'],
        result['weighted_ef_g_co2e_tkm']
    )


def calculate_transport_from_actuals(
    weight_kg: float,
    mode_distances_km: Dict[str, float],
    emission_factors: Dict[str, float]
) -> Tuple[float, Dict[str, float], Dict[str, float], float]:
    """Calculate transport CF from actual per-mode distances.

    Args:
        weight_kg: Total product weight in kg.
        mode_distances_km: Dict mode -> distance km (road, sea, etc.).
        emission_factors: Dict mode -> EF in g CO2e/tkm.

    Returns:
        (footprint_kg_co2e, mode_distances_km, mode_fractions, effective_ef)
    """
    weight_tonnes = weight_kg / 1000.0
    footprint = 0.0

    for mode, distance in mode_distances_km.items():
        if distance > 0:
            ef = emission_factors.get(mode, 0.0)
            footprint += weight_tonnes * distance * (ef / 1000.0)

    total_km = sum(mode_distances_km.values())

    if total_km > 0:
        mode_fractions = {
            mode: dist / total_km
            for mode, dist in mode_distances_km.items()
        }
        effective_ef = sum(
            mode_fractions[mode] * emission_factors.get(mode, 0.0)
            for mode in mode_fractions
        )
    else:
        mode_fractions = {mode: 0.0 for mode in mode_distances_km}
        effective_ef = 0.0

    return footprint, mode_distances_km, mode_fractions, effective_ef


def get_material_family(name: str) -> Optional[str]:
    """Get the non-textile family for a material, if any.

    Args:
        name: Canonical (resolved) material name.

    Returns:
        Family identifier string, or None for textile materials.
    """
    return MATERIAL_FAMILY_MAP.get(name)


def is_step_applicable(family: str, step: str) -> bool:
    """Check if a textile processing step applies to a family.

    Args:
        family: Material family identifier (e.g., 'metal').
        step: Processing step name (e.g., 'spinning').

    Returns:
        True if the step is applicable to the family.
    """
    applicable = FAMILY_APPLICABLE_EXISTING.get(family, set())
    return step in applicable


def calculate_processing(
    materials: List[str],
    weights_kg: List[float],
    processing_steps: List[str],
    material_db: MaterialDatabase,
    processing_db: ProcessingDatabase,
    step_ef_lookup: Optional[Dict[str, float]] = None
) -> Tuple[float, List[str]]:
    """Calculate processing carbon footprint with family awareness.

    Three-tier lookup per material-step pair:
    1. Direct combination lookup in processing_db (covers all
       textile combos and new non-textile combos).
    2. If no combo found and material is non-textile, check
       family applicability. Inapplicable steps contribute zero.
    3. If step IS applicable but no combo row, use step-level EF
       from step_ef_lookup as safety net.
    The 1.0 default is kept only for textile materials that somehow
    have a missing combination.

    Args:
        materials: List of material names.
        weights_kg: List of material weights in kg.
        processing_steps: List of processing step names.
        material_db: Material emission factor database.
        processing_db: Processing combination database.
        step_ef_lookup: Optional step_name -> EF mapping for
            tier-3 fallback.

    Returns:
        Tuple of (footprint in kgCO2e, list of notes).
    """
    cf_processing = 0.0
    notes = []

    if len(materials) != len(weights_kg):
        notes.append(
            "Material/weight count mismatch for processing"
        )
        return 0.0, notes

    if step_ef_lookup is None:
        step_ef_lookup = {}

    for material, weight in zip(materials, weights_kg):
        resolved = resolve_material_name(material)
        raw_ef = material_db.get_emission_factor(resolved)
        if raw_ef is None:
            raw_ef = 5.0

        family = get_material_family(resolved)

        for step in processing_steps:
            # Tier 1: direct combination lookup
            combined_ef = processing_db.get_combined_ef(
                resolved, step
            )
            if combined_ef is not None:
                contribution = combined_ef - raw_ef
                if contribution > 0:
                    cf_processing += weight * contribution
                continue

            # Tier 2+3: non-textile family handling
            if family is not None:
                if not is_step_applicable(family, step):
                    # Inapplicable textile step for this family
                    continue
                # Tier 3: applicable step, no combo row
                s_ef = step_ef_lookup.get(step, 0.0)
                if s_ef > 0:
                    cf_processing += weight * s_ef
                continue

            # Textile fallback: 1.0 kgCO2e/kg default
            default_proc_ef = 1.0
            cf_processing += weight * default_proc_ef

    return cf_processing, notes


def calculate_packaging(
    categories: List[str],
    masses_kg: List[float],
    packaging_ef: Dict[str, float]
) -> Tuple[float, List[str]]:
    """Calculate packaging carbon footprint.

    CF_packaging = sum(m_j * EF_j)

    Args:
        categories: List of packaging category names.
        masses_kg: List of packaging masses in kg.
        packaging_ef: Category-to-EF mapping.

    Returns:
        Tuple of (footprint in kgCO2e, list of notes).
    """
    cf_packaging = 0.0
    notes = []

    if len(categories) != len(masses_kg):
        notes.append("Packaging category/mass count mismatch")
        return 0.0, notes

    for category, mass in zip(categories, masses_kg):
        ef = packaging_ef.get(category)

        if ef is None:
            for cat_name, cat_ef in packaging_ef.items():
                if cat_name.lower() == category.lower():
                    ef = cat_ef
                    break

        if ef is None:
            ef = packaging_ef.get('Other/Unspecified', 2.5)
            notes.append(
                f"Unknown packaging category '{category}'"
            )

        cf_packaging += mass * ef

    return cf_packaging, notes
