"""Component carbon footprint calculators for Layer 6.

Individual calculation methods for raw materials, transport, processing,
and packaging. Used by CarbonFootprintCalculator as delegation targets.
"""

import json
import logging
from typing import Dict, List, Optional, Set, Tuple

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

logger = logging.getLogger(__name__)


def calculate_raw_materials(
    materials: List[str],
    weights_kg: List[float],
    material_db: MaterialDatabase,
    stats: Dict[str, int]
) -> Tuple[float, List[str]]:
    """Calculate raw materials CF = sum(w_i * EF_i)."""
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
    """Calculate transport CF via multinomial logit model (legacy)."""
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
    """Calculate transport CF from actual per-mode distances."""
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
    """Get the non-textile family for a material, or None."""
    return MATERIAL_FAMILY_MAP.get(name)


def is_step_applicable(family: str, step: str) -> bool:
    """Check if a textile processing step applies to a family."""
    applicable = FAMILY_APPLICABLE_EXISTING.get(family, set())
    return step in applicable


def extract_material_step_routing(
    transport_legs_json: str,
    preprocessing_steps: List[str]
) -> Dict[str, Set[str]]:
    """Extract per-material processing steps from transport legs.

    Parses leg objects (material, from_step, to_step), collects step
    endpoints per material, intersects with preprocessing_steps to
    filter out logistics nodes. Returns empty dict on malformed input.
    """
    try:
        legs = json.loads(transport_legs_json)
    except (json.JSONDecodeError, TypeError):
        logger.warning("Malformed transport_legs JSON, skipping routing")
        return {}

    if not isinstance(legs, list):
        return {}

    valid_steps = {s.lower().strip() for s in preprocessing_steps}
    routing: Dict[str, Set[str]] = {}

    for leg in legs:
        if not isinstance(leg, dict):
            continue
        mat = leg.get("material", "")
        if not mat:
            continue
        steps = routing.setdefault(mat, set())
        for field in ("from_step", "to_step"):
            val = leg.get(field, "")
            if val:
                normed = val.lower().strip()
                if normed in valid_steps:
                    steps.add(normed)

    return routing


def calculate_processing(
    materials: List[str],
    weights_kg: List[float],
    processing_steps: List[str],
    material_db: MaterialDatabase,
    processing_db: ProcessingDatabase,
    step_ef_lookup: Optional[Dict[str, float]] = None,
    material_step_routing: Optional[Dict[str, Set[str]]] = None
) -> Tuple[float, List[str]]:
    """Calculate processing CF with family-aware 3-tier EF lookup.

    Tiers: 1) direct combo in processing_db, 2) family applicability
    filter for non-textiles, 3) step_ef_lookup fallback. Textile
    materials without a combo get 1.0 kgCO2e/kg default.

    When material_step_routing is provided, each material iterates
    only its own steps instead of all preprocessing_steps. Materials
    missing from the routing dict fall back to all steps.
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

    use_routing = bool(material_step_routing)

    for material, weight in zip(materials, weights_kg):
        resolved = resolve_material_name(material)
        raw_ef = material_db.get_emission_factor(resolved)
        if raw_ef is None:
            raw_ef = 5.0

        family = get_material_family(resolved)

        if use_routing and material in material_step_routing:
            steps_iter = material_step_routing[material]
        else:
            steps_iter = processing_steps

        for step in steps_iter:
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
    """Calculate packaging CF = sum(m_j * EF_j)."""
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
