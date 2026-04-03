"""Data loader for Layer 5: reads Layer 4 parquet output into CompleteProductRecords."""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import CompleteProductRecord

logger = logging.getLogger(__name__)


def _parse_list(val: Any) -> list:
    """Parse a JSON list string or return as-is if already a list."""
    if isinstance(val, list):
        return val
    if not val or (isinstance(val, float) and pd.isna(val)):
        return []
    try:
        return json.loads(str(val))
    except (json.JSONDecodeError, TypeError, ValueError):
        return []


def _extract_from_legs(legs: list, field: str) -> list:
    """Extract a field from each transport leg dict."""
    return [leg.get(field) for leg in legs if isinstance(leg, dict)]


def load_records(
    config: Layer5Config,
    max_records: Optional[int] = None,
) -> List[CompleteProductRecord]:
    """Load CompleteProductRecords from the Layer 4 parquet output."""
    path = config.complete_dataset_path
    if not Path(path).exists():
        logger.error("Dataset not found: %s", path)
        return []
    logger.info("Loading dataset from %s", path)

    df = pd.read_parquet(path)
    if max_records:
        df = df.head(max_records)

    records: List[CompleteProductRecord] = []
    for i, row in df.iterrows():
        try:
            records.append(_row_to_record(row, len(records)))
        except Exception as exc:
            logger.warning("Skipping row %d: %s", i, exc)
        if len(records) % 10_000 == 0 and len(records) > 0:
            logger.info("Loaded %d rows...", len(records))

    logger.info("Loaded %d records total", len(records))
    return records


def _row_to_record(row: Any, idx: int) -> CompleteProductRecord:
    """Parse a parquet row (Series) into a CompleteProductRecord."""
    legs = _parse_list(row.get("transport_legs", "[]"))
    packaging_masses = _parse_list(row.get("packaging_masses_kg", "[]"))
    total_pkg = sum(float(m) for m in packaging_masses if m is not None)

    # Extract transport data from legs
    transport_modes_list = []
    transport_distances = []
    for leg in legs:
        if isinstance(leg, dict):
            modes = leg.get("transport_modes", [])
            if isinstance(modes, list):
                transport_modes_list.extend(modes)
            dist = leg.get("distance_km", 0)
            transport_distances.append(float(dist) if dist else 0.0)

    # Derive supply chain type from total distance
    total_dist = float(row.get("total_distance_km", 0) or 0)
    if total_dist < 500:
        chain_type = "local"
    elif total_dist < 3000:
        chain_type = "medium_haul"
    else:
        chain_type = "long_haul"

    return CompleteProductRecord(
        category_id=str(row.get("category_id", f"cat_{idx}")),
        category_name=str(row.get("category_name", "")),
        subcategory_id=str(row.get("subcategory_id", f"subcat_{idx}")),
        subcategory_name=str(row.get("subcategory_name", "")),
        materials=_parse_list(row.get("materials", "[]")),
        material_weights_kg=_parse_list(row.get("material_weights_kg", "[]")),
        material_percentages=_parse_list(row.get("material_percentages", "[]")),
        total_weight_kg=float(row.get("total_weight_kg", 0) or 0),
        preprocessing_path_id=str(row.get("preprocessing_path_id", f"pp_{idx}")),
        preprocessing_steps=_parse_list(row.get("preprocessing_steps", "[]")),
        transport_scenario_id=f"ts_{idx}",
        total_transport_distance_km=total_dist,
        supply_chain_type=chain_type,
        transport_items=[str(leg.get("material", "")) for leg in legs if isinstance(leg, dict)],
        transport_modes=list(set(transport_modes_list)),
        transport_distances_kg=transport_distances,
        transport_emissions_kg_co2e=[],
        packaging_config_id=f"pkg_{idx}",
        packaging_items=_parse_list(row.get("packaging_categories", "[]")),
        packaging_categories=_parse_list(row.get("packaging_categories", "[]")),
        packaging_masses_kg=[float(m) for m in packaging_masses if m is not None],
        total_packaging_mass_kg=total_pkg,
    )
