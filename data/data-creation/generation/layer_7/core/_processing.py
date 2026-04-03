"""Input processing logic for Layer 7 orchestrator.

Handles reading the joined input DataFrame (Layer 5 + Layer 4),
extracting materials from step_material_mapping, running the water
footprint calculator on each record, and assembling the output.

Primary functions:
    process_records -- Calculate water footprints for all records.
    extract_materials_from_mapping -- Get material names from
        the step_material_mapping JSON column.

Dependencies:
    pandas for DataFrame operations.
    calculator module for water footprint computation.
    databases module for WaterCalculationResult.
    Layer 6 material_aliases for name resolution.
"""

import ast
import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from data.data_generation.layer_7.core.calculator import (
    WaterFootprintCalculator,
)
from data.data_generation.layer_7.core.databases import (
    WaterCalculationResult,
)
from data.data_generation.layer_6.core.material_aliases import (
    resolve_material_name,
)

logger = logging.getLogger(__name__)


def extract_materials_from_mapping(row: pd.Series) -> List[str]:
    """Extract detailed material names from step_material_mapping.

    The 'materials' column may contain simplified names. The actual
    detailed names are stored as keys in step_material_mapping.

    Args:
        row: A single row from the input DataFrame.

    Returns:
        List of detailed material names.
    """
    smm_raw = row.get('step_material_mapping', '{}')
    try:
        if isinstance(smm_raw, dict) and smm_raw:
            return list(smm_raw.keys())
        smm = json.loads(str(smm_raw))
        if isinstance(smm, dict) and smm:
            return list(smm.keys())
    except (json.JSONDecodeError, TypeError):
        pass

    # Fallback to the materials column
    mats_raw = row.get('materials', '[]')
    try:
        if isinstance(mats_raw, list):
            return mats_raw
        return json.loads(str(mats_raw))
    except (json.JSONDecodeError, TypeError):
        return []


def _add_wf_fields(
    row: Dict[str, Any],
    result: WaterCalculationResult,
) -> Dict[str, Any]:
    """Add water footprint fields to an output row.

    Args:
        row: The output row dictionary.
        result: Water footprint calculation result.

    Returns:
        Row dict with WF fields added.
    """
    row['wf_raw_materials_m3_world_eq'] = round(
        result.wf_raw_materials_m3_world_eq, 6
    )
    row['wf_processing_m3_world_eq'] = round(
        result.wf_processing_m3_world_eq, 6
    )
    row['wf_packaging_m3_world_eq'] = round(
        result.wf_packaging_m3_world_eq, 6
    )
    row['wf_total_m3_world_eq'] = round(
        result.wf_total_m3_world_eq, 6
    )
    row['calculation_timestamp'] = datetime.now().isoformat()
    row['calculation_version'] = 'v1.0'
    return row


def process_records(
    input_df: pd.DataFrame,
    calculator: WaterFootprintCalculator,
    on_result: Callable[[WaterCalculationResult], None],
    on_validation_issue: Callable[[], None],
    enable_validation: bool = True,
    verbose: bool = False,
) -> Optional[pd.DataFrame]:
    """Process all input records and calculate water footprints.

    Args:
        input_df: Joined DataFrame (Layer 5 + Layer 4 transport_legs).
        calculator: Initialized calculator with loaded databases.
        on_result: Callback for each result (for statistics).
        on_validation_issue: Callback when validation issues found.
        enable_validation: Whether to run validation checks.
        verbose: Whether to log detailed warnings.

    Returns:
        DataFrame with water footprint columns, or None on failure.
    """
    try:
        output_rows = []
        record_count = 0

        for _, row in input_df.iterrows():
            raw_materials = extract_materials_from_mapping(row)

            # Align materials with weights
            weights_raw = row.get('material_weights_kg', '[]')
            try:
                if isinstance(weights_raw, list):
                    _weights = weights_raw
                else:
                    try:
                        _weights = json.loads(str(weights_raw))
                    except json.JSONDecodeError:
                        _weights = ast.literal_eval(
                            str(weights_raw)
                        )
            except (ValueError, SyntaxError, TypeError):
                _weights = []

            n = min(len(raw_materials), len(_weights))
            raw_materials = raw_materials[:n]

            canonical = [
                resolve_material_name(m) for m in raw_materials
            ]

            record = row.to_dict()
            record['materials'] = json.dumps(canonical)

            result = calculator.calculate_record(record)

            if enable_validation:
                issues = calculator.validate_result(result)
                if issues:
                    on_validation_issue()
                    if verbose:
                        logger.warning(
                            "Record %d: %s",
                            record_count, ', '.join(issues)
                        )

            out = {
                'record_id': record.get('record_id', ''),
            }
            out = _add_wf_fields(out, result)
            output_rows.append(out)

            on_result(result)
            record_count += 1

            if record_count % 10000 == 0:
                logger.info(
                    "Processed %s records...",
                    f"{record_count:,}"
                )

        logger.info(
            "Completed processing %s records",
            f"{record_count:,}"
        )
        return pd.DataFrame(output_rows)

    except Exception as e:
        logger.error("Error processing records: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        return None
