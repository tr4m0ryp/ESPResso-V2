"""Input processing logic for Layer 6 orchestrator.

Handles reading the Layer 4 Parquet input, extracting detailed
material names from step_material_mapping, running the calculator
on each record, and assembling the output DataFrame.

Primary functions:
    process_input_file -- Read, calculate, and assemble output.
    extract_materials_from_mapping -- Get material names from
        the step_material_mapping JSON column.

Dependencies:
    pandas for Parquet I/O.
    calculator module for footprint computation.
    databases module for CalculationResult.
"""

import json
import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from data.data_generation.layer_6.config.config import Layer6Config
from data.data_generation.layer_6.core.calculator import (
    CarbonFootprintCalculator,
)
from data.data_generation.layer_6.core.databases import (
    CalculationResult
)
from data.data_generation.layer_6.core.material_aliases import (
    resolve_material_name
)

logger = logging.getLogger(__name__)


def extract_materials_from_mapping(row: pd.Series) -> List[str]:
    """Extract detailed material names from step_material_mapping.

    The 'materials' column in Layer 4 contains simplified names
    (e.g., 'Mixed textile'). The actual detailed material names
    are stored as keys in the step_material_mapping JSON dict.

    Args:
        row: A single row from the input DataFrame.

    Returns:
        List of detailed material names.
    """
    smm_raw = row.get('step_material_mapping', '{}')
    try:
        if isinstance(smm_raw, dict):
            return list(smm_raw.keys())
        smm = json.loads(str(smm_raw))
        if isinstance(smm, dict):
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


def _normalize_smm(smm_raw: Any) -> str:
    """Normalize step_material_mapping keys to canonical names.

    Args:
        smm_raw: Raw step_material_mapping (JSON string or dict).

    Returns:
        JSON string with canonical material name keys.
    """
    try:
        if isinstance(smm_raw, dict):
            smm = smm_raw
        else:
            smm = json.loads(str(smm_raw))
        if not isinstance(smm, dict):
            return str(smm_raw)
        normalized = {
            resolve_material_name(k): v for k, v in smm.items()
        }
        return json.dumps(normalized)
    except (json.JSONDecodeError, TypeError):
        return str(smm_raw)


def add_cf_fields(
    row: Dict[str, Any],
    result: CalculationResult
) -> Dict[str, Any]:
    """Add calculated CF fields to an output row dict.

    Args:
        row: The output row dictionary.
        result: Calculation result for this record.

    Returns:
        Row dict with CF fields added.
    """
    row['cf_raw_materials_kg_co2e'] = round(
        result.cf_raw_materials_kg_co2e, 6
    )
    row['cf_transport_kg_co2e'] = round(
        result.cf_transport_kg_co2e, 6
    )
    row['cf_processing_kg_co2e'] = round(
        result.cf_processing_kg_co2e, 6
    )
    row['cf_packaging_kg_co2e'] = round(
        result.cf_packaging_kg_co2e, 6
    )
    row['cf_modelled_kg_co2e'] = round(
        result.cf_modelled_kg_co2e, 6
    )
    row['cf_adjustment_kg_co2e'] = round(
        result.cf_adjustment_kg_co2e, 6
    )
    row['cf_total_kg_co2e'] = round(
        result.cf_total_kg_co2e, 6
    )
    row['transport_mode_probabilities'] = json.dumps(
        {k: round(v, 4)
         for k, v in result.transport_mode_probabilities.items()}
    )
    row['weighted_ef_g_co2e_tkm'] = round(
        result.weighted_ef_g_co2e_tkm, 2
    )
    row['calculation_timestamp'] = datetime.now().isoformat()
    row['calculation_version'] = 'v2.0'
    return row


def process_input_file(
    config: Layer6Config,
    calculator: CarbonFootprintCalculator,
    on_result: Callable[[CalculationResult], None],
    on_validation_issue: Callable[[], None]
) -> Optional[pd.DataFrame]:
    """Process the input Parquet file and calculate footprints.

    Args:
        config: Layer 6 configuration.
        calculator: Initialized calculator with loaded databases.
        on_result: Callback for each result (for statistics).
        on_validation_issue: Callback when validation issues found.

    Returns:
        DataFrame with all input columns plus CF columns,
        or None on failure.
    """
    try:
        input_df = pd.read_parquet(config.input_path)
        logger.info(
            "Read %d records from %s",
            len(input_df), config.input_path
        )

        output_rows = []
        record_count = 0

        for _, row in input_df.iterrows():
            raw_materials = extract_materials_from_mapping(row)

            # Align material list with weights to prevent
            # the length mismatch that causes zero CF values.
            weights_raw = row.get('material_weights_kg', '[]')
            try:
                if isinstance(weights_raw, list):
                    _weights = weights_raw
                else:
                    import ast
                    try:
                        _weights = json.loads(str(weights_raw))
                    except json.JSONDecodeError:
                        _weights = ast.literal_eval(
                            str(weights_raw)
                        )
            except (ValueError, SyntaxError, TypeError):
                _weights = []
            n = min(len(raw_materials), len(_weights))
            if n < len(raw_materials):
                logger.debug(
                    "Truncating %d materials to %d (weight count)",
                    len(raw_materials), n
                )
            raw_materials = raw_materials[:n]

            canonical = [
                resolve_material_name(m) for m in raw_materials
            ]

            record = row.to_dict()
            record['materials'] = json.dumps(canonical)

            result = calculator.calculate_record(record)

            if config.enable_validation:
                issues = calculator.validate_result(result)
                if issues:
                    on_validation_issue()
                    if config.verbose:
                        logger.warning(
                            "Record %d: %s",
                            record_count, ', '.join(issues)
                        )

            out = dict(row)
            out['materials'] = json.dumps(canonical)
            out['step_material_mapping'] = _normalize_smm(
                row.get('step_material_mapping', '{}')
            )
            out = add_cf_fields(out, result)
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
        logger.error("Error processing input file: %s", e)
        import traceback
        logger.error(traceback.format_exc())
        return None
