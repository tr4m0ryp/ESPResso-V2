#!/usr/bin/env python3
"""Generate non-textile processing steps and combinations.

One-time script that:
1. Reads base_materials.parquet for raw EFs of the 30 non-textile
   materials.
2. Reads existing processing_steps.parquet (32 rows).
3. Appends new non-textile processing steps from FAMILY_PROCESSING_STEPS.
4. For each of the 30 materials, generates combination rows for all
   applicable steps (existing cross-applicable + new family-specific).
5. Appends new rows to material_processing_combinations.parquet.

Usage:
    python -m data.data_generation.scripts.generate_nontextile_processing
"""

import logging
import sys
import uuid
from pathlib import Path

import pandas as pd

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from data.data_generation.layer_6.core._family_data import (
    MATERIAL_FAMILY_MAP,
    FAMILY_PROCESSING_STEPS,
    FAMILY_APPLICABLE_EXISTING,
)

logger = logging.getLogger(__name__)

STEPS_PATH = (
    'data/datasets/pre-model/final/processing_steps.parquet'
)
COMBOS_PATH = (
    'data/datasets/pre-model/final/'
    'material_processing_combinations.parquet'
)
MATERIALS_PATH = (
    'data/datasets/pre-model/final/base_materials.parquet'
)

# Category mapping for new family steps
FAMILY_STEP_CATEGORIES = {
    'metal': 'metal_processing',
    'leather': 'leather_processing',
    'foam': 'foam_processing',
    'polymer': 'polymer_processing',
    'rubber': 'rubber_processing',
    'feathers': 'feather_processing',
    'cork': 'cork_processing',
}


def generate_step_ref_id(step_name: str) -> str:
    """Generate a deterministic UUID-like ref_id for a new step.

    Args:
        step_name: Name of the processing step.

    Returns:
        UUID string derived from the step name.
    """
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, step_name))


def append_processing_steps(
    steps_df: pd.DataFrame,
) -> pd.DataFrame:
    """Append non-textile processing steps to the steps DataFrame.

    Args:
        steps_df: Existing processing steps DataFrame.

    Returns:
        Updated DataFrame with new steps appended.
    """
    existing_names = set(steps_df['process_name'].values)
    new_rows = []

    for family, steps in FAMILY_PROCESSING_STEPS.items():
        category = FAMILY_STEP_CATEGORIES[family]
        for step_name, ef, source in steps:
            if step_name in existing_names:
                logger.warning(
                    "Step '%s' already exists, skipping",
                    step_name
                )
                continue
            new_rows.append({
                'process_name': step_name,
                'ref_id': generate_step_ref_id(step_name),
                'category': category,
                'step_type': 'processing',
                'carbon_footprint_kgCO2e_per_kg': ef,
                'applies_to': family,
                'description': f'{step_name} ({source})',
            })
            existing_names.add(step_name)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        steps_df = pd.concat(
            [steps_df, new_df], ignore_index=True
        )
        logger.info("Appended %d new processing steps", len(new_rows))
    else:
        logger.info("No new processing steps to append")

    return steps_df


def build_step_ef_lookup(
    steps_df: pd.DataFrame,
) -> dict:
    """Build step_name -> EF lookup from steps DataFrame.

    Args:
        steps_df: Processing steps DataFrame.

    Returns:
        Dictionary mapping step names to emission factors.
    """
    lookup = {}
    for _, row in steps_df.iterrows():
        lookup[row['process_name']] = float(
            row['carbon_footprint_kgCO2e_per_kg']
        )
    return lookup


def generate_combinations(
    materials_df: pd.DataFrame,
    steps_df: pd.DataFrame,
    existing_combos_df: pd.DataFrame,
) -> pd.DataFrame:
    """Generate material-process combination rows for non-textile materials.

    For each non-textile material, creates combination rows for:
    - New family-specific processing steps.
    - Existing textile steps that are applicable to the family.

    Args:
        materials_df: Base materials DataFrame.
        steps_df: Updated processing steps DataFrame.
        existing_combos_df: Existing combinations DataFrame.

    Returns:
        Updated combinations DataFrame with new rows appended.
    """
    step_ef = build_step_ef_lookup(steps_df)
    existing_keys = set(
        zip(
            existing_combos_df['material_name'],
            existing_combos_df['process_name'],
        )
    )

    # Build material info lookup
    mat_info = {}
    for _, row in materials_df.iterrows():
        name = row['material_name']
        mat_info[name] = {
            'ref_id': row['ref_id'],
            'category': row['category'],
            'source': row['source'],
            'raw_ef': float(
                row['carbon_footprint_kgCO2e_per_kg']
            ),
        }

    # Build step ref_id lookup
    step_refs = {}
    step_cats = {}
    for _, row in steps_df.iterrows():
        step_refs[row['process_name']] = row['ref_id']
        step_cats[row['process_name']] = row['category']

    new_rows = []
    for mat_name, family in MATERIAL_FAMILY_MAP.items():
        info = mat_info.get(mat_name)
        if info is None:
            logger.warning(
                "Material '%s' not found in base_materials",
                mat_name
            )
            continue

        raw_ef = info['raw_ef']

        # Collect all applicable steps for this family
        applicable_steps = set()

        # Family-specific new steps
        for step_name, _, _ in FAMILY_PROCESSING_STEPS.get(
            family, []
        ):
            applicable_steps.add(step_name)

        # Cross-applicable existing steps
        for step_name in FAMILY_APPLICABLE_EXISTING.get(
            family, set()
        ):
            applicable_steps.add(step_name)

        for step_name in sorted(applicable_steps):
            key = (mat_name, step_name)
            if key in existing_keys:
                continue

            s_ef = step_ef.get(step_name, 0.0)
            combined_ef = raw_ef + s_ef

            new_rows.append({
                'material_name': mat_name,
                'material_ref_id': info['ref_id'],
                'material_category': info['category'],
                'material_source': info['source'],
                'process_name': step_name,
                'process_ref_id': step_refs.get(step_name, ''),
                'process_category': step_cats.get(
                    step_name, 'unknown'
                ),
                'combined_cf_kgCO2e_per_kg': round(combined_ef, 4),
            })
            existing_keys.add(key)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        existing_combos_df = pd.concat(
            [existing_combos_df, new_df], ignore_index=True
        )
        logger.info(
            "Appended %d new material-process combinations",
            len(new_rows)
        )
    else:
        logger.info("No new combinations to append")

    return existing_combos_df


def main() -> int:
    """Run the non-textile processing generation pipeline.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )

    logger.info("Loading reference data...")
    materials_df = pd.read_parquet(MATERIALS_PATH)
    steps_df = pd.read_parquet(STEPS_PATH)
    combos_df = pd.read_parquet(COMBOS_PATH)

    logger.info(
        "Before: %d steps, %d combinations",
        len(steps_df), len(combos_df)
    )

    # Append new processing steps
    steps_df = append_processing_steps(steps_df)

    # Generate new material-process combinations
    combos_df = generate_combinations(
        materials_df, steps_df, combos_df
    )

    logger.info(
        "After: %d steps, %d combinations",
        len(steps_df), len(combos_df)
    )

    # Write updated Parquet files
    steps_df.to_parquet(
        STEPS_PATH, index=False, compression='gzip'
    )
    logger.info("Wrote %s", STEPS_PATH)

    combos_df.to_parquet(
        COMBOS_PATH, index=False, compression='gzip'
    )
    logger.info("Wrote %s", COMBOS_PATH)

    # Summary
    new_materials = set(MATERIAL_FAMILY_MAP.keys())
    new_combos = combos_df[
        combos_df['material_name'].isin(new_materials)
    ]
    logger.info(
        "Non-textile materials now have %d combinations "
        "across %d unique materials",
        len(new_combos),
        new_combos['material_name'].nunique()
    )

    return 0


if __name__ == '__main__':
    sys.exit(main())
