"""Post-calculation enrichment for Layer 6 training dataset.

Formalizes three post-processing steps that were previously applied
ad-hoc after the carbon footprint calculation:

1. Replace broad origin_region categories from Layer 4 with
   country-level values from Layer 3 via transport_scenario_id join.
2. Derive transport_strategy from the transport_scenario_id suffix.
3. Rename step_material_mapping to detailed_preprocessing_steps.

Primary functions:
    enrich_origin_region -- Merge country-level origins from Layer 3.
    derive_transport_strategy -- Extract strategy from scenario ID.
    rename_step_mapping -- Rename column for clarity.
    run_enrichment -- Run all three steps in sequence.

Dependencies:
    pandas for DataFrame operations.
"""

import logging
import re
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Pattern to extract transport strategy from scenario IDs
# Format: pp-NNN_ts-{strategy}
_STRATEGY_RE = re.compile(r'_ts-(\w+)$')


def enrich_origin_region(
    df: pd.DataFrame,
    layer3_path: str
) -> pd.DataFrame:
    """Replace broad origin_region with country-level values.

    Layer 4 overwrites origin_region with broad categories
    (Asia-Pacific, Continental, etc.). This function merges back
    the country-level origin_region from Layer 3 using
    transport_scenario_id as the join key.

    Args:
        df: Training DataFrame with broad origin_region values.
        layer3_path: Path to Layer 3 Parquet file.

    Returns:
        DataFrame with country-level origin_region.
    """
    if not Path(layer3_path).exists():
        logger.warning(
            "Layer 3 file not found at %s; "
            "skipping origin_region enrichment",
            layer3_path
        )
        return df

    layer3 = pd.read_parquet(
        layer3_path,
        columns=['transport_scenario_id', 'origin_region']
    )

    # Deduplicate: one origin per scenario ID
    layer3_dedup = layer3.drop_duplicates(
        subset='transport_scenario_id'
    ).rename(columns={'origin_region': 'origin_region_l3'})

    original_len = len(df)
    merged = df.merge(
        layer3_dedup,
        on='transport_scenario_id',
        how='left'
    )

    # Use Layer 3 origin where available, keep Layer 4 as fallback
    merged['origin_region'] = merged['origin_region_l3'].fillna(
        merged['origin_region']
    )
    merged.drop(columns=['origin_region_l3'], inplace=True)

    enriched_count = merged['origin_region'].notna().sum()
    unique_origins = merged['origin_region'].nunique()
    logger.info(
        "Origin region enrichment: %d/%d rows enriched, "
        "%d unique origins",
        enriched_count, original_len, unique_origins
    )

    if len(merged) != original_len:
        logger.warning(
            "Row count changed during merge: %d -> %d. "
            "Possible duplicate transport_scenario_id in Layer 3.",
            original_len, len(merged)
        )

    return merged


def derive_transport_strategy(df: pd.DataFrame) -> pd.DataFrame:
    """Extract transport strategy from transport_scenario_id.

    The transport_scenario_id has format pp-NNN_ts-{strategy} where
    strategy is one of: cost, speed, eco, risk, regional.

    Args:
        df: DataFrame with transport_scenario_id column.

    Returns:
        DataFrame with added transport_strategy column.
    """
    def _extract_strategy(scenario_id: Optional[str]) -> str:
        if not scenario_id or not isinstance(scenario_id, str):
            return 'unknown'
        match = _STRATEGY_RE.search(scenario_id)
        return match.group(1) if match else 'unknown'

    df['transport_strategy'] = df['transport_scenario_id'].apply(
        _extract_strategy
    )

    strategies = df['transport_strategy'].value_counts()
    logger.info("Transport strategies derived: %s", dict(strategies))

    return df


def rename_step_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Rename step_material_mapping to detailed_preprocessing_steps.

    This rename clarifies the column's purpose in the training
    dataset: it contains the per-material processing step breakdown.

    Args:
        df: DataFrame with step_material_mapping column.

    Returns:
        DataFrame with renamed column.
    """
    if 'step_material_mapping' in df.columns:
        df = df.rename(columns={
            'step_material_mapping': 'detailed_preprocessing_steps'
        })
        logger.info(
            "Renamed step_material_mapping -> "
            "detailed_preprocessing_steps"
        )
    return df


def run_enrichment(
    df: pd.DataFrame,
    layer3_path: str
) -> pd.DataFrame:
    """Run all enrichment steps in sequence.

    Args:
        df: Raw training DataFrame from calculation.
        layer3_path: Path to Layer 3 Parquet file.

    Returns:
        Enriched DataFrame.
    """
    logger.info("Starting post-calculation enrichment...")

    df = enrich_origin_region(df, layer3_path)
    df = derive_transport_strategy(df)
    df = rename_step_mapping(df)

    logger.info("Enrichment complete.")
    return df
