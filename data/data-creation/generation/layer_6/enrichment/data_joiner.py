"""Data joiner for Layer 6 enrichment: joins Layer 5 validated records
with Layer 4 transport_legs via preprocessing path ID.

Layer 5 has the validated record set (50,480 rows) but lacks transport_legs.
Layer 4 has transport_legs stored as JSON strings in the parquet file.
Records are joined using the pp-XXXXXX identifier extracted from record_id.

Primary functions:
    join_transport_legs -- Load and join both datasets.
    extract_pp_id -- Extract pp-XXXXXX from a record_id string.

Dependencies:
    pandas for DataFrame operations.
    re for record_id pattern matching.
"""

import logging
import re
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)

# Pattern to extract pp-XXXXXX from record_id like cl-2-3_pp-015810
_PP_ID_RE = re.compile(r'(pp-\d+)')

# Columns to pull from Layer 4 (keeps memory footprint small)
_LAYER4_COLUMNS = ['preprocessing_path_id', 'transport_legs', 'total_distance_km']

# Expected minimum row count after join (stop condition from task spec)
_EXPECTED_MIN_ROWS = 50_000


def extract_pp_id(record_id: str) -> str:
    """Extract pp-XXXXXX from record_id like cl-2-3_pp-015810.

    Args:
        record_id: Full record identifier string.

    Returns:
        The pp-XXXXXX substring if found, otherwise empty string.
    """
    if not record_id or not isinstance(record_id, str):
        return ''
    match = _PP_ID_RE.search(record_id)
    return match.group(1) if match else ''


def join_transport_legs(
    layer5_path: str,
    layer4_path: str
) -> pd.DataFrame:
    """Join Layer 5 validated data with Layer 4 transport_legs.

    Returns DataFrame with all Layer 5 columns plus transport_legs
    and total_distance_km from Layer 4.

    The join key is extracted from Layer 5 record_id using
    the pattern r'(pp-\\d+)', matched against Layer 4
    preprocessing_path_id.

    Args:
        layer5_path: Path to Layer 5 validated CSV file.
        layer4_path: Path to Layer 4 complete dataset Parquet file.

    Returns:
        Merged DataFrame (inner join) with all Layer 5 columns
        plus transport_legs and total_distance_km from Layer 4.

    Raises:
        FileNotFoundError: If either input file does not exist.
        ValueError: If the join produces significantly fewer rows
            than the expected 50,480.
    """
    _verify_files_exist(layer5_path, layer4_path)

    layer5_df = _load_layer5(layer5_path)
    layer4_df = _load_layer4(layer4_path)

    merged = _join(layer5_df, layer4_df)

    _check_unmatched(layer5_df, merged)
    _validate_row_count(merged)

    return merged


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _verify_files_exist(layer5_path: str, layer4_path: str) -> None:
    """Raise FileNotFoundError if either path does not exist."""
    for path in (layer5_path, layer4_path):
        if not Path(path).exists():
            raise FileNotFoundError(f"Required input file not found: {path}")


def _load_layer5(layer5_path: str) -> pd.DataFrame:
    """Read Layer 5 CSV and add pp_id join key column.

    Args:
        layer5_path: Path to the Layer 5 validated CSV.

    Returns:
        DataFrame with all 27 original columns plus pp_id.
    """
    logger.info("Loading Layer 5 dataset from %s", layer5_path)
    df = pd.read_csv(layer5_path)
    logger.info("Loaded %d Layer 5 records (%d columns)", len(df), len(df.columns))

    if 'record_id' not in df.columns:
        raise ValueError(
            f"Layer 5 CSV missing 'record_id' column. "
            f"Available columns: {list(df.columns)}"
        )

    df['pp_id'] = df['record_id'].apply(extract_pp_id)

    unextracted = (df['pp_id'] == '').sum()
    if unextracted > 0:
        logger.warning(
            "%d record_id values did not match pp-XXXXXX pattern",
            unextracted
        )
        sample = df.loc[df['pp_id'] == '', 'record_id'].head(5).tolist()
        logger.warning("Sample unmatched record_ids: %s", sample)

    unique_pp_ids = df['pp_id'].nunique()
    logger.info(
        "Extracted %d unique pp_ids from %d records",
        unique_pp_ids, len(df)
    )
    return df


def _load_layer4(layer4_path: str) -> pd.DataFrame:
    """Read only the needed columns from Layer 4 parquet.

    Args:
        layer4_path: Path to the Layer 4 complete dataset parquet.

    Returns:
        DataFrame with preprocessing_path_id, transport_legs,
        total_distance_km columns, deduplicated by preprocessing_path_id.
    """
    logger.info("Loading Layer 4 transport_legs from %s", layer4_path)
    df = pd.read_parquet(layer4_path, columns=_LAYER4_COLUMNS)
    logger.info("Loaded %d Layer 4 records", len(df))

    if 'preprocessing_path_id' not in df.columns:
        raise ValueError(
            f"Layer 4 parquet missing 'preprocessing_path_id' column. "
            f"Available columns: {list(df.columns)}"
        )

    # Deduplicate so the join remains 1-to-1 per pp_id
    before_dedup = len(df)
    df = df.drop_duplicates(subset='preprocessing_path_id')
    if len(df) < before_dedup:
        logger.info(
            "Deduplicated Layer 4 on preprocessing_path_id: %d -> %d rows",
            before_dedup, len(df)
        )

    return df


def _join(layer5_df: pd.DataFrame, layer4_df: pd.DataFrame) -> pd.DataFrame:
    """Inner-join Layer 5 on pp_id == Layer 4 preprocessing_path_id.

    Args:
        layer5_df: Layer 5 DataFrame with pp_id column added.
        layer4_df: Layer 4 DataFrame with preprocessing_path_id column.

    Returns:
        Merged DataFrame. pp_id helper column is dropped after join.
    """
    merged = layer5_df.merge(
        layer4_df,
        left_on='pp_id',
        right_on='preprocessing_path_id',
        how='inner'
    )

    # Drop the temporary join key and the duplicate id from Layer 4
    cols_to_drop = [c for c in ('pp_id', 'preprocessing_path_id') if c in merged.columns]
    merged.drop(columns=cols_to_drop, inplace=True)

    logger.info(
        "Join result: %d rows, %d columns",
        len(merged), len(merged.columns)
    )
    return merged


def _check_unmatched(
    layer5_df: pd.DataFrame,
    merged: pd.DataFrame
) -> None:
    """Log warnings for any Layer 5 records that did not match Layer 4.

    Args:
        layer5_df: Original Layer 5 DataFrame (with pp_id column).
        merged: Joined DataFrame after inner join.
    """
    original_count = len(layer5_df)
    matched_count = len(merged)
    unmatched_count = original_count - matched_count

    if unmatched_count > 0:
        logger.warning(
            "%d Layer 5 records did not match any Layer 4 record "
            "(%d of %d matched)",
            unmatched_count, matched_count, original_count
        )
        # Identify which pp_ids were lost
        if 'record_id' in merged.columns:
            matched_pp = merged['record_id'].apply(extract_pp_id)
        else:
            matched_pp = pd.Series(dtype=str)

        lost = layer5_df.loc[
            ~layer5_df['pp_id'].isin(matched_pp), 'pp_id'
        ].head(10).tolist()
        logger.warning("Sample unmatched pp_ids: %s", lost)
    else:
        logger.info(
            "All %d Layer 5 records matched Layer 4 records",
            matched_count
        )


def _validate_row_count(merged: pd.DataFrame) -> None:
    """Raise ValueError if row count falls significantly below expected.

    Args:
        merged: Joined DataFrame.

    Raises:
        ValueError: If row count is below _EXPECTED_MIN_ROWS.
    """
    row_count = len(merged)
    if row_count < _EXPECTED_MIN_ROWS:
        raise ValueError(
            f"Join produced only {row_count} rows, expected at least "
            f"{_EXPECTED_MIN_ROWS}. Check that Layer 4 contains the "
            f"matching preprocessing_path_id values for all Layer 5 records."
        )
    logger.info(
        "Row count validation passed: %d rows (>= %d expected minimum)",
        row_count, _EXPECTED_MIN_ROWS
    )
