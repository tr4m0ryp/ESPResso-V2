"""Clean the full dataset for ML model training.

Applies data quality fixes identified during exploratory analysis:
  - Fix category name typo ("Underewear" -> "Underwear")
  - Drop rows with unknown transport strategy
  - Drop zero-information and redundant columns
  - Cap outliers in weighted_ef_g_co2e_tkm at 99th percentile

Supports both Parquet and CSV input/output (auto-detected by extension).

Usage:
    python clean_dataset.py
    python clean_dataset.py --input path/to/input.parquet
    python clean_dataset.py --output path/to/output.parquet
"""

import argparse
import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

COLUMNS_TO_DROP = [
    "calculation_version",
    "calculation_timestamp",
    "transport_scenario_id",
    "cf_adjustment_kg_co2e",
]

CATEGORY_TYPO_MAP = {
    "Underewear": "Underwear",
}

OUTLIER_CAP_PERCENTILE = 99

OUTLIER_CAP_COLUMNS = [
    "weighted_ef_g_co2e_tkm",
]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Clean full dataset for ML model training"
    )
    parser.add_argument(
        '--input', type=str, default=None,
        help=(
            'Path to input file '
            '(default: datasets/model/full_dataset.parquet)'
        ),
    )
    parser.add_argument(
        '--output', type=str, default=None,
        help=(
            'Path to output file '
            '(default: datasets/model/cleaned_dataset.parquet)'
        ),
    )
    return parser.parse_args()


def get_project_root() -> Path:
    """Get project root directory.

    Walks up from this file until it finds CLAUDE.md,
    which marks the project root.

    Returns:
        Path to the project root directory.
    """
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / 'CLAUDE.md').exists():
            return parent
    return current.parent.parent


def fix_category_typos(df: pd.DataFrame) -> pd.DataFrame:
    """Fix known typos in the category_name column.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with corrected category names.
    """
    col = "category_name"
    for wrong, correct in CATEGORY_TYPO_MAP.items():
        mask = df[col] == wrong
        count = mask.sum()
        if count > 0:
            df.loc[mask, col] = correct
            logger.info(
                "Fixed category typo: '%s' -> '%s' (%d rows)",
                wrong, correct, count,
            )
        else:
            logger.info(
                "Category typo '%s' not found (already clean)",
                wrong,
            )
    return df


def drop_unknown_transport(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where transport_strategy is 'unknown'.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with unknown transport rows removed.
    """
    col = "transport_strategy"
    mask = df[col] == "unknown"
    count = mask.sum()
    if count > 0:
        df = df[~mask].reset_index(drop=True)
        logger.info(
            "Dropped %d rows with transport_strategy='unknown' "
            "(%.2f%% of original)",
            count, 100.0 * count / (len(df) + count),
        )
    else:
        logger.info("No 'unknown' transport_strategy rows found")
    return df


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Drop zero-information and redundant columns.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with specified columns removed.
    """
    present = [c for c in COLUMNS_TO_DROP if c in df.columns]
    missing = [c for c in COLUMNS_TO_DROP if c not in df.columns]

    if missing:
        logger.warning(
            "Columns not found (skipped): %s",
            ", ".join(missing),
        )

    if present:
        df = df.drop(columns=present)
        logger.info(
            "Dropped %d columns: %s",
            len(present), ", ".join(present),
        )
    return df


def cap_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """Cap outlier values at the specified percentile.

    Uses the 99th percentile as the cap value for each
    configured column.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with outliers capped.
    """
    for col in OUTLIER_CAP_COLUMNS:
        if col not in df.columns:
            logger.warning(
                "Outlier cap column '%s' not found, skipping",
                col,
            )
            continue

        cap_value = np.percentile(
            df[col].dropna(), OUTLIER_CAP_PERCENTILE
        )
        above_count = (df[col] > cap_value).sum()
        original_max = df[col].max()

        df[col] = df[col].clip(upper=cap_value)

        logger.info(
            "Capped %s at p%d = %.4f "
            "(was max %.4f, %d values clipped)",
            col, OUTLIER_CAP_PERCENTILE, cap_value,
            original_max, above_count,
        )
    return df


def log_summary(
    df_before: pd.DataFrame,
    df_after: pd.DataFrame,
) -> None:
    """Log a cleaning summary comparing before and after.

    Args:
        df_before: Original DataFrame.
        df_after: Cleaned DataFrame.
    """
    print("\n" + "=" * 60)
    print("DATASET CLEANING SUMMARY")
    print("=" * 60)

    print(f"\n  Rows:    {len(df_before):>8,} -> {len(df_after):>8,}"
          f"  (dropped {len(df_before) - len(df_after):,})")
    print(f"  Columns: {len(df_before.columns):>7,} -> "
          f"{len(df_after.columns):>7,}"
          f"  (dropped "
          f"{len(df_before.columns) - len(df_after.columns):,})")

    # Category distribution after cleaning.
    col = "category_name"
    if col in df_after.columns:
        print(f"\n  Category distribution ({col}):")
        counts = df_after[col].value_counts().sort_values(
            ascending=False
        )
        for cat, count in counts.items():
            pct = 100.0 * count / len(df_after)
            print(f"    {cat:<30s} {count:>7,} ({pct:5.1f}%)")

    # Transport strategy distribution after cleaning.
    col = "transport_strategy"
    if col in df_after.columns:
        print(f"\n  Transport strategy distribution ({col}):")
        counts = df_after[col].value_counts().sort_values(
            ascending=False
        )
        for strategy, count in counts.items():
            pct = 100.0 * count / len(df_after)
            print(f"    {strategy:<30s} {count:>7,} ({pct:5.1f}%)")

    # Key numeric column stats after cleaning.
    numeric_cols = [
        "cf_total_kg_co2e",
        "cf_raw_materials_kg_co2e",
        "weighted_ef_g_co2e_tkm",
        "total_transport_distance_km",
        "total_weight_kg",
    ]
    present_cols = [c for c in numeric_cols if c in df_after.columns]
    if present_cols:
        print("\n  Key numeric statistics (after cleaning):")
        for col in present_cols:
            series = df_after[col]
            print(
                f"    {col}:"
                f"  min={series.min():.4f}"
                f"  median={series.median():.4f}"
                f"  mean={series.mean():.4f}"
                f"  max={series.max():.4f}"
            )

    print("\n" + "=" * 60 + "\n")


def main():
    """Main entry point for dataset cleaning."""
    args = parse_args()
    project_root = get_project_root()

    if args.input is None:
        input_path = (
            project_root / "data" / "datasets" / "model"
            / "full_dataset.parquet"
        )
    else:
        input_path = Path(args.input)

    if args.output is None:
        output_path = (
            project_root / "data" / "datasets" / "model"
            / "cleaned_dataset.parquet"
        )
    else:
        output_path = Path(args.output)

    logger.info("Input:  %s", input_path)
    logger.info("Output: %s", output_path)

    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        sys.exit(1)

    # Load (auto-detect format by extension).
    logger.info("Loading dataset...")
    if input_path.suffix == '.parquet':
        df = pd.read_parquet(input_path)
    else:
        df = pd.read_csv(input_path)
    logger.info(
        "Loaded %d rows, %d columns",
        len(df), len(df.columns),
    )
    df_before = df.copy()

    # Step 1: Fix category typos.
    logger.info("Step 1: Fixing category typos...")
    df = fix_category_typos(df)

    # Step 2: Drop unknown transport rows.
    logger.info("Step 2: Dropping unknown transport rows...")
    df = drop_unknown_transport(df)

    # Step 3: Drop zero-information and redundant columns.
    logger.info("Step 3: Dropping columns...")
    df = drop_columns(df)

    # Step 4: Cap outliers.
    logger.info("Step 4: Capping outliers...")
    df = cap_outliers(df)

    # Summary.
    log_summary(df_before, df)

    # Write output (auto-detect format by extension).
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix == '.parquet':
        df.to_parquet(
            output_path, index=False, compression='gzip'
        )
    else:
        df.to_csv(output_path, index=False)
    logger.info("Saved cleaned dataset: %s", output_path)
    logger.info(
        "Final: %d rows, %d columns",
        len(df), len(df.columns),
    )


if __name__ == '__main__':
    main()
