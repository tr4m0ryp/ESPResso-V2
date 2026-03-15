"""
Create model-specific pre-split datasets from Layer 6 output.

Splits at the core-product level to prevent data leakage, then
generates per-model CSVs with only the required columns and
appropriate deduplication.

Output: datasets/model/{composition_weight,processing,distance}/*.csv

Usage:
    python create_splits.py
    python create_splits.py --seed 42 --train-ratio 0.70
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sklearn.model_selection import train_test_split

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CORE_PRODUCT_COLS = [
    "category_name",
    "subcategory_name",
    "materials",
    "material_percentages",
    "total_weight_kg",
]

PROCESSING_PATH_COLS = CORE_PRODUCT_COLS + [
    "preprocessing_steps",
    "detailed_preprocessing_steps",
]

MODEL_COLUMNS = {
    "composition_weight": [
        "category_name",
        "subcategory_name",
        "materials",
        "material_percentages",
        "total_weight_kg",
    ],
    "processing": [
        "category_name",
        "subcategory_name",
        "materials",
        "preprocessing_steps",
        "detailed_preprocessing_steps",
    ],
    "distance": [
        "category_name",
        "subcategory_name",
        "materials",
        "preprocessing_steps",
        "origin_region",
        "transport_strategy",
        "total_transport_distance_km",
        "detailed_preprocessing_steps",
        "supply_chain_type",
    ],
    "carbon": [
        "category_name",
        "subcategory_name",
        "materials",
        "material_weights_kg",
        "material_percentages",
        "total_weight_kg",
        "preprocessing_steps",
        "total_transport_distance_km",
        "cf_total_kg_co2e",
    ],
}

DEDUP_KEYS = {
    "composition_weight": CORE_PRODUCT_COLS,
    "processing": PROCESSING_PATH_COLS,
    "distance": None,
    "carbon": None,
}

SPLIT_NAMES = ["train", "validation", "test"]


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Create model-specific train/val/test splits "
            "from Layer 6 data"
        )
    )
    parser.add_argument(
        '--input', type=str, default=None,
        help=(
            'Path to input CSV '
            '(default: datasets/generated/layer_6/training_dataset.csv)'
        ),
    )
    parser.add_argument(
        '--output-dir', type=str, default=None,
        help='Output directory (default: datasets/model)',
    )
    parser.add_argument(
        '--train-ratio', type=float, default=0.70,
        help='Training set ratio (default: 0.70)',
    )
    parser.add_argument(
        '--val-ratio', type=float, default=0.15,
        help='Validation set ratio (default: 0.15)',
    )
    parser.add_argument(
        '--test-ratio', type=float, default=0.15,
        help='Test set ratio (default: 0.15)',
    )
    parser.add_argument(
        '--seed', type=int, default=42,
        help='Random seed for reproducibility (default: 42)',
    )
    parser.add_argument(
        '--stratify-by', type=str, default='category_name',
        help='Column to stratify by (default: category_name)',
    )
    return parser.parse_args()


def get_project_root() -> Path:
    """Get project root directory."""
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / 'CLAUDE.md').exists():
            return parent
    return current.parent.parent


def validate_ratios(
    train_ratio: float,
    val_ratio: float,
    test_ratio: float,
) -> None:
    """Validate that split ratios sum to 1.0."""
    total = train_ratio + val_ratio + test_ratio
    if abs(total - 1.0) > 0.001:
        raise ValueError(
            f"Split ratios must sum to 1.0, got {total:.3f} "
            f"(train={train_ratio}, val={val_ratio}, "
            f"test={test_ratio})"
        )


def assign_product_splits(
    df: pd.DataFrame,
    seed: int,
    train_ratio: float,
    val_ratio: float,
    test_ratio: float,
    stratify_col: str,
) -> pd.DataFrame:
    """Assign each core product to a split, then propagate.

    Splits at the core-product level so all transport
    scenarios for a product stay in the same split.

    Args:
        df: Full source DataFrame.
        seed: Random seed.
        train_ratio: Training set fraction.
        val_ratio: Validation set fraction.
        test_ratio: Test set fraction.
        stratify_col: Column for stratified splitting.

    Returns:
        DataFrame with a new 'split' column.
    """
    validate_ratios(train_ratio, val_ratio, test_ratio)

    # Build one-row-per-core-product table.
    products = (
        df[CORE_PRODUCT_COLS]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    products["_product_id"] = range(len(products))
    logger.info(
        "Found %d unique core products", len(products)
    )

    # Separate rare categories (< 3 products) that would
    # break stratification. Assign them directly to train.
    rare_mask = pd.Series(False, index=products.index)
    if stratify_col in products.columns:
        cat_counts = products[stratify_col].value_counts()
        # Need enough products per category for stratified
        # splitting into both val and test. With small val/test
        # ratios, require more products per category.
        min_products = max(3, int(2 / min(
            val_ratio, test_ratio,
        )))
        rare_cats = cat_counts[
            cat_counts < min_products
        ].index
        if len(rare_cats) > 0:
            rare_mask = products[stratify_col].isin(rare_cats)
            logger.info(
                "Moving %d products from %d rare categories "
                "to train (too few for stratification)",
                rare_mask.sum(),
                len(rare_cats),
            )

    rare_products = products[rare_mask].copy()
    splittable = products[~rare_mask].copy()

    # Stratified split at product level.
    stratify = None
    if stratify_col in splittable.columns:
        stratify = splittable[stratify_col]
        logger.info("Stratifying by '%s'", stratify_col)

    train_prod, temp_prod = train_test_split(
        splittable,
        train_size=train_ratio,
        random_state=seed,
        stratify=stratify,
    )

    val_adjusted = val_ratio / (val_ratio + test_ratio)
    stratify_temp = None
    if stratify_col in temp_prod.columns:
        stratify_temp = temp_prod[stratify_col]

    val_prod, test_prod = train_test_split(
        temp_prod,
        train_size=val_adjusted,
        random_state=seed,
        stratify=stratify_temp,
    )

    # Merge rare-category products into train.
    train_prod = pd.concat(
        [train_prod, rare_products], ignore_index=True
    )

    # Build product_id -> split mapping.
    train_prod = train_prod.copy()
    val_prod = val_prod.copy()
    test_prod = test_prod.copy()
    train_prod["split"] = "train"
    val_prod["split"] = "validation"
    test_prod["split"] = "test"

    product_splits = pd.concat(
        [train_prod, val_prod, test_prod],
        ignore_index=True,
    )

    # Merge split assignment to all rows.
    result = df.merge(
        product_splits[CORE_PRODUCT_COLS + ["split"]],
        on=CORE_PRODUCT_COLS,
        how="left",
    )

    unassigned = result["split"].isna().sum()
    if unassigned > 0:
        logger.warning(
            "%d rows could not be assigned a split", unassigned
        )
        result["split"] = result["split"].fillna("train")

    for split_name in SPLIT_NAMES:
        count = (result["split"] == split_name).sum()
        logger.info("  %s: %d rows", split_name, count)

    return result


def generate_model_dataset(
    df: pd.DataFrame,
    model_name: str,
    output_dir: Path,
) -> dict:
    """Generate train/val/test CSVs for one model.

    Applies model-specific column selection and deduplication.

    Args:
        df: Full DataFrame with 'split' column.
        model_name: One of the MODEL_COLUMNS keys.
        output_dir: Root output directory.

    Returns:
        Dictionary with per-split row counts.
    """
    columns = MODEL_COLUMNS[model_name]
    dedup_key = DEDUP_KEYS[model_name]

    model_dir = output_dir / model_name
    model_dir.mkdir(parents=True, exist_ok=True)

    counts = {}
    for split_name in SPLIT_NAMES:
        split_df = df[df["split"] == split_name].copy()

        if dedup_key is not None:
            before = len(split_df)
            split_df = split_df.drop_duplicates(
                subset=dedup_key
            )
            logger.info(
                "  %s/%s: %d -> %d rows (dedup on %d cols)",
                model_name,
                split_name,
                before,
                len(split_df),
                len(dedup_key),
            )

        split_df = split_df[columns]
        out_path = model_dir / f"{split_name}.csv"
        split_df.to_csv(out_path, index=False)

        counts[split_name] = len(split_df)
        logger.info(
            "  Saved %s/%s.csv: %d rows, %d columns",
            model_name,
            split_name,
            len(split_df),
            len(columns),
        )

    return counts


def generate_split_info(
    all_counts: dict,
    seed: int,
    output_dir: Path,
) -> None:
    """Write split_info.csv with metadata for all models.

    Args:
        all_counts: Dict of {model: {split: row_count}}.
        seed: Random seed used.
        output_dir: Root output directory.
    """
    splits_dir = output_dir / "splits"
    splits_dir.mkdir(parents=True, exist_ok=True)

    rows = []
    timestamp = datetime.now(timezone.utc).isoformat()
    for model_name in MODEL_COLUMNS:
        columns = MODEL_COLUMNS[model_name]
        dedup_level = DEDUP_KEYS[model_name]
        dedup_desc = "none"
        if dedup_level is not None:
            if dedup_level == CORE_PRODUCT_COLS:
                dedup_desc = "core_product"
            else:
                dedup_desc = "processing_path"

        for split_name in SPLIT_NAMES:
            count = all_counts.get(
                model_name, {}
            ).get(split_name, 0)
            rows.append({
                "model": model_name,
                "split": split_name,
                "rows": count,
                "columns": len(columns),
                "column_names": "|".join(columns),
                "dedup_level": dedup_desc,
                "seed": seed,
                "timestamp": timestamp,
            })

    info_df = pd.DataFrame(rows)
    info_path = splits_dir / "split_info.csv"
    info_df.to_csv(info_path, index=False)
    logger.info("Saved split metadata: %s", info_path)


def print_summary(all_counts: dict) -> None:
    """Print summary of all generated datasets."""
    print("\n" + "=" * 60)
    print("MODEL DATASET SPLIT SUMMARY")
    print("=" * 60)

    for model_name in MODEL_COLUMNS:
        counts = all_counts.get(model_name, {})
        total = sum(counts.values())
        cols = len(MODEL_COLUMNS[model_name])
        dedup = DEDUP_KEYS[model_name]
        dedup_str = (
            "none" if dedup is None
            else f"{len(dedup)}-col dedup"
        )

        print(f"\n  {model_name} ({cols} columns, {dedup_str}):")
        for split_name in SPLIT_NAMES:
            n = counts.get(split_name, 0)
            pct = 100 * n / total if total > 0 else 0
            print(f"    {split_name:<12} {n:>8,} ({pct:.1f}%)")
        print(f"    {'total':<12} {total:>8,}")

    print("\n" + "=" * 60 + "\n")


def main():
    """Main entry point."""
    args = parse_args()
    project_root = get_project_root()

    if args.input is None:
        input_path = (
            project_root / "data" / "datasets" / "generated"
            / "layer_6" / "training_dataset.csv"
        )
    else:
        input_path = Path(args.input)

    if args.output_dir is None:
        output_dir = project_root / "data" / "datasets" / "model"
    else:
        output_dir = Path(args.output_dir)

    logger.info("Input file: %s", input_path)
    logger.info("Output directory: %s", output_dir)

    if not input_path.exists():
        logger.error("Input file not found: %s", input_path)
        sys.exit(1)

    logger.info("Loading dataset...")
    df = pd.read_csv(input_path)
    logger.info(
        "Loaded %d records with %d columns",
        len(df),
        len(df.columns),
    )

    # Assign splits at core-product level.
    df = assign_product_splits(
        df=df,
        seed=args.seed,
        train_ratio=args.train_ratio,
        val_ratio=args.val_ratio,
        test_ratio=args.test_ratio,
        stratify_col=args.stratify_by,
    )

    # Generate per-model datasets.
    all_counts = {}
    for model_name in MODEL_COLUMNS:
        logger.info("Generating %s dataset...", model_name)
        counts = generate_model_dataset(
            df, model_name, output_dir
        )
        all_counts[model_name] = counts

    generate_split_info(all_counts, args.seed, output_dir)
    print_summary(all_counts)

    logger.info("Done!")


if __name__ == '__main__':
    main()
