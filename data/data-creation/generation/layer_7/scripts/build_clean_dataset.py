#!/usr/bin/env python3
"""
Build the final model-ready water footprint dataset.

Loads layer 5 + layer 4 data, extracts material_journeys from transport_legs,
removes unnecessary columns, and saves a clean dataset ready for water footprint
value merging.
"""

import json
import os
import re
import sys

import pandas as pd

# ---------------------------------------------------------------------------
# Paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..")
)
L5_PATH = os.path.join(
    PROJECT_ROOT,
    "data/datasets/pre-model/generated/layer_5/layer_5_validated_dataset.csv",
)
L4_PATH = os.path.join(
    PROJECT_ROOT,
    "data/datasets/pre-model/generated/layer_4/layer_4_complete_dataset.parquet",
)
OUTPUT_DIR = os.path.join(PROJECT_ROOT, "data/datasets/model/final")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "water_footprint.csv")

# ---------------------------------------------------------------------------
# Country alias map
# ---------------------------------------------------------------------------
COUNTRY_ALIASES = {
    "Turkey": "Turkiye",
    "USA": "United States of America",
    "UK": "United Kingdom",
    "England": "United Kingdom",
    "Scotland": "United Kingdom",
    "Czech Republic": "Czechia",
    "Kitwe": "Zambia",
    "Lusaka": "Zambia",
    "Tashkent": "Uzbekistan",
    "Fergana Valley": "Uzbekistan",
}

# ---------------------------------------------------------------------------
# Columns to keep in the final dataset
# ---------------------------------------------------------------------------
KEEP_COLUMNS = [
    "record_id",
    "subcategory_name",
    "category_name",
    "materials",
    "material_weights_kg",
    "material_percentages",
    "preprocessing_steps",
    "total_weight_kg",
    "total_packaging_mass_kg",
    "packaging_categories",
    "material_journeys",
]


def resolve_country(location: str) -> str:
    """Extract country from a 'City, Country' location string and apply aliases."""
    parts = [p.strip() for p in location.split(",")]
    raw = parts[-1] if len(parts) >= 2 else parts[0]
    return COUNTRY_ALIASES.get(raw, raw)


def extract_material_journeys(transport_legs_str: str) -> str:
    """
    Parse transport_legs JSON and build material_journeys (Option B format).

    For each material:
      - origin = from_location of the first leg (lowest leg_index)
      - processing = from_location of the last leg (highest leg_index),
        representing the final processing step before warehouse/assembly
    """
    try:
        legs = json.loads(transport_legs_str)
    except (json.JSONDecodeError, TypeError):
        return "[]"

    if not legs:
        return "[]"

    # Group legs by material name
    material_legs = {}
    for leg in legs:
        mat = leg.get("material", "unknown")
        material_legs.setdefault(mat, []).append(leg)

    journeys = []
    for mat, mat_legs in material_legs.items():
        mat_legs.sort(key=lambda l: l.get("leg_index", 0))
        first = mat_legs[0]
        last = mat_legs[-1]

        journeys.append({
            "material": mat,
            "origin_country": resolve_country(first["from_location"]),
            "origin_lat": first["from_lat"],
            "origin_lon": first["from_lon"],
            "processing_country": resolve_country(last["from_location"]),
            "processing_lat": last["from_lat"],
            "processing_lon": last["from_lon"],
        })

    return json.dumps(journeys)


def load_and_merge() -> pd.DataFrame:
    """Load layer 5 + layer 4, merge on preprocessing_path_id."""
    print(f"Loading layer 5 from: {L5_PATH}")
    l5 = pd.read_csv(L5_PATH)
    print(f"  Layer 5: {l5.shape[0]} rows, {l5.shape[1]} columns")

    print(f"Loading layer 4 from: {L4_PATH}")
    l4 = pd.read_parquet(L4_PATH, columns=["preprocessing_path_id", "transport_legs"])
    l4 = l4.drop_duplicates(subset="preprocessing_path_id")
    print(f"  Layer 4: {l4.shape[0]} unique preprocessing paths")

    # Extract pp-ID from record_id for join
    l5["pp_id"] = l5["record_id"].apply(
        lambda x: m.group(1) if (m := re.search(r"(pp-\d+)", str(x))) else ""
    )
    merged = l5.merge(l4, left_on="pp_id", right_on="preprocessing_path_id", how="inner")
    merged.drop(columns=["pp_id", "preprocessing_path_id"], inplace=True)
    print(f"  Merged: {merged.shape[0]} rows")
    return merged


def build_clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    """Extract material_journeys and select final columns."""
    print("Extracting material_journeys from transport_legs...")
    df["material_journeys"] = df["transport_legs"].apply(extract_material_journeys)

    # Select only the columns we need
    missing = [c for c in KEEP_COLUMNS if c not in df.columns]
    if missing:
        print(f"  WARNING: Missing columns: {missing}")

    available = [c for c in KEEP_COLUMNS if c in df.columns]
    result = df[available].copy()
    print(f"  Final dataset: {result.shape[0]} rows, {result.shape[1]} columns")
    return result


def print_summary(df: pd.DataFrame) -> None:
    """Print dataset summary and sample material_journeys for verification."""
    print("\n--- Dataset Summary ---")
    print(f"Rows: {df.shape[0]}")
    print(f"Columns: {df.shape[1]}")
    print(f"Column names: {list(df.columns)}")

    print("\n--- Sample material_journeys (first 3 records) ---")
    for i in range(min(3, len(df))):
        rid = df.iloc[i]["record_id"]
        mj = df.iloc[i]["material_journeys"]
        parsed = json.loads(mj)
        print(f"\n  Record: {rid}")
        print(f"  Materials: {df.iloc[i]['materials']}")
        for j in parsed:
            print(
                f"    {j['material']}: "
                f"origin={j['origin_country']} ({j['origin_lat']}, {j['origin_lon']}) -> "
                f"processing={j['processing_country']} "
                f"({j['processing_lat']}, {j['processing_lon']})"
            )

    # Country distribution across all journeys
    country_counts = {}
    for mj_str in df["material_journeys"]:
        for j in json.loads(mj_str):
            for key in ("origin_country", "processing_country"):
                c = j[key]
                country_counts[c] = country_counts.get(c, 0) + 1
    top_10 = sorted(country_counts.items(), key=lambda x: -x[1])[:10]
    print("\n--- Top 10 countries (origin + processing combined) ---")
    for c, n in top_10:
        print(f"  {c}: {n}")


def main() -> None:
    """Entry point: load, clean, save."""
    merged = load_and_merge()
    clean = build_clean_dataset(merged)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    clean.to_csv(OUTPUT_PATH, index=False)
    print(f"\nSaved to: {OUTPUT_PATH}")

    print_summary(clean)

    # Verify no alias targets leaked through
    alias_leaks = []
    for mj_str in clean["material_journeys"]:
        for j in json.loads(mj_str):
            for key in ("origin_country", "processing_country"):
                if j[key] in COUNTRY_ALIASES:
                    alias_leaks.append(j[key])
    if alias_leaks:
        print(f"\n  WARNING: Unresolved aliases found: {set(alias_leaks)}")
    else:
        print("\n  Country alias resolution: all clean.")


if __name__ == "__main__":
    main()
