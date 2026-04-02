"""Extract carbon footprint features from the training dataset.

Reads layer_6/training_dataset.parquet, applies quality filters, extracts
step_locations and material_chains from transport_legs, computes completeness
heuristics, and writes model/data/carbon_footprint.parquet.
"""

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
SRC_PATH = ROOT / "data/datasets/pre-model/generated/layer_6/training_dataset.parquet"
OUT_PARQUET = ROOT / "model/data/carbon_footprint.parquet"
OUT_STATS = ROOT / "model/data/category_stats.json"

STAGE_MAP = {
    "ginning": "raw_prep", "retting": "raw_prep", "scutching": "raw_prep",
    "decortication": "raw_prep", "sorting_grading": "raw_prep",
    "cork_boiling": "raw_prep", "cork_grinding": "raw_prep",
    "degumming": "fiber_proc", "bleaching": "fiber_proc",
    "scouring": "fiber_proc", "mercerizing": "fiber_proc",
    "carding": "yarn", "combing": "yarn", "drawing": "yarn",
    "spinning": "yarn", "texturing": "yarn",
    "weaving": "fabric", "knitting": "fabric",
    "nonwoven production": "fabric", "laminating": "fabric",
    "batch dyeing": "wet_proc", "continuous dyeing": "wet_proc",
    "printing": "wet_proc", "washing_sanitising": "wet_proc",
    "waterproofing": "wet_proc", "coating": "wet_proc",
    "finishing": "finishing", "softening": "finishing",
    "heat setting": "finishing", "sanforizing": "finishing",
    "calendering": "finishing", "raising": "finishing",
    "flame retardant treatment": "finishing",
    "antimicrobial treatment": "finishing",
}
ALL_STAGES = {"raw_prep", "fiber_proc", "yarn", "fabric", "wet_proc", "finishing"}


def load_and_filter(path: Path) -> pd.DataFrame:
    """Load source parquet and apply quality filters."""
    df = pd.read_parquet(path)
    n_before = len(df)
    mask = (df["coherence_recommendation"] != "reject") & (~df["is_duplicate"])
    df = df[mask].reset_index(drop=True)
    print(f"Quality filter: {n_before} -> {len(df)} rows "
          f"(dropped {n_before - len(df)} rejects/duplicates)")
    return df


def extract_step_locations(legs: list) -> dict:
    """Extract step -> [{lat, lon}, ...] mapping from transport legs."""
    step_locs: dict[str, set] = {}
    for leg in legs:
        for prefix in ("from", "to"):
            step = leg[f"{prefix}_step"]
            loc = (round(leg[f"{prefix}_lat"], 4), round(leg[f"{prefix}_lon"], 4))
            step_locs.setdefault(step, set()).add(loc)
    return {
        k: [{"lat": lat, "lon": lon} for lat, lon in sorted(v)]
        for k, v in step_locs.items()
    }


def extract_material_chains(legs: list) -> dict:
    """Extract material -> [{step, lat, lon}, ...] chains from transport legs."""
    chains: dict[str, list] = {}
    for leg in legs:
        mat = leg["material"]
        from_entry = {"step": leg["from_step"],
                      "lat": leg["from_lat"], "lon": leg["from_lon"]}
        to_entry = {"step": leg["to_step"],
                    "lat": leg["to_lat"], "lon": leg["to_lon"]}
        chains.setdefault(mat, [])
        if not chains[mat] or chains[mat][-1] != from_entry:
            chains[mat].append(from_entry)
        chains[mat].append(to_entry)
    return chains


def count_step_loc_tokens(step_locations: dict) -> int:
    """Count total (step, location) tokens for a product."""
    return sum(len(locs) for locs in step_locations.values())


def get_stages(step_locations: dict) -> set:
    """Map step names to pipeline stages, returning the set of covered stages."""
    stages = set()
    for step in step_locations:
        stage = STAGE_MAP.get(step)
        if stage is not None:
            stages.add(stage)
    return stages


def compute_transport_extractions(df: pd.DataFrame) -> pd.DataFrame:
    """Parse transport_legs and compute step_locations, material_chains."""
    step_locs_list = []
    mat_chains_list = []
    n_tokens_list = []
    stages_list = []

    for tl_json in df["transport_legs"]:
        legs = json.loads(tl_json)
        sl = extract_step_locations(legs)
        mc = extract_material_chains(legs)
        step_locs_list.append(json.dumps(sl))
        mat_chains_list.append(json.dumps(mc))
        n_tokens_list.append(count_step_loc_tokens(sl))
        stages_list.append(get_stages(sl))

    df = df.copy()
    df["step_locations"] = step_locs_list
    df["material_chains"] = mat_chains_list
    df["_n_tokens"] = n_tokens_list
    df["_stages"] = stages_list
    return df


def compute_completeness(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Compute step_zscore and stage_coverage per product."""
    cat_stats = df.groupby("category_name")["_n_tokens"].agg(["mean", "std"])
    cat_stats["std"] = cat_stats["std"].replace(0.0, 1.0)

    # step_zscore
    df = df.copy()
    df["step_zscore"] = df.apply(
        lambda r: (r["_n_tokens"] - cat_stats.loc[r["category_name"], "mean"])
                  / cat_stats.loc[r["category_name"], "std"],
        axis=1,
    )

    # Expected stages per category (present in >50% of products)
    cat_stage_freq: dict[str, dict[str, float]] = {}
    for cat, grp in df.groupby("category_name"):
        stage_counts: dict[str, int] = {s: 0 for s in ALL_STAGES}
        for stages in grp["_stages"]:
            for s in stages:
                stage_counts[s] += 1
        n = len(grp)
        cat_stage_freq[cat] = {s: c / n for s, c in stage_counts.items()}

    expected_stages: dict[str, set] = {}
    for cat, freqs in cat_stage_freq.items():
        expected_stages[cat] = {s for s, f in freqs.items() if f > 0.5}

    # stage_coverage
    coverages = []
    for _, row in df.iterrows():
        cat = row["category_name"]
        exp = expected_stages[cat]
        if not exp:
            coverages.append(1.0)
        else:
            present = row["_stages"] & exp
            coverages.append(len(present) / len(exp))
    df["stage_coverage"] = coverages

    # Build stats dict for inference
    stats_dict = {}
    for cat in cat_stats.index:
        stats_dict[cat] = {
            "mean_tokens": round(float(cat_stats.loc[cat, "mean"]), 4),
            "std_tokens": round(float(cat_stats.loc[cat, "std"]), 4),
            "expected_stages": sorted(expected_stages.get(cat, set())),
        }

    return df, stats_dict


def compute_mode_fractions(df: pd.DataFrame) -> pd.DataFrame:
    """Compute road_frac and sea_frac from distance columns."""
    df = df.copy()
    total = df["total_transport_distance_km"].replace(0.0, np.nan)
    df["road_frac"] = (df["road_km"] / total).fillna(0.0)
    df["sea_frac"] = (df["sea_km"] / total).fillna(0.0)
    return df


def select_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Select and order final output columns."""
    cols = [
        # Identity
        "record_id", "category_name", "subcategory_name",
        # Inference-time features
        "materials", "material_percentages",
        "total_weight_kg", "total_packaging_mass_kg",
        "preprocessing_steps", "step_locations",
        "packaging_categories", "packaging_masses_kg",
        "step_zscore", "stage_coverage",
        # Privileged features
        "material_chains",
        "road_km", "sea_km", "rail_km", "air_km", "inland_waterway_km",
        "total_transport_distance_km",
        "road_frac", "sea_frac",
        # Targets
        "cf_raw_materials_kg_co2e", "cf_transport_kg_co2e",
        "cf_processing_kg_co2e", "cf_packaging_kg_co2e",
        # Quality reference
        "is_outlier",
    ]
    return df[cols].copy()


def print_summary(df: pd.DataFrame) -> None:
    """Print summary statistics for verification."""
    print(f"\n--- Output Summary ---")
    print(f"Rows: {len(df)}")
    print(f"Columns ({len(df.columns)}): {list(df.columns)}")
    print(f"\nTarget distributions:")
    targets = ["cf_raw_materials_kg_co2e", "cf_transport_kg_co2e",
               "cf_processing_kg_co2e", "cf_packaging_kg_co2e"]
    for t in targets:
        s = df[t]
        print(f"  {t}: mean={s.mean():.3f}  std={s.std():.3f}  "
              f"min={s.min():.3f}  max={s.max():.3f}  "
              f"zeros={int((s == 0).sum())}")
    print(f"\nCompleteness heuristics:")
    print(f"  step_zscore: mean={df['step_zscore'].mean():.4f}  "
          f"std={df['step_zscore'].std():.4f}")
    print(f"  stage_coverage: mean={df['stage_coverage'].mean():.4f}  "
          f"std={df['stage_coverage'].std():.4f}")
    print(f"\nMode fractions:")
    print(f"  road_frac: mean={df['road_frac'].mean():.4f}")
    print(f"  sea_frac: mean={df['sea_frac'].mean():.4f}")
    frac_sum = df["road_frac"] + df["sea_frac"]
    rail_frac = df["rail_km"] / df["total_transport_distance_km"].replace(0, np.nan)
    air_frac = df["air_km"] / df["total_transport_distance_km"].replace(0, np.nan)
    ww_frac = df["inland_waterway_km"] / df["total_transport_distance_km"].replace(0, np.nan)
    total_frac = (frac_sum + rail_frac.fillna(0) + air_frac.fillna(0)
                  + ww_frac.fillna(0))
    valid = total_frac.dropna()
    print(f"  all-mode frac sum: mean={valid.mean():.6f}  "
          f"min={valid.min():.6f}  max={valid.max():.6f}")
    print(f"\nCategories: {df['category_name'].nunique()}")
    print(f"Category distribution:")
    for cat, cnt in df["category_name"].value_counts().items():
        print(f"  {cat}: {cnt}")


def main() -> None:
    if not SRC_PATH.exists():
        print(f"ERROR: Source parquet not found at {SRC_PATH}", file=sys.stderr)
        sys.exit(1)

    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    df = load_and_filter(SRC_PATH)
    df = compute_transport_extractions(df)
    df, cat_stats = compute_completeness(df)
    df = compute_mode_fractions(df)
    df = select_columns(df)

    df.to_parquet(OUT_PARQUET, index=False)
    print(f"Wrote {OUT_PARQUET} ({OUT_PARQUET.stat().st_size / 1024 / 1024:.1f} MB)")

    with open(OUT_STATS, "w") as f:
        json.dump(cat_stats, f, indent=2)
    print(f"Wrote {OUT_STATS}")

    print_summary(df)


if __name__ == "__main__":
    main()
