#!/usr/bin/env python3
"""
Extract net water consumption (m3/kg) from EcoInvent 3.12 Derby DB
for all base materials, processing steps, and packaging categories.

Strategy: pre-dump all processes, water exchanges, and reference flows
from Derby, then match in-memory. Only searches among processes that
actually have Water elementary flow data.

Outputs to data/datasets/pre-model/final/water_footprint/:
  - base_materials_water.csv
  - processing_steps_water.csv
  - packaging_water.csv
  - water_data_gaps.csv
"""

import csv
import sys
from pathlib import Path

# Add script directory to path for sibling imports
sys.path.insert(0, str(Path(__file__).resolve().parent))

from ecoinvent_db import find_best_match, load_ecoinvent_data
from ecoinvent_patterns import (
    MATERIAL_PATTERNS, PACKAGING_PATTERNS, PROCESS_PATTERNS,
)

# -- Paths --
PROJECT_ROOT = Path(__file__).resolve().parents[4]
FINAL_DIR = PROJECT_ROOT / "data" / "datasets" / "pre-model" / "final"
PARQUET_DIR = FINAL_DIR / "carbon_footprint"
OUTPUT_DIR = FINAL_DIR / "water_footprint"


def write_csv(path: Path, rows: list[dict], fields: list[str]):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
    print(f"  Wrote {len(rows)} rows -> {path.name}")


def search_items(names, pattern_map, water_procs, water_map, ref_map,
                 item_type, label):
    """Search EcoInvent for a list of items. Returns (results, gaps)."""
    results, gaps = [], []
    print(f"\n[{label}] {len(names)} items")

    for name in names:
        patterns = pattern_map.get(name)
        if not patterns:
            core = name.split(",")[0].strip().replace("_", " ")
            patterns = [f"%{core}%production%", f"%market for%{core}%"]

        r = find_best_match(patterns, water_procs, water_map, ref_map)
        if r:
            results.append({
                "name": name,
                "water_m3_per_kg": r["water_m3_per_kg"],
                "process_name": r["process_name"],
                "process_id": r["process_id"],
                "location": r["location"],
            })
            print(f"  [OK] {name} -> {r['water_m3_per_kg']} m3/kg")
        else:
            sq = "; ".join(p.replace("%", "") for p in patterns[:3])
            gaps.append({
                "item_name": name,
                "item_type": item_type,
                "searched_queries": sq,
                "notes": "No EcoInvent match found",
            })
            print(f"  [GAP] {name}")

    return results, gaps


def main():
    import pandas as pd

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("Loading EcoInvent data...")
    all_procs, water_procs, water_map, ref_map = load_ecoinvent_data()
    print(f"  {len(all_procs)} total processes, "
          f"{len(water_procs)} with water data")

    # Load source data
    mat_df = pd.read_parquet(PARQUET_DIR / "base_materials.parquet")
    proc_df = pd.read_parquet(PARQUET_DIR / "processing_steps.parquet")
    pkg_df = pd.read_parquet(
        PARQUET_DIR / "packaging_materials_by_category.parquet")

    # -- Materials --
    mat_res, mat_gaps = search_items(
        mat_df["material_name"].tolist(),
        MATERIAL_PATTERNS, water_procs, water_map, ref_map,
        "material", "MATERIALS")

    # -- Processing steps --
    proc_res, proc_gaps = search_items(
        proc_df["process_name"].tolist(),
        PROCESS_PATTERNS, water_procs, water_map, ref_map,
        "process", "PROCESSING")

    # -- Packaging --
    pkg_res, pkg_gaps = search_items(
        pkg_df["name"].tolist(),
        PACKAGING_PATTERNS, water_procs, water_map, ref_map,
        "packaging", "PACKAGING")

    # -- Write CSVs --
    print("\nWriting CSVs...")
    write_csv(
        OUTPUT_DIR / "base_materials_water.csv",
        [{"material_name": r["name"],
          "water_consumption_m3_per_kg": r["water_m3_per_kg"],
          "ecoinvent_process_name": r["process_name"],
          "ecoinvent_process_id": r["process_id"],
          "ecoinvent_location": r["location"],
          "source": "ecoinvent"} for r in mat_res],
        ["material_name", "water_consumption_m3_per_kg",
         "ecoinvent_process_name", "ecoinvent_process_id",
         "ecoinvent_location", "source"])

    write_csv(
        OUTPUT_DIR / "processing_steps_water.csv",
        [{"process_name": r["name"],
          "water_consumption_m3_per_kg": r["water_m3_per_kg"],
          "ecoinvent_process_name": r["process_name"],
          "ecoinvent_process_id": r["process_id"],
          "ecoinvent_location": r["location"],
          "source": "ecoinvent"} for r in proc_res],
        ["process_name", "water_consumption_m3_per_kg",
         "ecoinvent_process_name", "ecoinvent_process_id",
         "ecoinvent_location", "source"])

    write_csv(
        OUTPUT_DIR / "packaging_water.csv",
        [{"category": r["name"],
          "water_consumption_m3_per_kg": r["water_m3_per_kg"],
          "ecoinvent_process_name": r["process_name"],
          "ecoinvent_process_id": r["process_id"],
          "source": "ecoinvent"} for r in pkg_res],
        ["category", "water_consumption_m3_per_kg",
         "ecoinvent_process_name", "ecoinvent_process_id",
         "source"])

    all_gaps = mat_gaps + proc_gaps + pkg_gaps
    write_csv(OUTPUT_DIR / "water_data_gaps.csv", all_gaps,
              ["item_name", "item_type", "searched_queries", "notes"])

    # -- Summary --
    n_mat = len(mat_df)
    n_proc = len(proc_df)
    n_pkg = len(pkg_df)
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Materials:  {len(mat_res)}/{n_mat} matched, "
          f"{len(mat_gaps)} gaps")
    print(f"Processing: {len(proc_res)}/{n_proc} matched, "
          f"{len(proc_gaps)} gaps")
    print(f"Packaging:  {len(pkg_res)}/{n_pkg} matched, "
          f"{len(pkg_gaps)} gaps")
    print(f"Total gaps: {len(all_gaps)}")
    print(f"\nOutput: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
