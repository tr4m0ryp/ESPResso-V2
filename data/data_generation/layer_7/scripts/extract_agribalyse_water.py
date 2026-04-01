#!/usr/bin/env python3
"""
Extract water consumption from Agribalyse 3.2 for EcoInvent gaps.

Reads water_data_gaps.csv, searches Agribalyse DB for matches,
appends found values to the existing water CSVs with source="agribalyse",
and updates the gaps file to remove found items.

Requires: Agribalyse DB extracted at /tmp/agribalyse_db/agribalyse_db/
          Derby JDBC driver at /tmp/derby/db-derby-10.17.1.0-lib/lib/
"""

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from agribalyse_db import find_best_match, load_agribalyse_data
from agribalyse_patterns import MATERIAL_PATTERNS, PROCESS_PATTERNS

PROJECT_ROOT = Path(__file__).resolve().parents[4]
OUTPUT_DIR = (
    PROJECT_ROOT / "data" / "datasets" / "pre-model"
    / "final" / "water_footprint"
)
GAPS_FILE = OUTPUT_DIR / "water_data_gaps.csv"


def load_gaps():
    """Read current water_data_gaps.csv."""
    gaps = {"material": [], "process": [], "packaging": []}
    with open(GAPS_FILE) as f:
        for row in csv.DictReader(f):
            t = row["item_type"]
            if t in gaps:
                gaps[t].append(row)
    return gaps


def append_to_csv(path: Path, new_rows: list[dict]):
    """Append rows to an existing CSV, preserving header."""
    if not new_rows:
        return
    with open(path) as f:
        fields = csv.DictReader(f).fieldnames
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writerows(new_rows)
    print(f"  Appended {len(new_rows)} rows -> {path.name}")


def search_items(gap_list, pattern_map, water_procs, water_map,
                 ref_map, item_type, label):
    """Search Agribalyse for gap items. Returns (found, remaining)."""
    found, remaining = [], []
    print(f"\n[{label}] {len(gap_list)} items")

    for gap in gap_list:
        name = gap["item_name"]
        patterns = pattern_map.get(name)
        if not patterns:
            core = name.split(",")[0].strip().replace("_", " ")
            patterns = [f"%{core}%production%", f"%{core}%"]

        r = find_best_match(
            patterns, water_procs, water_map, ref_map
        )
        if r:
            found.append({
                "name": name,
                "water_m3_per_kg": r["water_m3_per_kg"],
                "process_name": r["process_name"],
                "process_id": r["process_id"],
                "location": r["location"],
            })
            print(f"  [OK] {name} -> {r['water_m3_per_kg']} m3/kg")
        else:
            remaining.append(gap)
            print(f"  [GAP] {name}")

    return found, remaining


def main():
    print("=== Agribalyse Gap-Fill ===\n")

    print("Loading Agribalyse data...")
    all_procs, water_procs, water_map, ref_map = (
        load_agribalyse_data()
    )
    print(f"  {len(all_procs)} total, "
          f"{len(water_procs)} with water data")

    gaps = load_gaps()
    total = sum(len(v) for v in gaps.values())
    print(f"\n{total} gaps ({len(gaps['material'])} mat, "
          f"{len(gaps['process'])} proc)")

    # -- Materials --
    mat_found, mat_remain = search_items(
        gaps["material"], MATERIAL_PATTERNS,
        water_procs, water_map, ref_map, "material", "MATERIALS",
    )

    # -- Processes --
    proc_found, proc_remain = search_items(
        gaps["process"], PROCESS_PATTERNS,
        water_procs, water_map, ref_map, "process", "PROCESSING",
    )

    # -- Packaging (unlikely but check) --
    pkg_remain = list(gaps["packaging"])

    # -- Update CSVs --
    print("\nUpdating CSVs...")
    if mat_found:
        append_to_csv(
            OUTPUT_DIR / "base_materials_water.csv",
            [{"material_name": r["name"],
              "water_consumption_m3_per_kg": r["water_m3_per_kg"],
              "ecoinvent_process_name": r["process_name"],
              "ecoinvent_process_id": r["process_id"],
              "ecoinvent_location": r["location"],
              "source": "agribalyse"} for r in mat_found],
        )
    if proc_found:
        append_to_csv(
            OUTPUT_DIR / "processing_steps_water.csv",
            [{"process_name": r["name"],
              "water_consumption_m3_per_kg": r["water_m3_per_kg"],
              "ecoinvent_process_name": r["process_name"],
              "ecoinvent_process_id": r["process_id"],
              "ecoinvent_location": r["location"],
              "source": "agribalyse"} for r in proc_found],
        )

    # -- Update gaps --
    remaining = mat_remain + proc_remain + pkg_remain
    with open(GAPS_FILE, "w", newline="") as f:
        w = csv.DictWriter(
            f, fieldnames=["item_name", "item_type",
                           "searched_queries", "notes"],
        )
        w.writeheader()
        w.writerows(remaining)
    print(f"  Gaps: {total} -> {len(remaining)}")

    # -- Summary --
    filled = len(mat_found) + len(proc_found)
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Materials:  {len(mat_found)} found, "
          f"{len(mat_remain)} remaining")
    print(f"Processing: {len(proc_found)} found, "
          f"{len(proc_remain)} remaining")
    print(f"Total filled: {filled}")
    print(f"Remaining: {len(remaining)}")


if __name__ == "__main__":
    main()
