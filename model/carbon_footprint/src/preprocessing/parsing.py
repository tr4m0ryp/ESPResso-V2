"""Parsing helpers for CarbonDataset: vocabularies, coordinate encoding,
haversine computation, and per-record parsing.

Split from dataset.py to stay under 300 lines per file.
"""

import json
import logging
import math
from itertools import combinations
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Vocabulary builder
# ---------------------------------------------------------------------------

def build_vocabularies(df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    """Scan all rows and build token->index mappings (0=padding/missing)."""
    materials: set = set()
    steps: set = set()
    categories: set = set()
    subcategories: set = set()

    for _, row in df.iterrows():
        categories.add(row["category_name"])
        if pd.notna(row["subcategory_name"]):
            subcategories.add(row["subcategory_name"])
        for m in json.loads(row["materials"]):
            materials.add(m)
        step_locs = json.loads(row["step_locations"])
        for step_name in step_locs:
            steps.add(step_name)

    def _make_vocab(items: set) -> Dict[str, int]:
        return {v: i + 1 for i, v in enumerate(sorted(items))}

    vocab = {
        "materials": _make_vocab(materials),
        "steps": _make_vocab(steps),
        "categories": _make_vocab(categories),
        "subcategories": _make_vocab(subcategories),
    }
    for name, v in vocab.items():
        logger.info("Vocab %s: %d entries (+ 0=pad/missing)", name, len(v))
    return vocab


# ---------------------------------------------------------------------------
# Coordinate encoding (Decision 20)
# ---------------------------------------------------------------------------

def encode_coords(lat: float, lon: float, n_scales: int = 8) -> List[float]:
    """Multi-scale sin/cos encoding with 2^k frequency bands.

    Returns 4 * n_scales features (default 32).
    """
    features: List[float] = []
    for k in range(n_scales):
        freq = 2 ** k
        lat_r = lat * math.pi / 90.0 * freq
        lon_r = lon * math.pi / 180.0 * freq
        features.extend([
            math.sin(lat_r), math.cos(lat_r),
            math.sin(lon_r), math.cos(lon_r),
        ])
    return features


# ---------------------------------------------------------------------------
# Haversine
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def compute_haversine_stats(
    unique_points: List[Tuple[float, float]],
) -> Tuple[float, float, float]:
    """All-pairs haversine distances -> (sum, max, mean). Returns zeros
    if fewer than 2 unique points."""
    if len(unique_points) < 2:
        return 0.0, 0.0, 0.0
    dists = []
    for (lat1, lon1), (lat2, lon2) in combinations(unique_points, 2):
        dists.append(haversine_km(lat1, lon1, lat2, lon2))
    return float(sum(dists)), float(max(dists)), float(np.mean(dists))


# -- Distance bin edges (km). 16 bins: [0,100), [100,500), ... [40000, inf) --
_DIST_BIN_EDGES = np.array([
    0, 100, 500, 1000, 2000, 3000, 5000, 7500,
    10000, 12500, 15000, 17500, 20000, 25000, 30000, 40000,
], dtype=np.float64)


def compute_distance_histogram(
    unique_points: List[Tuple[float, float]],
    n_bins: int = 16,
) -> np.ndarray:
    """All-pairs haversine distances binned into a normalized histogram.

    Returns array of shape [n_bins] with counts normalized by total pairs
    (or zeros if fewer than 2 points).
    """
    hist = np.zeros(n_bins, dtype=np.float64)
    if len(unique_points) < 2:
        return hist.astype(np.float32)
    dists = []
    for (lat1, lon1), (lat2, lon2) in combinations(unique_points, 2):
        dists.append(haversine_km(lat1, lon1, lat2, lon2))
    indices = np.searchsorted(_DIST_BIN_EDGES, dists, side="right") - 1
    indices = np.clip(indices, 0, n_bins - 1)
    for idx in indices:
        hist[idx] += 1.0
    total = float(len(dists))
    return (hist / total).astype(np.float32)


def compute_step_conditioned_stats(
    step_locs: Dict[str, Any],
    vocab: Dict[str, int],
    top_k: int = 8,
) -> np.ndarray:
    """Min haversine between each ordered step-type pair, top-K descending.

    For each unique pair of step types (A, B) present in the record,
    compute the minimum distance between any location of A and any location
    of B. Return the top_k largest such distances, padded with 0.
    """
    step_points: Dict[str, List[Tuple[float, float]]] = {}
    for step_name, locs in step_locs.items():
        pts = [(loc["lat"], loc["lon"]) for loc in locs]
        if pts:
            step_points[step_name] = pts

    pair_dists: List[float] = []
    step_names = sorted(step_points.keys())
    for i in range(len(step_names)):
        for j in range(i + 1, len(step_names)):
            pts_a = step_points[step_names[i]]
            pts_b = step_points[step_names[j]]
            min_d = min(
                haversine_km(a[0], a[1], b[0], b[1])
                for a in pts_a for b in pts_b
            )
            pair_dists.append(min_d)

    pair_dists.sort(reverse=True)
    result = np.zeros(top_k, dtype=np.float32)
    for i in range(min(top_k, len(pair_dists))):
        result[i] = pair_dists[i]
    return result


# ---------------------------------------------------------------------------
# Per-record parser
# ---------------------------------------------------------------------------

def parse_record(
    row: pd.Series,
    vocab: Dict[str, Dict[str, int]],
    max_materials: int,
    max_step_loc_tokens: int,
    coord_scales: int = 8,
    n_dist_bins: int = 16,
    n_step_pair_dists: int = 8,
) -> Dict[str, Any]:
    """Parse one parquet row into numpy arrays ready for tensor conversion."""
    rec: Dict[str, Any] = {}

    # -- Category / subcategory --
    rec["category_idx"] = vocab["categories"].get(row["category_name"], 0)
    subcat = row["subcategory_name"]
    rec["subcategory_idx"] = (
        vocab["subcategories"].get(subcat, 0) if pd.notna(subcat) else 0
    )

    # -- Scalars --
    rec["total_weight"] = float(row["total_weight_kg"])
    rec["total_packaging_mass"] = float(row["total_packaging_mass_kg"])
    rec["step_zscore"] = float(row["step_zscore"])
    rec["stage_coverage"] = float(row["stage_coverage"])

    # -- Materials (padded to max_materials) --
    mats = json.loads(row["materials"])
    pcts = json.loads(row["material_percentages"])
    n_mat = min(len(mats), max_materials)

    mat_ids = np.zeros(max_materials, dtype=np.int64)
    mat_pcts = np.zeros(max_materials, dtype=np.float32)
    mat_mask = np.zeros(max_materials, dtype=np.bool_)
    for i in range(n_mat):
        mat_ids[i] = vocab["materials"].get(mats[i], 0)
        mat_pcts[i] = float(pcts[i])
        mat_mask[i] = True
    rec["material_ids"] = mat_ids
    rec["material_pcts"] = mat_pcts
    rec["material_mask"] = mat_mask

    # -- Step-location tokens (padded to max_step_loc_tokens) --
    step_locs = json.loads(row["step_locations"])
    tokens_step_ids: List[int] = []
    tokens_coords: List[List[float]] = []
    unique_points: set = set()
    for step_name, locs in step_locs.items():
        step_id = vocab["steps"].get(step_name, 0)
        for loc in locs:
            lat, lon = loc["lat"], loc["lon"]
            tokens_step_ids.append(step_id)
            tokens_coords.append(encode_coords(lat, lon, coord_scales))
            unique_points.add((lat, lon))

    coord_dim = 4 * coord_scales
    n_tok = min(len(tokens_step_ids), max_step_loc_tokens)
    sl_step_ids = np.zeros(max_step_loc_tokens, dtype=np.int64)
    sl_coords = np.zeros((max_step_loc_tokens, coord_dim), dtype=np.float32)
    sl_mask = np.zeros(max_step_loc_tokens, dtype=np.bool_)
    for i in range(n_tok):
        sl_step_ids[i] = tokens_step_ids[i]
        sl_coords[i] = tokens_coords[i]
        sl_mask[i] = True
    rec["step_loc_step_ids"] = sl_step_ids
    rec["step_loc_coords"] = sl_coords
    rec["step_loc_mask"] = sl_mask

    # -- Haversine stats --
    pts_list = list(unique_points)
    h_sum, h_max, h_mean = compute_haversine_stats(pts_list)
    rec["haversine_sum"] = h_sum
    rec["haversine_max"] = h_max
    rec["haversine_mean"] = h_mean

    # -- Distance histogram and step-pair distances --
    raw_hist = compute_distance_histogram(pts_list, n_dist_bins)
    rec["distance_histogram"] = np.log1p(raw_hist).astype(np.float32)
    raw_pairs = compute_step_conditioned_stats(
        step_locs, vocab["steps"], n_step_pair_dists,
    )
    rec["step_pair_distances"] = np.log1p(raw_pairs).astype(np.float32)

    # -- Privileged features (log1p distances, mode fracs) --
    rec["priv_road_km"] = math.log1p(float(row["road_km"]))
    rec["priv_sea_km"] = math.log1p(float(row["sea_km"]))
    rec["priv_rail_km"] = math.log1p(float(row["rail_km"]))
    rec["priv_air_km"] = math.log1p(float(row["air_km"]))
    rec["priv_waterway_km"] = math.log1p(float(row["inland_waterway_km"]))
    rec["priv_total_distance_km"] = math.log1p(
        float(row["total_transport_distance_km"])
    )
    rec["priv_road_frac"] = float(row["road_frac"])
    rec["priv_sea_frac"] = float(row["sea_frac"])

    # -- Targets (raw float values, transforms applied externally) --
    rec["cf_raw_materials"] = float(row["cf_raw_materials_kg_co2e"])
    rec["cf_transport"] = float(row["cf_transport_kg_co2e"])
    rec["cf_processing"] = float(row["cf_processing_kg_co2e"])
    rec["cf_packaging"] = float(row["cf_packaging_kg_co2e"])

    # -- Metadata --
    rec["category_name"] = row["category_name"]
    rec["n_materials"] = len(mats)

    return rec
