"""WaterFootprintDataset -- parses CSV with JSON columns into padded tensors.

Implements tensor layout D3 from notes/water-model-implementation.md.
Vocabularies: material->idx, step->idx, category->idx, subcategory->idx,
country->idx, packaging->idx (0 = padding/missing in all cases).
"""

import json
import logging
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

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
    countries: set = set()
    packaging_cats: set = set()

    for _, row in df.iterrows():
        categories.add(row["category_name"])
        if pd.notna(row["subcategory_name"]):
            subcategories.add(row["subcategory_name"])
        for m in json.loads(row["materials"]):
            materials.add(m)
        for s in json.loads(row["preprocessing_steps"]):
            steps.add(s)
        for j in json.loads(row["material_journeys"]):
            countries.add(j["origin_country"])
            countries.add(j["processing_country"])
        for p in json.loads(row["packaging_categories"]):
            packaging_cats.add(p)

    def _make_vocab(items: set) -> Dict[str, int]:
        return {v: i + 1 for i, v in enumerate(sorted(items))}

    vocab = {
        "material": _make_vocab(materials),
        "step": _make_vocab(steps),
        "category": _make_vocab(categories),
        "subcategory": _make_vocab(subcategories),
        "country": _make_vocab(countries),
        "packaging": _make_vocab(packaging_cats),
    }
    for name, v in vocab.items():
        logger.info("Vocab %s: %d entries (+ 0=pad/missing)", name, len(v))
    return vocab


# ---------------------------------------------------------------------------
# Coordinate encoding
# ---------------------------------------------------------------------------

def encode_coords(lat: float, lon: float) -> List[float]:
    """Sin/cos encoding: [sin(pi*lat/90), cos(pi*lat/90),
    sin(pi*lon/180), cos(pi*lon/180)]."""
    return [
        math.sin(math.pi * lat / 90.0),
        math.cos(math.pi * lat / 90.0),
        math.sin(math.pi * lon / 180.0),
        math.cos(math.pi * lon / 180.0),
    ]


# ---------------------------------------------------------------------------
# Record pre-parser (called once in __init__, not per __getitem__)
# ---------------------------------------------------------------------------

def _parse_record(row: pd.Series, vocab: Dict[str, Dict[str, int]],
                  max_mat: int, max_step: int, max_loc: int,
                  max_pkg: int) -> Dict[str, Any]:
    """Parse a single CSV row into numpy arrays ready for tensor conversion."""
    rec: Dict[str, Any] = {}

    # -- Fixed scalars --
    rec["category_idx"] = vocab["category"].get(row["category_name"], 0)
    subcat = row["subcategory_name"]
    rec["subcategory_idx"] = (
        vocab["subcategory"].get(subcat, 0) if pd.notna(subcat) else 0
    )
    rec["total_weight"] = float(row["total_weight_kg"])
    rec["total_packaging_mass"] = float(row["total_packaging_mass_kg"])

    # -- Materials (padded to max_mat) --
    mats = json.loads(row["materials"])
    weights = json.loads(row["material_weights_kg"])
    pcts = json.loads(row["material_percentages"])
    n_mat = min(len(mats), max_mat)

    mat_ids = np.zeros(max_mat, dtype=np.int64)
    mat_wts = np.zeros(max_mat, dtype=np.float32)
    mat_pcts = np.zeros(max_mat, dtype=np.float32)
    mat_mask = np.zeros(max_mat, dtype=np.bool_)
    for i in range(n_mat):
        mat_ids[i] = vocab["material"].get(mats[i], 0)
        mat_wts[i] = float(weights[i])
        mat_pcts[i] = float(pcts[i])
        mat_mask[i] = True
    rec["material_ids"] = mat_ids
    rec["material_weights"] = mat_wts
    rec["material_pcts"] = mat_pcts
    rec["material_mask"] = mat_mask

    # -- Preprocessing steps (padded to max_step) --
    raw_steps = json.loads(row["preprocessing_steps"])
    n_step = min(len(raw_steps), max_step)
    step_ids = np.zeros(max_step, dtype=np.int64)
    step_mask = np.zeros(max_step, dtype=np.bool_)
    for i in range(n_step):
        step_ids[i] = vocab["step"].get(raw_steps[i], 0)
        step_mask[i] = True
    rec["step_ids"] = step_ids
    rec["step_mask"] = step_mask

    # -- Material journeys (padded to max_mat) --
    journeys = json.loads(row["material_journeys"])
    n_j = min(len(journeys), max_mat)
    j_origin_ids = np.zeros(max_mat, dtype=np.int64)
    j_origin_coords = np.zeros((max_mat, 4), dtype=np.float32)
    j_proc_ids = np.zeros(max_mat, dtype=np.int64)
    j_proc_coords = np.zeros((max_mat, 4), dtype=np.float32)
    for i in range(n_j):
        j = journeys[i]
        j_origin_ids[i] = vocab["country"].get(j["origin_country"], 0)
        j_origin_coords[i] = encode_coords(j["origin_lat"], j["origin_lon"])
        j_proc_ids[i] = vocab["country"].get(j["processing_country"], 0)
        j_proc_coords[i] = encode_coords(
            j["processing_lat"], j["processing_lon"]
        )
    rec["journey_origin_loc_ids"] = j_origin_ids
    rec["journey_origin_coords"] = j_origin_coords
    rec["journey_proc_loc_ids"] = j_proc_ids
    rec["journey_proc_coords"] = j_proc_coords

    # -- Location set: unique countries from journeys (padded to max_loc) --
    loc_set: dict = {}  # country -> (lat, lon), preserves insertion order
    for j in journeys:
        oc = j["origin_country"]
        if oc not in loc_set:
            loc_set[oc] = (j["origin_lat"], j["origin_lon"])
        pc = j["processing_country"]
        if pc not in loc_set:
            loc_set[pc] = (j["processing_lat"], j["processing_lon"])
    loc_ids = np.zeros(max_loc, dtype=np.int64)
    loc_coords = np.zeros((max_loc, 4), dtype=np.float32)
    loc_mask = np.zeros(max_loc, dtype=np.bool_)
    for i, (country, (lat, lon)) in enumerate(loc_set.items()):
        if i >= max_loc:
            break
        loc_ids[i] = vocab["country"].get(country, 0)
        loc_coords[i] = encode_coords(lat, lon)
        loc_mask[i] = True
    rec["location_ids"] = loc_ids
    rec["location_coords"] = loc_coords
    rec["location_mask"] = loc_mask

    # -- Packaging categories (fixed=max_pkg) --
    pkg_cats = json.loads(row["packaging_categories"])
    pkg_ids = np.zeros(max_pkg, dtype=np.int64)
    for i in range(min(len(pkg_cats), max_pkg)):
        pkg_ids[i] = vocab["packaging"].get(pkg_cats[i], 0)
    rec["pkg_ids"] = pkg_ids

    # -- Targets (raw float values, transforms applied later) --
    rec["wf_raw"] = float(row["wf_raw_materials_m3_world_eq"])
    rec["wf_processing"] = float(row["wf_processing_m3_world_eq"])
    rec["wf_packaging"] = float(row["wf_packaging_m3_world_eq"])

    # -- Metadata (for stratified splitting) --
    rec["category_name"] = row["category_name"]

    return rec


# ---------------------------------------------------------------------------
# Dataset
# ---------------------------------------------------------------------------

class WaterFootprintDataset(Dataset):
    """PyTorch Dataset for the water footprint CSV with JSON columns.

    Parses all records once at init time. __getitem__ converts to tensors.
    """

    def __init__(self, data_path: str,
                 vocab: Optional[Dict[str, Dict[str, int]]] = None,
                 max_materials: int = 5, max_steps: int = 27,
                 max_locations: int = 8, max_packaging: int = 3) -> None:
        df = pd.read_csv(data_path)
        logger.info("Loaded %d records from %s", len(df), data_path)

        if vocab is None:
            vocab = build_vocabularies(df)
        self.vocab = vocab
        self.max_materials = max_materials
        self.max_steps = max_steps
        self.max_locations = max_locations
        self.max_packaging = max_packaging

        self.records: List[Dict[str, Any]] = []
        for idx in range(len(df)):
            self.records.append(
                _parse_record(
                    df.iloc[idx], vocab,
                    max_materials, max_steps, max_locations, max_packaging,
                )
            )
        logger.info("Parsed %d records", len(self.records))

    def __len__(self) -> int:
        return len(self.records)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        r = self.records[idx]
        return {
            "category_idx": torch.tensor(r["category_idx"], dtype=torch.long),
            "subcategory_idx": torch.tensor(r["subcategory_idx"], dtype=torch.long),
            "total_weight": torch.tensor(r["total_weight"], dtype=torch.float32),
            "total_packaging_mass": torch.tensor(r["total_packaging_mass"], dtype=torch.float32),
            "material_ids": torch.from_numpy(r["material_ids"]),
            "material_weights": torch.from_numpy(r["material_weights"]),
            "material_pcts": torch.from_numpy(r["material_pcts"]),
            "material_mask": torch.from_numpy(r["material_mask"]),
            "step_ids": torch.from_numpy(r["step_ids"]),
            "step_mask": torch.from_numpy(r["step_mask"]),
            "location_ids": torch.from_numpy(r["location_ids"]),
            "location_coords": torch.from_numpy(r["location_coords"]),
            "location_mask": torch.from_numpy(r["location_mask"]),
            "journey_origin_loc_ids": torch.from_numpy(r["journey_origin_loc_ids"]),
            "journey_origin_coords": torch.from_numpy(r["journey_origin_coords"]),
            "journey_proc_loc_ids": torch.from_numpy(r["journey_proc_loc_ids"]),
            "journey_proc_coords": torch.from_numpy(r["journey_proc_coords"]),
            "pkg_ids": torch.from_numpy(r["pkg_ids"]),
            "wf_raw": torch.tensor(r["wf_raw"], dtype=torch.float32),
            "wf_processing": torch.tensor(r["wf_processing"], dtype=torch.float32),
            "wf_packaging": torch.tensor(r["wf_packaging"], dtype=torch.float32),
        }

    @property
    def category_names(self) -> List[str]:
        """Category name per record, for stratified splitting."""
        return [r["category_name"] for r in self.records]
