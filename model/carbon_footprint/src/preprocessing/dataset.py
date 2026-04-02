"""CarbonDataset -- loads parquet with JSON columns into padded tensors.

Vocabularies: material->idx, step->idx, category->idx, subcategory->idx
(0 = padding/missing in all cases). All JSON parsing and coordinate encoding
happen at init time; __getitem__ only converts pre-parsed numpy to tensors.

See tasks/carbon-model/rules.md for the tensor dict specification.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import torch
from torch.utils.data import Dataset

from model.carbon_footprint.src.preprocessing.parsing import (
    build_vocabularies,
    parse_record,
)
from model.carbon_footprint.src.utils.config import CarbonConfig

logger = logging.getLogger(__name__)


class CarbonDataset(Dataset):
    """PyTorch Dataset for the carbon footprint parquet with JSON columns.

    Parses all records once at init time. __getitem__ converts to tensors.
    """

    def __init__(
        self,
        data_path: str,
        vocab: Optional[Dict[str, Dict[str, int]]] = None,
        config: Optional[CarbonConfig] = None,
    ) -> None:
        if config is None:
            config = CarbonConfig()

        path = Path(data_path)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file not found: {data_path}")

        df = pd.read_parquet(data_path)
        logger.info("Loaded %d records from %s", len(df), data_path)

        if vocab is None:
            vocab = build_vocabularies(df)
        self.vocab = vocab
        self.config = config

        max_mat = config.max_materials
        max_sl = config.max_step_loc_tokens

        self.records: List[Dict[str, Any]] = []
        for idx in range(len(df)):
            self.records.append(
                parse_record(df.iloc[idx], vocab, max_mat, max_sl)
            )
        logger.info("Parsed %d records", len(self.records))

    def __len__(self) -> int:
        return len(self.records)

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        r = self.records[idx]
        return {
            # Product features
            "category_idx": torch.tensor(r["category_idx"], dtype=torch.long),
            "subcategory_idx": torch.tensor(
                r["subcategory_idx"], dtype=torch.long
            ),
            "total_weight": torch.tensor(
                r["total_weight"], dtype=torch.float32
            ),
            "total_packaging_mass": torch.tensor(
                r["total_packaging_mass"], dtype=torch.float32
            ),
            "step_zscore": torch.tensor(
                r["step_zscore"], dtype=torch.float32
            ),
            "stage_coverage": torch.tensor(
                r["stage_coverage"], dtype=torch.float32
            ),
            # Materials (padded to max_materials)
            "material_ids": torch.from_numpy(r["material_ids"]),
            "material_pcts": torch.from_numpy(r["material_pcts"]),
            "material_mask": torch.from_numpy(r["material_mask"]),
            # Step-location tokens (padded to max_step_loc_tokens)
            "step_loc_step_ids": torch.from_numpy(r["step_loc_step_ids"]),
            "step_loc_coords": torch.from_numpy(r["step_loc_coords"]),
            "step_loc_mask": torch.from_numpy(r["step_loc_mask"]),
            # Haversine stats
            "haversine_sum": torch.tensor(
                r["haversine_sum"], dtype=torch.float32
            ),
            "haversine_max": torch.tensor(
                r["haversine_max"], dtype=torch.float32
            ),
            "haversine_mean": torch.tensor(
                r["haversine_mean"], dtype=torch.float32
            ),
            # Privileged features
            "priv_road_km": torch.tensor(
                r["priv_road_km"], dtype=torch.float32
            ),
            "priv_sea_km": torch.tensor(
                r["priv_sea_km"], dtype=torch.float32
            ),
            "priv_rail_km": torch.tensor(
                r["priv_rail_km"], dtype=torch.float32
            ),
            "priv_air_km": torch.tensor(
                r["priv_air_km"], dtype=torch.float32
            ),
            "priv_waterway_km": torch.tensor(
                r["priv_waterway_km"], dtype=torch.float32
            ),
            "priv_total_distance_km": torch.tensor(
                r["priv_total_distance_km"], dtype=torch.float32
            ),
            "priv_road_frac": torch.tensor(
                r["priv_road_frac"], dtype=torch.float32
            ),
            "priv_sea_frac": torch.tensor(
                r["priv_sea_frac"], dtype=torch.float32
            ),
            # Targets (raw values; transforms applied externally)
            "cf_raw_materials": torch.tensor(
                r["cf_raw_materials"], dtype=torch.float32
            ),
            "cf_transport": torch.tensor(
                r["cf_transport"], dtype=torch.float32
            ),
            "cf_processing": torch.tensor(
                r["cf_processing"], dtype=torch.float32
            ),
            "cf_packaging": torch.tensor(
                r["cf_packaging"], dtype=torch.float32
            ),
        }

    @property
    def category_names(self) -> List[str]:
        """Category name per record, for stratified splitting."""
        return [r["category_name"] for r in self.records]

    @property
    def n_materials_list(self) -> List[int]:
        """Material count per record, for stratified splitting."""
        return [r["n_materials"] for r in self.records]
