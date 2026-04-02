"""CarbonConfig dataclass and seed utility for the carbon footprint model.

All hyperparameters from the carbon model design notes (Decisions 9, 10, 12,
15, 16, 17, 20, 21). Single source of truth -- no magic numbers elsewhere.
"""

import json
import os
import random
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List

import numpy as np
import torch


def set_seed(seed: int) -> None:
    """Set all random seeds for reproducibility."""
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    torch.cuda.manual_seed_all(seed)
    torch.backends.cudnn.deterministic = True
    torch.backends.cudnn.benchmark = False
    os.environ["PYTHONHASHSEED"] = str(seed)


@dataclass
class CarbonConfig:
    """Complete configuration for the carbon footprint model.

    Groups: vocabulary sizes, embedding dimensions, encoder output dims,
    step-location proxy, trunk, loss, training, curriculum, LUPI schedule,
    padding, data split, and paths.
    """

    # -- Vocabulary sizes (Decision 9) --
    vocab_categories: int = 48
    vocab_subcategories: int = 107
    vocab_steps: int = 56
    vocab_materials: int = 73

    # -- Embedding dimensions (Decision 10) --
    material_emb: int = 32
    step_emb: int = 24
    category_emb: int = 16
    subcategory_emb: int = 16

    # -- Encoder output dimensions (Decision 10) --
    material_out: int = 64
    step_loc_out: int = 64
    product_out: int = 48

    # -- StepLocProxy config (Decision 4) --
    max_step_loc_tokens: int = 40
    step_loc_attn_heads: int = 4

    # -- Trunk (Decision 10) --
    trunk_hidden: int = 128
    trunk_blocks: int = 2
    trunk_dropout: float = 0.20

    # -- Output heads --
    num_heads: int = 4
    head_names: List[str] = field(
        default_factory=lambda: [
            "raw_materials", "transport", "processing", "packaging",
        ]
    )

    # -- Loss (Decision 15) --
    temperature: float = 1.0
    aux_alpha: float = 0.1
    distill_peak: float = 0.1
    distill_floor: float = 0.02
    div_alpha: float = 0.02
    head_loss_types: Dict[str, str] = field(default_factory=lambda: {
        "raw_materials": "mse",
        "processing": "mse",
        "transport": "log_cosh",
        "packaging": "log_cosh",
    })

    # -- Target transforms --
    target_transform: str = "log1p_zscore"

    # -- Training --
    optimizer: str = "adamw"
    lr: float = 5e-4
    weight_decay: float = 0.01
    batch_size: int = 256
    max_epochs: int = 100
    patience: int = 15
    warmup_epochs: int = 5
    scheduler: str = "cosine"
    num_workers: int = 4
    pin_memory: bool = True
    persistent_workers: bool = True

    # -- Curriculum (Decision 21) --
    curriculum_warmup_epochs: int = 20
    tier_probs: Dict[str, float] = field(default_factory=lambda: {
        "A": 0.25, "B": 0.20, "C": 0.20, "D": 0.20, "E": 0.10, "F": 0.05,
    })
    curriculum_start_probs: Dict[str, float] = field(default_factory=lambda: {
        "A": 0.10, "B": 0.10, "C": 0.10, "D": 0.15, "E": 0.20, "F": 0.35,
    })

    # -- LUPI schedule --
    priv_ratio: float = 0.60

    # -- Padding --
    max_materials: int = 5

    # -- Coordinate encoding (Decision 20) --
    coord_dim: int = 4  # sin(lat), cos(lat), sin(lon), cos(lon)

    # -- Data split --
    split_ratios: List[float] = field(
        default_factory=lambda: [0.70, 0.15, 0.15]
    )
    split_seed: int = 42

    # -- Global seed --
    seed: int = 42

    # -- Smoke test --
    smoke_test_rows: int = 100
    smoke_test_batches: int = 2

    # -- Viability check --
    canary_epochs: int = 5

    # -- Checkpointing --
    checkpoint_interval_epochs: int = 5

    # -- Paths --
    data_dir: str = "model/data"
    data_file: str = "carbon_footprint.parquet"
    checkpoint_dir: str = "checkpoints/carbon_footprint"
    runs_log: str = "runs.jsonl"

    # -- Preset classmethods --

    @classmethod
    def smoke(cls) -> "CarbonConfig":
        """Phase 1: tiny dims, CPU, fast iteration."""
        return cls(
            material_emb=8,
            step_emb=8,
            category_emb=8,
            subcategory_emb=8,
            material_out=32,
            step_loc_out=32,
            product_out=24,
            trunk_hidden=32,
            trunk_blocks=1,
            step_loc_attn_heads=1,
            batch_size=32,
            max_epochs=3,
        )

    @classmethod
    def dev(cls) -> "CarbonConfig":
        """Phase 2: medium dims, GPU, hyperparameter search."""
        return cls(
            material_emb=16,
            step_emb=16,
            category_emb=12,
            subcategory_emb=12,
            material_out=48,
            step_loc_out=48,
            product_out=32,
            trunk_hidden=64,
            trunk_blocks=1,
            step_loc_attn_heads=2,
            batch_size=128,
            max_epochs=50,
        )

    @classmethod
    def full(cls) -> "CarbonConfig":
        """Phase 3: production dims, GPU, final training."""
        return cls()

    # -- Serialization --

    def to_json(self) -> str:
        """Serialize config to JSON string for experiment logging."""
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> "CarbonConfig":
        """Deserialize config from a JSON string."""
        data = json.loads(json_str)
        return cls(**data)

    @classmethod
    def from_file(cls, path: str) -> "CarbonConfig":
        """Load config from a JSON file."""
        with open(path, "r") as f:
            return cls.from_json(f.read())

    def save(self, path: str) -> None:
        """Save config to a JSON file."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.to_json())

    @property
    def device(self) -> torch.device:
        """Device-agnostic: GPU if available, else CPU."""
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")

    @property
    def data_path(self) -> Path:
        """Full path to the training parquet file."""
        return Path(self.data_dir) / self.data_file
