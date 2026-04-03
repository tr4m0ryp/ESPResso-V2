"""WA1Config dataclass and seed utility for the water footprint model.

All hyperparameters from research/water-model-design.md (WA1 lean variant)
and notes/water-model-implementation.md (decisions D4, D8). Single source
of truth -- no magic numbers elsewhere in the codebase.
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
class WA1Config:
    """Complete configuration for the WA1 water footprint model.

    Groups: vocabulary sizes, embedding dimensions, encoder settings,
    cross-attention, confidence gate, trunk, output heads, loss,
    target transforms, training, masking tiers, padding, and paths.
    """

    # -- Vocabulary sizes (D8) --
    vocab_materials: int = 73
    vocab_steps: int = 56
    vocab_categories: int = 48
    vocab_subcategories: int = 107
    vocab_countries: int = 75
    vocab_packaging: int = 4

    # -- Embedding dimensions (D8) --
    embed_dim_material: int = 32
    embed_dim_step: int = 24
    embed_dim_category: int = 16
    embed_dim_subcategory: int = 16
    embed_dim_country: int = 32

    # -- Encoder output dimension (D8) --
    encoder_output_dim: int = 64
    product_enc_output_dim: int = 48
    pkg_enc_output_dim: int = 32
    mat_self_attn_heads: int = 2

    # -- Cross-attention (D8) --
    cross_attn_layers: int = 2
    cross_attn_heads: int = 4
    cross_attn_d_k: int = 16
    cross_attn_d_model: int = 64
    cross_attn_dropout: float = 0.15

    # -- Confidence gate (D8) --
    gate_hidden_dim: int = 16
    # Input: encoder_output_dim (64), output: 1 (sigmoid)

    # -- Shared trunk (D8) --
    trunk_input_dim: int = 208
    trunk_hidden_dim: int = 128
    trunk_layers: int = 2
    trunk_dropout: float = 0.20

    # -- Output heads (D8) --
    num_heads: int = 3
    head_input_dim: int = 128
    head_hidden_dim: int = 64
    head_output_dim: int = 1
    # Heads: raw_materials, processing, packaging

    # -- Loss (D8) --
    loss_raw_type: str = "mse"
    loss_processing_type: str = "mse"
    loss_packaging_type: str = "huber"
    huber_delta: float = 1.5

    # -- Target transforms (D8) --
    target_transform: str = "log1p_zscore"

    # -- Training (D8) --
    optimizer: str = "adamw"
    learning_rate: float = 5e-4
    weight_decay: float = 0.01
    batch_size: int = 1024
    max_epochs: int = 100
    patience: int = 15
    warmup_epochs: int = 5
    scheduler: str = "cosine"
    num_workers: int = 4
    pin_memory: bool = True
    persistent_workers: bool = True

    # -- Auxiliary weight prediction (D1) --
    aux_weight_alpha: float = 0.3

    # -- Curriculum learning (D1) --
    curriculum_warmup_epochs: int = 20
    curriculum_start_probs: Dict[str, float] = field(default_factory=lambda: {
        "A": 0.10, "B": 0.10, "C": 0.10, "D": 0.15, "E": 0.20, "F": 0.35,
    })

    # -- Masking tiers (D8, F11) --
    tier_probs: Dict[str, float] = field(default_factory=lambda: {
        "A": 0.35,
        "B": 0.25,
        "C": 0.15,
        "D": 0.10,
        "E": 0.10,
        "F": 0.05,
    })
    subcategory_mask_prob: float = 0.15
    linked_mode_prob: float = 0.50

    # -- Padding sizes (D3) --
    max_materials: int = 5
    max_steps: int = 27
    max_locations: int = 8
    max_packaging: int = 3

    # -- Coordinate encoding --
    coord_dim: int = 2  # lat, lon (raw or sin/cos encoded)

    # -- Data split (D5) --
    split_ratios: List[float] = field(
        default_factory=lambda: [0.70, 0.15, 0.15]
    )
    split_seed: int = 42

    # -- Global seed --
    seed: int = 42

    # -- Smoke test (D6) --
    smoke_test_rows: int = 100
    smoke_test_batches: int = 2

    # -- Viability check (D6) --
    canary_epochs: int = 5

    # -- Checkpointing --
    checkpoint_interval_epochs: int = 5

    # -- Paths --
    data_dir: str = "model/data"
    data_file: str = "water_footprint.csv"
    checkpoint_dir: str = "checkpoints/water_footprint"
    runs_log: str = "runs.jsonl"

    @classmethod
    def production(cls) -> "WA1Config":
        """Scaled-up production model: 1.5x embeddings, wider trunk."""
        return cls(
            embed_dim_material=48,
            embed_dim_step=32,
            embed_dim_category=24,
            embed_dim_subcategory=24,
            embed_dim_country=48,
            encoder_output_dim=96,
            product_enc_output_dim=64,
            pkg_enc_output_dim=48,
            cross_attn_d_model=96,
            cross_attn_d_k=24,
            gate_hidden_dim=24,
            # trunk_input: mat(96) + step(96) + product(64) + pkg(48) = 304
            trunk_input_dim=304,
            trunk_hidden_dim=192,
            trunk_layers=3,
            head_input_dim=192,
            head_hidden_dim=96,
        )

    def to_json(self) -> str:
        """Serialize config to JSON string for experiment logging."""
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, json_str: str) -> "WA1Config":
        """Deserialize config from a JSON string."""
        data = json.loads(json_str)
        return cls(**data)

    @classmethod
    def from_file(cls, path: str) -> "WA1Config":
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
        """Full path to the training CSV."""
        return Path(self.data_dir) / self.data_file
