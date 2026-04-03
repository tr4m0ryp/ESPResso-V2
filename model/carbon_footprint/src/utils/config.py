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

    # -- Embedding dimensions (scaled up from water model baseline) --
    material_emb: int = 48
    step_emb: int = 32
    category_emb: int = 24
    subcategory_emb: int = 24

    # -- Encoder output dimensions --
    material_out: int = 96
    step_loc_out: int = 96
    product_out: int = 64

    # -- StepLocProxy config (Decision 4) --
    max_step_loc_tokens: int = 40
    step_loc_attn_heads: int = 4

    # -- Material-location cross-attention assignment --
    assign_dim: int = 48   # common projection dimension for cross-attention
    assign_out: int = 32   # output dimension after assignment pooling
    sinkhorn_iters: int = 3  # Sinkhorn normalization iterations

    # -- Trunk --
    trunk_hidden: int = 192
    trunk_blocks: int = 3
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
    entropy_alpha: float = 0.01  # penalize uniform attention (high entropy)
    rkd_alpha: float = 0.5  # RKD blend: 1.0=pure instance MSE, 0.0=pure relational
    # Minimum weight floor per head in UW-SO. Prevents packaging (near-
    # constant target with tiny loss) from getting starved by inverse-loss
    # weighting. 0.10 ensures each head gets at least 10% of the gradient
    # budget. Set to 0.0 to disable.
    min_head_weight: float = 0.10
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
    batch_size: int = 1024
    max_epochs: int = 100
    patience: int = 15
    warmup_epochs: int = 10
    scheduler: str = "cosine"
    num_workers: int = 4
    pin_memory: bool = True
    persistent_workers: bool = True

    # -- Differential learning rates (Decision: attention vs MLP) --
    # Attention layers (self_attn, LayerNorm, CLS params) are sensitive to LR
    # and benefit from a lower rate to prevent early oscillation. MLP layers
    # (ProductEncoder, trunk, heads) are more robust and use the base LR.
    # Ratio applied to base lr for attention parameter group.
    attn_lr_ratio: float = 0.2
    # Lower weight decay for attention to preserve rank (high decay induces
    # low-rank attention matrices -- OpenReview 2024).
    attn_weight_decay: float = 0.005
    # Embedding layers: zero weight decay (standard practice).
    emb_weight_decay: float = 0.0

    # -- Curriculum (Decision 21, revised for carbon signal strength) --
    # Carbon's geographic signal is weak (~3% of total CF from transport),
    # unlike water where AWARE factors cause 100x variance. The original
    # distribution (85% at tiers A-D) starved the model of location/step
    # signal, producing flat MAE across all tiers (model ignored those
    # features). Revised: ~45% degraded (A-C) for robustness, ~55%
    # near-complete (D-F) so the model can actually learn from geographic
    # and step features. Warmup extended from 20 to 30 epochs for smoother
    # transition. Literature (CVPR 2024 modality dropout studies) supports
    # 30-50% masking as the sweet spot for robustness without accuracy loss.
    curriculum_warmup_epochs: int = 30
    tier_probs: Dict[str, float] = field(default_factory=lambda: {
        "A": 0.10, "B": 0.15, "C": 0.20, "D": 0.20, "E": 0.20, "F": 0.15,
    })
    curriculum_start_probs: Dict[str, float] = field(default_factory=lambda: {
        "A": 0.05, "B": 0.05, "C": 0.10, "D": 0.15, "E": 0.25, "F": 0.40,
    })

    # -- LUPI schedule --
    priv_ratio: float = 0.60

    # -- Padding --
    max_materials: int = 5

    # -- Coordinate encoding (Decision 20, multi-scale) --
    coord_dim: int = 32  # 4 * coord_scales sin/cos features
    coord_scales: int = 8  # number of frequency scales for positional encoding

    # -- Distance histogram features --
    n_dist_bins: int = 16  # pairwise distance histogram bins
    n_step_pair_dists: int = 8  # top-K step-pair distances

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
            assign_dim=16,
            assign_out=8,
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
            assign_dim=24,
            assign_out=16,
            trunk_hidden=64,
            trunk_blocks=1,
            step_loc_attn_heads=2,
            batch_size=128,
            max_epochs=50,
        )

    @classmethod
    def full(cls) -> "CarbonConfig":
        """Phase 3: water-model-matching dims, GPU, baseline training."""
        return cls(
            material_emb=32,
            step_emb=24,
            category_emb=16,
            subcategory_emb=16,
            material_out=64,
            step_loc_out=64,
            product_out=48,
            assign_dim=32,
            assign_out=24,
            trunk_hidden=128,
            trunk_blocks=2,
        )

    @classmethod
    def production(cls) -> "CarbonConfig":
        """Phase 4: scaled-up dims, GPU, final production model."""
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
