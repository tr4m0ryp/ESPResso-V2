"""
Configuration for Layer 4: Packaging Configuration Generator (V2)
"""

import os
from dataclasses import dataclass, field
from pathlib import Path

from data.data_generation.shared.paths import PipelinePaths


@dataclass
class Layer4Config:
    """Configuration settings for Layer 4 V2 generation."""

    # -- Centralized path resolution -----------------------------------

    _paths: PipelinePaths = field(
        default_factory=PipelinePaths, repr=False
    )

    @property
    def project_root(self) -> Path:
        """Backward-compatible project root accessor."""
        return self._paths.root

    # -- API configuration ---------------------------------------------

    api_key_env_var: str = "UVA_API_KEY"
    api_model: str = "claude-sonnet-4.6"
    api_base_url: str = "http://localhost:3000/v1"
    temperature: float = 0.3
    max_tokens: int = 3000

    # -- Post-init: env-driven fields ----------------------------------

    def __post_init__(self):
        """Initialize fields that depend on environment variables."""
        # Load .env.uva from data/data_generation/ (same as other layers)
        env_path = (
            Path(__file__).resolve().parent.parent.parent / ".env.uva"
        )
        if env_path.exists():
            with open(env_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ.setdefault(key, value)

        # Processing settings
        object.__setattr__(
            self, "batch_size",
            int(os.getenv("LAYER4_BATCH_SIZE", "50")),
        )
        object.__setattr__(
            self, "checkpoint_interval",
            int(os.getenv("CHECKPOINT_INTERVAL", "5000")),
        )
        object.__setattr__(
            self, "max_retries",
            int(os.getenv("MAX_RETRIES", "3")),
        )
        object.__setattr__(
            self, "retry_delay",
            float(os.getenv("RETRY_DELAY", "2.0")),
        )

        # Batch / parallel settings
        object.__setattr__(
            self, "products_per_batch",
            int(os.getenv("LAYER4_PRODUCTS_PER_BATCH", "10")),
        )

        # Validation thresholds (absolute mass bounds, not ratios)
        object.__setattr__(
            self, "min_packaging_mass_kg",
            float(os.getenv("LAYER4_MIN_PKG_MASS", "0.005")),
        )
        object.__setattr__(
            self, "max_packaging_mass_kg",
            float(os.getenv("LAYER4_MAX_PKG_MASS", "0.500")),
        )
        object.__setattr__(
            self, "max_footwear_packaging_mass_kg",
            float(os.getenv("LAYER4_MAX_FW_PKG_MASS", "1.000")),
        )
        object.__setattr__(
            self, "min_reasoning_length",
            int(os.getenv("LAYER4_MIN_REASONING", "20")),
        )

    # -- Parallel processing -------------------------------------------

    @property
    def parallel_workers(self) -> int:
        return int(os.getenv("LAYER4_PARALLEL_WORKERS", "1"))

    @property
    def effective_rate_limit(self) -> int:
        return int(os.getenv("LAYER4_RATE_LIMIT", "600"))

    # -- Input / output paths ------------------------------------------

    @property
    def layer3_output_path(self) -> Path:
        return self._paths.layer3_output

    @property
    def output_dir(self) -> Path:
        return self._paths.layer_output_dir(4)

    @property
    def output_path(self) -> Path:
        return self._paths.layer4_output

    @property
    def checkpoint_dir(self) -> Path:
        return self.output_dir / "checkpoints"

    # -- API key management --------------------------------------------

    @property
    def api_key(self) -> str:
        """Get API key from environment variable (defaults to 'uva-local')."""
        return os.environ.get(self.api_key_env_var, "uva-local")

    def has_api_key(self) -> bool:
        """Return True if the API key is available and non-empty."""
        return bool(self.api_key)

    # -- Directory helpers ---------------------------------------------

    def ensure_directories(self) -> None:
        """Create output and checkpoint directories if they do not exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
