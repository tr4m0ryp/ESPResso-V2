"""
Configuration for Layer 3: Transport Scenario Generator (V2)

Per-leg transport scenario generation with coordinate-based routing,
semantic validation, and statistical quality monitoring.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import FrozenSet, List

from data.data_generation.shared.paths import PipelinePaths


# Module-level constant: the five valid transport modes for any leg.
ALLOWED_TRANSPORT_MODES: FrozenSet[str] = frozenset({
    "road", "rail", "sea", "air", "inland_waterway",
})


@dataclass
class Layer3Config:
    """Configuration settings for Layer 3 V2 generation."""

    # -- Centralized path resolution -----------------------------------

    _paths: PipelinePaths = field(
        default_factory=PipelinePaths, repr=False
    )

    @property
    def project_root(self) -> Path:
        """Backward-compatible project root accessor."""
        return self._paths.root

    # -- API key configuration -----------------------------------------

    api_key_env_vars: List[str] = field(
        default_factory=lambda: ["UVA_API_KEY"]
    )

    # -- Input / output paths ------------------------------------------

    @property
    def layer2_output_path(self) -> Path:
        """Cleaned Layer 2 dataset used as input."""
        return self._paths.layer2_output

    @property
    def output_dir(self) -> Path:
        return self._paths.layer_output_dir(3)

    @property
    def output_path(self) -> Path:
        return self._paths.layer3_output

    @property
    def checkpoint_dir(self) -> Path:
        return self.output_dir / "checkpoints"

    @property
    def system_prompts_dir(self) -> Path:
        """Path to prompts/system/ directory."""
        return Path(__file__).parent.parent / "prompts" / "system"

    # -- API configuration ---------------------------------------------

    api_base_url: str = "http://localhost:3000/v1"
    api_model: str = "claude-sonnet-4.6"
    api_key_env_var: str = "UVA_API_KEY"

    # Provider switching (nvidia or uva)
    api_provider: str = field(
        default_factory=lambda: os.environ.get("API_PROVIDER", "uva")
    )

    # -- Generation parameters -----------------------------------------

    temperature: float = 0.7
    top_p: float = 0.9
    max_tokens: int = 2000

    # -- Post-init: env-driven fields ----------------------------------

    def __post_init__(self):
        """Initialize fields that depend on environment variables."""
        # Provider-specific API overrides
        if self.api_provider == "nvidia":
            nvidia_model = os.environ.get(
                "NVIDIA_MODEL",
                "nvidia/llama-3.1-nemotron-ultra-253b-v1",
            )
            object.__setattr__(
                self, "api_base_url",
                "https://integrate.api.nvidia.com/v1",
            )
            object.__setattr__(self, "api_model", nvidia_model)
            object.__setattr__(self, "api_key_env_var", "NVIDIA_API_KEY")

        # Batch and checkpoint settings
        object.__setattr__(
            self, "batch_size",
            int(os.getenv("LAYER3_BATCH_SIZE", "50")),
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

        # -- Deterministic validation thresholds -----------------------
        object.__setattr__(
            self, "min_leg_distance_km",
            float(os.getenv("LAYER3_MIN_LEG_DISTANCE", "1")),
        )
        object.__setattr__(
            self, "max_leg_distance_km",
            float(os.getenv("LAYER3_MAX_LEG_DISTANCE", "25000")),
        )
        object.__setattr__(
            self, "min_reasoning_length",
            int(os.getenv("LAYER3_MIN_REASONING_LEN", "50")),
        )
        object.__setattr__(
            self, "coordinate_decimal_places", 2,
        )

        # -- Semantic validation config --------------------------------
        object.__setattr__(
            self, "semantic_accept_threshold",
            float(os.getenv("LAYER3_SEMANTIC_ACCEPT", "0.80")),
        )
        object.__setattr__(
            self, "semantic_review_threshold",
            float(os.getenv("LAYER3_SEMANTIC_REVIEW", "0.60")),
        )
        object.__setattr__(
            self, "semantic_max_retries",
            int(os.getenv("LAYER3_SEMANTIC_RETRIES", "2")),
        )

        # -- Statistical validation config -----------------------------
        object.__setattr__(
            self, "location_diversity_threshold", 0.30,
        )
        object.__setattr__(
            self, "distance_outlier_zscore", 3.0,
        )
        object.__setattr__(
            self, "mode_max_single_percentage", 0.80,
        )

    # -- API key management --------------------------------------------

    @property
    def api_key(self) -> str:
        """Get primary API key from environment variable."""
        if self.api_provider != "nvidia":
            return os.environ.get("UVA_API_KEY", "uva-local")
        key = os.environ.get(self.api_key_env_var)
        if not key or key == "your_nvidia_api_key_here":
            return ""
        return key

    @property
    def api_keys(self) -> List[str]:
        """Get all available API keys from environment variables."""
        if self.api_provider != "nvidia":
            return [os.environ.get("UVA_API_KEY", "uva-local")]
        keys = []
        for env_var in self.api_key_env_vars:
            key = os.environ.get(env_var)
            if key and key not in [
                "YOUR_SECOND_API_KEY_HERE",
                "YOUR_THIRD_API_KEY_HERE",
                "your_nvidia_api_key_here",
            ]:
                keys.append(key)
        return keys

    @property
    def rate_limit_per_key(self) -> int:
        """Get rate limit per API key from environment or default."""
        return int(os.environ.get("RATE_LIMIT_PER_KEY", "42"))

    @property
    def total_rate_limit(self) -> int:
        """Total rate limit across all keys (capped at 400 req/min)."""
        return min(len(self.api_keys) * self.rate_limit_per_key, 400)

    @property
    def parallel_workers(self) -> int:
        """Get number of parallel workers from environment or default."""
        if self.api_provider == "nvidia":
            return int(os.environ.get("PARALLEL_WORKERS", "80"))
        return int(os.environ.get("UVA_PARALLEL_WORKERS", "25"))

    @property
    def effective_rate_limit(self) -> int:
        """Get effective rate limit, accounting for UVA provider."""
        if self.api_provider == "nvidia":
            return self.total_rate_limit
        return int(os.environ.get("UVA_RATE_LIMIT", "600"))

    def has_api_key(self) -> bool:
        """Check if API key is available."""
        if self.api_provider != "nvidia":
            return True
        key = os.environ.get(self.api_key_env_var)
        return bool(key) and key != "your_nvidia_api_key_here"

    # -- Directory helpers ---------------------------------------------

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
