"""
Configuration for Layer 5: Cross-Layer Coherence Checker (V2)

Five-stage quality gate: passport verification, cross-layer coherence
(LLM, 50 records/batch), statistical quality, sampled reward scoring
(1-5% sample), and final decision.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

from data.data_generation.shared.paths import PipelinePaths


@dataclass
class Layer5Config:
    """Configuration settings for Layer 5 validation (V2)."""

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

    # Internal override for output directory
    _output_dir_override: Optional[Path] = None

    # -- Input dataset paths -------------------------------------------

    @property
    def layer1_output_path(self) -> Path:
        return self._paths.layer1_output

    @property
    def layer2_output_path(self) -> Path:
        return self._paths.layer2_output

    @property
    def layer3_output_path(self) -> Path:
        return self._paths.layer3_output

    @property
    def layer4_output_path(self) -> Path:
        return self._paths.layer4_output

    @property
    def complete_dataset_path(self) -> Path:
        """Path to complete merged dataset from Layer 4."""
        return self._paths.layer4_output

    # Flag to use complete dataset instead of merging separate layer files
    use_complete_dataset: bool = True

    # -- Reference data paths ------------------------------------------

    @property
    def reference_materials_path(self) -> Path:
        return self._paths.materials_path

    @property
    def reference_processing_path(self) -> Path:
        return self._paths.processing_steps_path

    @property
    def reference_material_processing_path(self) -> Path:
        return self._paths.material_processing_path

    # -- Output paths --------------------------------------------------

    @property
    def output_dir(self) -> Path:
        if self._output_dir_override:
            return self._output_dir_override
        return self._paths.layer_output_dir(5)

    @property
    def accepted_output_path(self) -> Path:
        return self.output_dir / "layer_5_validated.parquet"

    @property
    def review_queue_path(self) -> Path:
        return self.output_dir / "layer_5_review_queue.parquet"

    @property
    def rejected_output_path(self) -> Path:
        return self.output_dir / "layer_5_rejected.parquet"

    @property
    def validation_report_path(self) -> Path:
        return self.output_dir / "validation_report.json"

    @property
    def checkpoint_dir(self) -> Path:
        return self.output_dir / "checkpoints" / "layer_5"

    # -- API configuration ---------------------------------------------
    # Single model for both coherence and reward scoring.
    # Token limit sized for batch processing of 50 records.

    api_base_url: str = "http://localhost:3000/v1"
    api_model_instruct: str = "claude-sonnet-4.5"
    api_key_env_var: str = "UVA_API_KEY"

    # Provider switching (nvidia or uva)
    api_provider: str = field(
        default_factory=lambda: os.environ.get("API_PROVIDER", "uva")
    )

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
            object.__setattr__(self, "api_model_instruct", nvidia_model)
            object.__setattr__(self, "api_key_env_var", "NVIDIA_API_KEY")

        # Batch and checkpoint settings
        object.__setattr__(
            self, "batch_size",
            int(os.getenv("LAYER5_BATCH_SIZE", "1000")),
        )
        object.__setattr__(
            self, "checkpoint_interval",
            int(os.getenv("CHECKPOINT_INTERVAL", "10000")),
        )
        object.__setattr__(
            self, "max_retries",
            int(os.getenv("MAX_RETRIES", "3")),
        )
        object.__setattr__(
            self, "retry_delay",
            float(os.getenv("RETRY_DELAY", "2.0")),
        )

        # API generation settings
        object.__setattr__(
            self, "temperature_instruct",
            float(os.getenv("LAYER5_TEMPERATURE_INSTRUCT", "0.3")),
        )
        # 8000 tokens supports full batch processing of 50 records
        object.__setattr__(
            self, "max_tokens_instruct",
            int(os.getenv("LAYER5_MAX_TOKENS_INSTRUCT", "8000")),
        )

        # Cross-layer coherence settings
        object.__setattr__(
            self, "coherence_batch_size",
            int(os.getenv("COHERENCE_BATCH_SIZE", "50")),
        )
        object.__setattr__(
            self, "coherence_accept_threshold",
            float(os.getenv("COHERENCE_ACCEPT_THRESHOLD", "0.85")),
        )
        object.__setattr__(
            self, "coherence_review_threshold",
            float(os.getenv("COHERENCE_REVIEW_THRESHOLD", "0.70")),
        )

        # Sampled reward scoring settings
        object.__setattr__(
            self, "reward_sample_rate",
            float(os.getenv("REWARD_SAMPLE_RATE", "0.03")),
        )
        object.__setattr__(
            self, "reward_accept_threshold",
            float(os.getenv("REWARD_ACCEPT_THRESHOLD", "0.60")),
        )

        # Passport verification
        object.__setattr__(
            self, "passport_enabled",
            os.getenv("PASSPORT_ENABLED", "true").lower()
            in ("true", "1", "yes"),
        )

        # Statistical quality settings
        object.__setattr__(
            self, "dedup_similarity_threshold",
            float(os.getenv("DEDUP_SIMILARITY_THRESHOLD", "0.95")),
        )
        object.__setattr__(self, "outlier_weight_sigma", 3.0)
        object.__setattr__(self, "outlier_ratio_sigma", 2.0)
        object.__setattr__(self, "outlier_transport_sigma", 2.5)
        object.__setattr__(self, "max_single_material_pct", 0.30)

    # -- API key management --------------------------------------------

    @property
    def api_key(self) -> str:
        """Get primary API key from environment variable."""
        if self.api_provider != "nvidia":
            return os.environ.get("UVA_API_KEY", "uva-local")
        key = os.environ.get(self.api_key_env_var)
        if not key or key == "your_nvidia_api_key_here":
            raise ValueError(
                f"API key not found. Set {self.api_key_env_var} "
                "environment variable."
            )
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
        """Get rate limit per API key (default 40 req/min)."""
        return int(os.environ.get("RATE_LIMIT_PER_KEY", "40"))

    @property
    def total_rate_limit(self) -> int:
        """Total rate limit across all keys (capped at 200 req/min)."""
        return min(len(self.api_keys) * self.rate_limit_per_key, 200)

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
        """Check if API key is available and valid."""
        try:
            key = self.api_key
            return key and key != "your_nvidia_api_key_here"
        except ValueError:
            return False

    # -- Directory helpers ---------------------------------------------

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # -- Coherence decision methods ------------------------------------

    def is_coherence_acceptable(self, score: float) -> bool:
        """Check if coherence score meets acceptance threshold."""
        return score >= self.coherence_accept_threshold

    def is_coherence_reviewable(self, score: float) -> bool:
        """Check if coherence score falls in review range."""
        return (
            self.coherence_review_threshold
            <= score
            < self.coherence_accept_threshold
        )

    # -- Reward sampling -----------------------------------------------

    def should_sample_for_reward(
        self, record_index: int, total_records: int
    ) -> bool:
        """Determine if a record should be sampled for reward scoring.

        Uses deterministic sampling based on record index for
        reproducibility. Sample every Nth record where
        N = 1 / reward_sample_rate.
        """
        if self.reward_sample_rate <= 0:
            return False
        if self.reward_sample_rate >= 1.0:
            return True
        sample_interval = int(1.0 / self.reward_sample_rate)
        return record_index % sample_interval == 0
