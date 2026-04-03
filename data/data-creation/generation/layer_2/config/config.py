"""
Configuration for Layer 2: Preprocessing Path Generator
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from data.data_generation.shared.paths import PipelinePaths


@dataclass
class Layer2Config:
    """Configuration settings for Layer 2 generation."""

    # Centralized path resolution
    _paths: PipelinePaths = field(
        default_factory=PipelinePaths, repr=False
    )

    @property
    def project_root(self) -> Path:
        """Backward-compatible project root accessor."""
        return self._paths.root

    # API key configuration
    
    api_key_env_vars: List[str] = field(default_factory=lambda: ["UVA_API_KEY"])

    # Input dataset paths
    @property
    def layer1_output_path(self) -> Path:
        return self._paths.layer1_output

    @property
    def processing_steps_path(self) -> Path:
        return self._paths.processing_steps_path

    @property
    def material_process_combinations_path(self) -> Path:
        return self._paths.material_processing_path

    # Output paths
    @property
    def output_dir(self) -> Path:
        return self._paths.layer_output_dir(2)

    @property
    def output_path(self) -> Path:
        return self._paths.layer2_output

    @property
    def checkpoint_dir(self) -> Path:
        return self.output_dir / "checkpoints"

    # API Configuration
    api_base_url: str = "http://localhost:3000/v1"
    api_model: str = "claude-sonnet-4.6"
    api_key_env_var: str = "UVA_API_KEY"

    # Provider switching (nvidia or uva)
    api_provider: str = field(default_factory=lambda: os.environ.get("API_PROVIDER", "uva"))

    def __post_init__(self):
        if self.api_provider == "nvidia":
            object.__setattr__(self, 'api_base_url', 'https://integrate.api.nvidia.com/v1')
            object.__setattr__(self, 'api_model', os.environ.get('NVIDIA_MODEL', 'nvidia/llama-3.1-nemotron-ultra-253b-v1'))
            object.__setattr__(self, 'api_key_env_var', 'NVIDIA_API_KEY')

    # Generation parameters - lower temperature for consistent manufacturing logic and JSON
    temperature: float = 0.3  # Lower temperature for more deterministic JSON generation
    top_p: float = 0.9
    max_tokens: int = 4000  # Increased for 10 detailed pathways

    # Batch and checkpoint settings
    batch_size: int = 25  # Process 25 L1 records per batch for efficiency
    checkpoint_interval: int = 2500
    max_retries: int = 3
    retry_delay: float = 2.0

    # Generation settings - 10 paths per product for 1M+ dataset target
    # L1(100) × L2(10) × L3(5) × L4(2) × 137 subcats = 1,370,000 records
    paths_per_product: int = 10  # Fixed at 10 for consistency

    # Processing step categories and their order
    processing_order: List[str] = field(default_factory=lambda: [
        "Pre-processing",
        "Primary processing",
        "Synthetic fibre production",
        "Glass/mineral fibre processing",
        "Wet processing",
        "Finishing",
        "Special treatments",
        "Composite processing",
        "Construction materials"
    ])

    @property
    def api_key(self) -> str:
        """Get primary API key from environment variable."""
        key = os.environ.get(self.api_key_env_var, "")
        if not key and self.api_provider == "nvidia":
            raise ValueError(f"API key not found. Set {self.api_key_env_var} environment variable.")
        return key or "uva-local"

    @property
    def api_keys(self) -> List[str]:
        """Get all available API keys from environment variables."""
        if self.api_provider != "nvidia":
            return [os.environ.get("UVA_API_KEY", "uva-local")]
        keys = []
        for env_var in self.api_key_env_vars:
            key = os.environ.get(env_var)
            if key and key != "YOUR_SECOND_API_KEY_HERE" and key != "YOUR_THIRD_API_KEY_HERE":
                keys.append(key)
        if not keys:
            raise ValueError(f"No API keys found. Set at least {self.api_key_env_vars[0]} environment variable.")
        return keys

    @property
    def rate_limit_per_key(self) -> int:
        """Get rate limit per API key from environment or default."""
        return int(os.environ.get("RATE_LIMIT_PER_KEY", "42"))

    @property
    def total_rate_limit(self) -> int:
        """Calculate total rate limit based on available keys (max 200 req/min for 5 keys)."""
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

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
