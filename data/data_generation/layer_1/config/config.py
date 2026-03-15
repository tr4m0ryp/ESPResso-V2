"""
Configuration for Layer 1: Product Composition Generator
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

from data.data_generation.shared.paths import PipelinePaths


@dataclass
class Layer1Config:
    """Configuration settings for Layer 1 generation."""

    # Centralized path resolution
    _paths: PipelinePaths = field(
        default_factory=PipelinePaths, repr=False
    )

    @property
    def project_root(self) -> Path:
        """Backward-compatible project root accessor."""
        return self._paths.root

    # Input dataset paths
    @property
    def taxonomy_path(self) -> Path:
        return self._paths.taxonomy_path

    @property
    def materials_path(self) -> Path:
        return self._paths.materials_path

    # Output paths
    @property
    def output_dir(self) -> Path:
        return self._paths.layer_output_dir(1)

    @property
    def output_path(self) -> Path:
        return self._paths.layer1_output

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

    # Generation parameters
    temperature: float = 0.7
    top_p: float = 0.9
    max_tokens: int = 500

    # Batch and checkpoint settings
    batch_size: int = 100
    checkpoint_interval: int = 1000
    max_retries: int = 3
    retry_delay: float = 2.0

    # Products per subcategory settings
    min_products_per_subcategory: int = 100
    max_products_per_subcategory: int = 1000

    # Weight ranges by category (kg)
    weight_ranges: Dict[str, tuple] = field(default_factory=lambda: {
        # Clothing
        "cl-1": (0.60, 2.50),   # Coats
        "cl-2": (0.20, 1.00),   # Dresses
        "cl-3": (0.30, 0.80),   # Sweatshirts & Hoodies
        "cl-4": (0.10, 0.35),   # T-shirts & Polos
        "cl-5": (0.15, 0.40),   # Shirts & Blouses
        "cl-6": (0.25, 0.60),   # Knitwear & Cardigans
        "cl-7": (0.35, 0.80),   # Trousers
        "cl-8": (0.35, 0.80),   # Jeans
        "cl-9": (0.40, 2.00),   # Jackets
        "cl-10": (0.40, 1.00),  # Tracksuits & Joggers
        "cl-11": (0.50, 1.50),  # Suits & Tailoring
        "cl-12": (0.15, 0.40),  # Shorts
        "cl-13": (0.15, 0.50),  # Skirts
        "cl-14": (0.03, 0.15),  # Underwear
        "cl-15": (0.02, 0.10),  # Socks & Tights
        "cl-16": (0.05, 0.25),  # Swimwear
        "cl-17": (0.20, 0.60),  # Loungewear & Sleepwear
        # Footwear
        "fw": (0.25, 1.50),     # All footwear
        # Accessories
        "ac": (0.02, 2.00),     # All accessories
    })

    def get_weight_range(self, category_id: str) -> tuple:
        """Get weight range for a category."""
        # Check for exact match first
        if category_id in self.weight_ranges:
            return self.weight_ranges[category_id]

        # Check for parent category match (e.g., cl-1-6 -> cl-1)
        parts = category_id.split("-")
        if len(parts) >= 2:
            parent = f"{parts[0]}-{parts[1]}"
            if parent in self.weight_ranges:
                return self.weight_ranges[parent]

        # Check for main category match (e.g., fw-1 -> fw)
        main_cat = parts[0]
        if main_cat in self.weight_ranges:
            return self.weight_ranges[main_cat]

        # Default range
        return (0.10, 2.00)

    @property
    def api_key(self) -> str:
        """Get API key from environment variable."""
        key = os.environ.get(self.api_key_env_var, "")
        if not key and self.api_provider == "nvidia":
            raise ValueError(f"API key not found. Set {self.api_key_env_var} environment variable.")
        return key or "uva-local"

    def ensure_directories(self) -> None:
        """Create necessary directories if they don't exist."""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
