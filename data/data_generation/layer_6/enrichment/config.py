"""Configuration for Layer 6 LLM enrichment phase.

Defines paths, batch settings, retry logic, and API parameters for the
transport distance extraction step that precedes the updated Layer 6
carbon footprint calculation.

Primary class:
    EnrichmentConfig -- Dataclass holding all enrichment configuration.
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentConfig:
    """Configuration for the Layer 6 LLM transport distance enrichment."""

    # -- Input paths -------------------------------------------------------

    layer5_path: str = field(
        default=(
            'data/datasets/pre-model/generated/'
            'layer_5/layer_5_validated_dataset.csv'
        )
    )
    layer4_path: str = field(
        default=(
            'data/datasets/pre-model/generated/'
            'layer_4/layer_4_complete_dataset.parquet'
        )
    )

    # -- Output paths ------------------------------------------------------

    output_dir: str = field(
        default='data/datasets/pre-model/generated/layer_6'
    )
    output_filename: str = field(
        default='pre_layer6_enriched.parquet'
    )

    # -- API settings ------------------------------------------------------

    api_base_url: str = field(default='http://localhost:3000/v1')
    api_model: str = field(default='claude-sonnet-4-5-20241022')
    api_key_env_var: str = field(default='UVA_API_KEY')
    temperature: float = field(default=0.2)
    max_tokens: int = field(default=8000)

    # -- Batch settings ----------------------------------------------------

    # Records processed per LLM call
    batch_size: int = field(default=20)
    # Write checkpoint every N records
    checkpoint_interval: int = field(default=5000)

    # -- Retry settings ----------------------------------------------------

    max_retries: int = field(default=5)

    # -- Validation settings -----------------------------------------------

    # Allowed fractional deviation when cross-checking mode distances
    # against total distance (1% tolerance)
    distance_tolerance: float = field(default=0.01)

    # -- Computed paths ----------------------------------------------------

    @property
    def output_path(self) -> str:
        """Full path to the enriched output parquet file."""
        return str(Path(self.output_dir) / self.output_filename)

    @property
    def checkpoint_dir(self) -> str:
        """Directory for intermediate checkpoints."""
        return str(Path(self.output_dir) / 'checkpoints' / 'enrichment')

    # -- API key -----------------------------------------------------------

    @property
    def api_key(self) -> str:
        """Return API key from environment (defaults to 'uva-local')."""
        return os.environ.get(self.api_key_env_var, 'uva-local')

    # -- Validation --------------------------------------------------------

    def validate(self) -> bool:
        """Check input paths exist and create output directory.

        Returns True if all required input files are present and the
        output directory was successfully created; False otherwise.
        Logs a warning for each missing file.
        """
        required_files = [self.layer5_path, self.layer4_path]
        all_ok = True

        for file_path in required_files:
            if not Path(file_path).exists():
                logger.warning(
                    'Required input file not found: %s', file_path
                )
                all_ok = False

        output = Path(self.output_dir)
        try:
            output.mkdir(parents=True, exist_ok=True)
            Path(self.checkpoint_dir).mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            logger.warning('Could not create output directory: %s', exc)
            all_ok = False

        return all_ok
