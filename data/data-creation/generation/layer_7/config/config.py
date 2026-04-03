"""Configuration for Layer 7: Water Footprint Calculation Layer.

Defines paths to input/output files, water reference databases, and
AWARE characterization factor databases. All water reference files
are CSV format (produced by task 006 extraction scripts).

Primary classes:
    Layer7Config -- Dataclass holding all pipeline configuration.

Constants:
    AWARE_FALLBACK_AGRI -- Global fallback for agricultural AWARE CF.
    AWARE_FALLBACK_NONAGRI -- Global fallback for non-agricultural AWARE CF.
"""

from dataclasses import dataclass, field
from pathlib import Path


# Global AWARE fallback values (GLO row from AWARE20 dataset)
AWARE_FALLBACK_AGRI: float = 43.1
AWARE_FALLBACK_NONAGRI: float = 17.9

# Base path for water footprint reference CSVs
_WATER_REF_DIR = 'data/datasets/pre-model/final/water_footprint'


@dataclass
class Layer7Config:
    """Configuration for Layer 7 water footprint calculation."""

    # Input paths (Layer 5 validated + Layer 4 for transport_legs join)
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

    # Water reference database paths (CSV)
    materials_water_path: str = field(
        default=f'{_WATER_REF_DIR}/base_materials_water.csv'
    )
    processing_water_path: str = field(
        default=f'{_WATER_REF_DIR}/processing_steps_water.csv'
    )
    material_processing_water_path: str = field(
        default=f'{_WATER_REF_DIR}/material_processing_water.csv'
    )
    packaging_water_path: str = field(
        default=f'{_WATER_REF_DIR}/packaging_water.csv'
    )

    # AWARE characterization factor paths (CSV)
    aware_agri_path: str = field(
        default=f'{_WATER_REF_DIR}/aware_factors_agri.csv'
    )
    aware_nonagri_path: str = field(
        default=f'{_WATER_REF_DIR}/aware_factors_nonagri.csv'
    )
    aware_aliases_path: str = field(
        default=f'{_WATER_REF_DIR}/aware_country_aliases.csv'
    )

    # Output paths
    output_dir: str = field(
        default='data/datasets/pre-model/generated/layer_7'
    )
    output_filename: str = field(
        default='water_footprint_dataset.csv'
    )
    summary_filename: str = field(
        default='calculation_summary.json'
    )

    # Calculation parameters
    enable_validation: bool = field(default=True)
    enable_statistics: bool = field(default=True)
    verbose: bool = field(default=False)
    batch_size: int = field(default=10000)

    @property
    def output_path(self) -> str:
        """Get full output file path."""
        return str(Path(self.output_dir) / self.output_filename)

    @property
    def summary_path(self) -> str:
        """Get full summary file path."""
        return str(Path(self.output_dir) / self.summary_filename)

    def validate(self) -> bool:
        """Validate that required input and reference files exist.

        Returns:
            True if all required files are found.
        """
        required_files = [
            self.layer5_path,
            self.layer4_path,
            self.materials_water_path,
            self.processing_water_path,
            self.packaging_water_path,
            self.aware_agri_path,
            self.aware_nonagri_path,
        ]

        import logging
        logger = logging.getLogger(__name__)

        all_ok = True
        for file_path in required_files:
            if not Path(file_path).exists():
                logger.error("Required file not found: %s", file_path)
                all_ok = False

        # Ensure output directory exists
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        return all_ok
