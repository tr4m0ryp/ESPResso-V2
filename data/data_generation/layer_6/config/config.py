"""Configuration for Layer 6: Carbon Footprint Calculation Layer.

Defines paths to input/output files and reference databases,
as well as calculation parameters. All file paths default to
Parquet format.

Primary classes:
    Layer6Config -- Dataclass holding all pipeline configuration.

Constants:
    TRANSPORT_EMISSION_FACTORS -- Per-mode EF in g CO2e/tkm.
    PACKAGING_EMISSION_FACTORS -- Per-category EF in kgCO2e/kg.
    TRANSPORT_MODE_PARAMS -- Multinomial logit parameters.
    ADJUSTMENT_FACTOR -- Unmodelled emissions multiplier.
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict


# Transport mode emission factors (g CO2e/tkm) from research paper
TRANSPORT_EMISSION_FACTORS: Dict[str, float] = {
    'road': 74.0,
    'rail': 22.0,
    'inland_waterway': 31.0,
    'sea': 10.3,
    'air': 782.0
}

# Packaging emission factors (kg CO2e/kg) from research paper
PACKAGING_EMISSION_FACTORS: Dict[str, float] = {
    'Paper/Cardboard': 1.3,
    'Plastic': 3.5,
    'Glass': 1.1,
    'Other/Unspecified': 2.5
}

# Multinomial logit model parameters for transport mode selection
# Fitted from 761K transport legs in the Layer 4 dataset (53,926 records)
TRANSPORT_MODE_PARAMS: Dict[str, Dict[str, float]] = {
    'road': {'alpha': 9.124443, 'beta': -0.01, 'd_ref': 500.0},
    'rail': {'alpha': -0.008712, 'beta': -0.00310896, 'd_ref': 1500.0},
    'sea': {'alpha': 1.291277, 'beta': 0.00369985, 'd_ref': 5000.0},
    'air': {'alpha': -2.077025, 'beta': 0.00483928, 'd_ref': 8000.0},
    'inland_waterway': {
        'alpha': 0.106072, 'beta': -0.00325982, 'd_ref': 800.0
    },
}

# Adjustment factor for unmodelled emissions
# (internal logistics 1% + waste 1%)
ADJUSTMENT_FACTOR = 1.02


@dataclass
class Layer6Config:
    """Configuration for Layer 6 carbon footprint calculation."""

    # Input paths
    input_path: str = field(
        default=(
            'data/datasets/pre-model/generated/'
            'layer_4/layer_4_complete_dataset.parquet'
        )
    )

    # Reference database paths (Parquet)
    materials_db_path: str = field(
        default='data/datasets/pre-model/final/base_materials.parquet'
    )
    processing_db_path: str = field(
        default=(
            'data/datasets/pre-model/final/'
            'material_processing_combinations.parquet'
        )
    )
    packaging_db_path: str = field(
        default=(
            'data/datasets/pre-model/final/'
            'packaging_materials_by_category.parquet'
        )
    )
    processing_steps_path: str = field(
        default=(
            'data/datasets/pre-model/final/'
            'processing_steps.parquet'
        )
    )

    # Layer 3 path for origin_region enrichment
    layer3_path: str = field(
        default=(
            'data/datasets/pre-model/generated/'
            'layer_3/layer_3_transport_scenarios.parquet'
        )
    )

    # Output paths
    output_dir: str = field(
        default='data/datasets/pre-model/generated/layer_6'
    )
    output_filename: str = field(
        default='training_dataset.parquet'
    )
    summary_filename: str = field(
        default='calculation_summary.json'
    )

    # Calculation parameters
    adjustment_factor: float = field(default=ADJUSTMENT_FACTOR)
    enable_validation: bool = field(default=True)
    enable_statistics: bool = field(default=True)

    # Processing parameters
    batch_size: int = field(default=10000)
    verbose: bool = field(default=False)

    # Emission factors
    transport_ef: Dict[str, float] = field(
        default_factory=lambda: TRANSPORT_EMISSION_FACTORS.copy()
    )
    packaging_ef: Dict[str, float] = field(
        default_factory=lambda: PACKAGING_EMISSION_FACTORS.copy()
    )

    @property
    def output_path(self) -> str:
        """Get full output file path."""
        return str(Path(self.output_dir) / self.output_filename)

    @property
    def summary_path(self) -> str:
        """Get full summary file path."""
        return str(Path(self.output_dir) / self.summary_filename)

    def validate(self) -> bool:
        """Validate configuration paths exist."""
        required_files = [
            self.input_path,
            self.materials_db_path,
            self.processing_db_path,
        ]

        for file_path in required_files:
            if not Path(file_path).exists():
                return False

        # Ensure output directory exists
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)

        return True
