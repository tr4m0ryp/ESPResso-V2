"""Centralized path management for the data generation pipeline.

Single source of truth for all filesystem paths used across Layers
1-6. Every config module delegates path resolution here so that
directory structure changes require editing only this file.

Primary classes:
    PipelinePaths -- Frozen dataclass exposing every reference DB,
        generated output, and log directory as a property.

Assumptions:
    Project root is detected by walking upward from this file until
    a directory containing both 'data' and '.git' is found.
"""

from dataclasses import dataclass, field
from pathlib import Path


def _find_project_root() -> Path:
    """Walk upward from this file to locate the project root.

    The project root is the first ancestor directory that contains
    both a 'data' subdirectory and a '.git' marker.

    Returns:
        Absolute path to the project root.

    Raises:
        RuntimeError: If no suitable root is found within 10 levels.
    """
    current = Path(__file__).resolve().parent
    for _ in range(10):
        if (current / "data").is_dir() and (current / ".git").exists():
            return current
        current = current.parent
    raise RuntimeError(
        "Cannot locate project root from %s" % Path(__file__)
    )


@dataclass(frozen=True)
class PipelinePaths:
    """Immutable container for all pipeline filesystem paths."""

    root: Path = field(
        default_factory=_find_project_root
    )

    # -- Top-level dataset directories --------------------------------

    @property
    def datasets_dir(self) -> Path:
        """Root of all dataset storage."""
        return self.root / "data" / "datasets"

    @property
    def final_dir(self) -> Path:
        """Reference databases (read-only inputs)."""
        return self.datasets_dir / "pre-model" / "final"

    @property
    def generated_dir(self) -> Path:
        """Generated layer outputs."""
        return self.datasets_dir / "pre-model" / "generated"

    # -- Reference database paths -------------------------------------

    @property
    def taxonomy_path(self) -> Path:
        return self.final_dir / "taxonomy_category.parquet"

    @property
    def materials_path(self) -> Path:
        return self.final_dir / "base_materials.parquet"

    @property
    def processing_steps_path(self) -> Path:
        return self.final_dir / "processing_steps.parquet"

    @property
    def material_processing_path(self) -> Path:
        return self.final_dir / "material_processing_combinations.parquet"

    @property
    def packaging_materials_path(self) -> Path:
        return self.final_dir / "packaging_materials_by_category.parquet"

    # -- Per-layer output directories ---------------------------------

    def layer_output_dir(self, layer_num: int) -> Path:
        """Return the output directory for a given layer number.

        Args:
            layer_num: Layer number (1-6).

        Returns:
            Path to the layer output directory.
        """
        return self.generated_dir / f"layer_{layer_num}"

    # -- Per-layer generated output files -----------------------------

    @property
    def layer1_output(self) -> Path:
        return (
            self.layer_output_dir(1)
            / "layer_1_product_compositions.parquet"
        )

    @property
    def layer2_output(self) -> Path:
        return (
            self.layer_output_dir(2)
            / "layer_2_preprocessing_paths.parquet"
        )

    @property
    def layer3_output(self) -> Path:
        return (
            self.layer_output_dir(3)
            / "layer_3_transport_scenarios.parquet"
        )

    @property
    def layer4_output(self) -> Path:
        return (
            self.layer_output_dir(4)
            / "layer_4_complete_dataset.parquet"
        )

    @property
    def layer6_output(self) -> Path:
        return (
            self.layer_output_dir(6)
            / "training_dataset.parquet"
        )

    # -- Token usage tracking ------------------------------------------

    @property
    def token_usage_path(self) -> Path:
        """Persistent all-time token usage tracking file."""
        return self.root / "data" / "data_generation" / "token_usage.json"

    # -- Logging directory --------------------------------------------

    @property
    def logs_dir(self) -> Path:
        """Centralized log directory for all layers."""
        return (
            self.root / "data" / "data_generation" / "logs"
        )
