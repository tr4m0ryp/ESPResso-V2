"""Output Writer for Layer 7: Water Footprint Calculation.

Handles writing calculated water footprints to CSV and the
calculation summary to JSON.

Output schema (D11):
    record_id, wf_raw_materials_m3_world_eq, wf_processing_m3_world_eq,
    wf_packaging_m3_world_eq, wf_total_m3_world_eq,
    calculation_timestamp, calculation_version

Primary classes:
    Layer7OutputWriter -- CSV and JSON output handler.

Dependencies:
    pandas for CSV serialization.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from data.data_generation.layer_7.config.config import Layer7Config

logger = logging.getLogger(__name__)

# Output CSV column order
_OUTPUT_COLUMNS = [
    'record_id',
    'wf_raw_materials_m3_world_eq',
    'wf_processing_m3_world_eq',
    'wf_packaging_m3_world_eq',
    'wf_total_m3_world_eq',
    'calculation_timestamp',
    'calculation_version',
]


class Layer7OutputWriter:
    """Handles output writing for Layer 7 calculation results."""

    def __init__(self, config: Layer7Config):
        """Initialize output writer.

        Args:
            config: Layer 7 configuration.
        """
        self.config = config
        self.records_written = 0

    def write_csv(self, df: pd.DataFrame) -> bool:
        """Write the output DataFrame to CSV.

        Args:
            df: Complete output DataFrame with WF columns.

        Returns:
            True if write successful.
        """
        try:
            output_path = Path(self.config.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Ensure column order and only output required columns
            cols = [
                c for c in _OUTPUT_COLUMNS if c in df.columns
            ]
            df[cols].to_csv(output_path, index=False)

            self.records_written = len(df)
            logger.info(
                "Written %d records to %s",
                self.records_written, output_path
            )
            return True

        except Exception as e:
            logger.error("Failed to write CSV output: %s", e)
            return False

    def write_summary(
        self,
        statistics: Dict[str, Any],
        wf_statistics: Dict[str, Dict[str, float]],
    ) -> bool:
        """Write calculation summary to JSON file.

        Args:
            statistics: Processing statistics.
            wf_statistics: Water footprint statistics.

        Returns:
            True if write successful.
        """
        try:
            summary = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'pipeline_version': 'v1.0',
                    'layer': 7,
                    'description': (
                        'Water footprint calculation statistics'
                    ),
                },
                'processing_summary': {
                    'total_records_processed': statistics.get(
                        'records_processed', 0
                    ),
                    'records_with_warnings': statistics.get(
                        'records_with_warnings', 0
                    ),
                    'material_match_rate': statistics.get(
                        'material_match_rate', 0.0
                    ),
                },
                'water_footprint_statistics': wf_statistics,
                'output_file': self.config.output_path,
                'input_files': {
                    'layer5': self.config.layer5_path,
                    'layer4': self.config.layer4_path,
                },
            }

            summary_path = Path(self.config.summary_path)
            summary_path.parent.mkdir(parents=True, exist_ok=True)

            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)

            logger.info(
                "Written calculation summary to %s", summary_path
            )
            return True

        except Exception as e:
            logger.error("Failed to write summary: %s", e)
            return False
