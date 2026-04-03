"""Output Writer for Layer 6: Carbon Footprint Calculation.

Handles writing calculated carbon footprints to Parquet (gzip
compressed) and the calculation summary to JSON.

Primary classes:
    Layer6OutputWriter -- Parquet and JSON output handler.

Dependencies:
    pandas for Parquet serialization.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

import pandas as pd

from data.data_generation.layer_6.config.config import Layer6Config

logger = logging.getLogger(__name__)


class Layer6OutputWriter:
    """Handles output writing for Layer 6 calculation results."""

    def __init__(self, config: Layer6Config):
        """Initialize output writer.

        Args:
            config: Layer 6 configuration.
        """
        self.config = config
        self.records_written = 0

    def write_parquet(self, df: pd.DataFrame) -> bool:
        """Write the output DataFrame to Parquet with gzip.

        Args:
            df: Complete output DataFrame with CF columns.

        Returns:
            True if write successful.
        """
        try:
            output_path = Path(self.config.output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df.to_parquet(
                output_path,
                engine='pyarrow',
                compression='gzip',
                index=False
            )

            self.records_written = len(df)
            logger.info(
                "Written %d records to %s",
                self.records_written, output_path
            )
            return True

        except Exception as e:
            logger.error("Failed to write Parquet output: %s", e)
            return False

    def write_summary(
        self,
        statistics: Dict[str, Any],
        cf_statistics: Dict[str, Dict[str, float]]
    ) -> bool:
        """Write calculation summary to JSON file.

        Args:
            statistics: Processing statistics.
            cf_statistics: Carbon footprint statistics.

        Returns:
            True if write successful.
        """
        try:
            summary = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'pipeline_version': 'v2.0',
                    'layer': 6,
                    'description': (
                        'Carbon footprint calculation statistics'
                    )
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
                    )
                },
                'carbon_footprint_statistics': cf_statistics,
                'output_file': self.config.output_path,
                'input_file': self.config.input_path
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
