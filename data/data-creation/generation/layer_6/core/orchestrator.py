"""Orchestrator for Layer 6: Carbon Footprint Calculation.

Coordinates the complete carbon footprint calculation pipeline:
1. Load reference databases from Parquet files.
2. Read input records from Parquet (Layer 4 output).
3. Extract detailed materials from step_material_mapping.
4. Calculate carbon footprints for each record.
5. Run post-calculation enrichment (origin_region, strategy).
6. Write output to Parquet with summary JSON.

Primary classes:
    Layer6Orchestrator -- Pipeline coordinator.

Dependencies:
    _processing module for input file handling.
    calculator module for footprint computation.
    enrichment module for post-processing.
    writer module for output serialization.
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Any, Optional

from data.data_generation.layer_6.config.config import Layer6Config
from data.data_generation.layer_6.core.calculator import (
    CarbonFootprintCalculator,
)
from data.data_generation.layer_6.core.databases import (
    CalculationResult
)
from data.data_generation.layer_6.core.enrichment import (
    run_enrichment
)
from data.data_generation.layer_6.core._processing import (
    process_input_file
)
from data.data_generation.layer_6.io.writer import Layer6OutputWriter

logger = logging.getLogger(__name__)


class Layer6Orchestrator:
    """Orchestrates the Layer 6 carbon footprint calculation."""

    def __init__(self, config: Optional[Layer6Config] = None):
        """Initialize orchestrator with configuration.

        Args:
            config: Layer 6 configuration (uses defaults if None).
        """
        self.config = config or Layer6Config()
        self.calculator = CarbonFootprintCalculator(self.config)
        self.writer = Layer6OutputWriter(self.config)

        self.cf_values: Dict[str, List[float]] = {
            'raw': [], 'transport': [], 'processing': [],
            'packaging': [], 'total': []
        }
        self.validation_issues = 0
        self.start_time = None
        self.end_time = None

    def run(self) -> Dict[str, Any]:
        """Run the complete Layer 6 calculation pipeline.

        Returns:
            Dictionary with processing results and statistics.
        """
        logger.info("=" * 80)
        logger.info(
            "Starting Layer 6 Carbon Footprint Calculation Pipeline"
        )
        logger.info("=" * 80)

        self.start_time = time.time()

        try:
            logger.info("[Step 1/6] Validating configuration...")
            if not self.config.validate():
                logger.error("Configuration validation failed")
                return self._create_error_result(
                    "Configuration validation failed"
                )

            logger.info("[Step 2/6] Loading reference databases...")
            if not self.calculator.load_databases():
                logger.error("Failed to load reference databases")
                return self._create_error_result(
                    "Failed to load databases"
                )

            logger.info("[Step 3/6] Processing input records...")
            output_df = process_input_file(
                self.config,
                self.calculator,
                self._track_cf_values,
                self._on_validation_issue
            )

            if output_df is None:
                logger.error("Failed to process input file")
                return self._create_error_result(
                    "Failed to process input"
                )

            logger.info("[Step 4/6] Running enrichment...")
            output_df = run_enrichment(
                output_df, self.config.layer3_path
            )

            logger.info("[Step 5/6] Writing output...")
            self.writer.write_parquet(output_df)

            logger.info("[Step 6/6] Calculating statistics...")
            cf_statistics = self._calculate_cf_statistics()
            calc_stats = self.calculator.get_statistics()
            self.writer.write_summary(calc_stats, cf_statistics)

            self.end_time = time.time()
            duration = self.end_time - self.start_time

            result = self._create_success_result(
                calc_stats, cf_statistics, duration
            )

            logger.info("=" * 80)
            logger.info(
                "Layer 6 Calculation Completed Successfully"
            )
            logger.info("=" * 80)
            self._print_summary(result)

            return result

        except Exception as e:
            logger.error("Pipeline failed with error: %s", e)
            import traceback
            logger.error(traceback.format_exc())
            self.end_time = time.time()
            return self._create_error_result(str(e))

    def _on_validation_issue(self) -> None:
        """Increment validation issue counter."""
        self.validation_issues += 1

    def _track_cf_values(self, result: CalculationResult) -> None:
        """Track CF values for statistics calculation."""
        self.cf_values['raw'].append(
            result.cf_raw_materials_kg_co2e
        )
        self.cf_values['transport'].append(
            result.cf_transport_kg_co2e
        )
        self.cf_values['processing'].append(
            result.cf_processing_kg_co2e
        )
        self.cf_values['packaging'].append(
            result.cf_packaging_kg_co2e
        )
        self.cf_values['total'].append(
            result.cf_total_kg_co2e
        )

    def _calculate_cf_statistics(
        self,
    ) -> Dict[str, Dict[str, float]]:
        """Calculate carbon footprint statistics."""
        import statistics

        stats = {}
        for component, values in self.cf_values.items():
            if values:
                stats[f'cf_{component}_kg_co2e'] = {
                    'mean': statistics.mean(values),
                    'std': (
                        statistics.stdev(values)
                        if len(values) > 1 else 0.0
                    ),
                    'min': min(values),
                    'max': max(values),
                    'median': statistics.median(values)
                }
            else:
                stats[f'cf_{component}_kg_co2e'] = {
                    'mean': 0.0, 'std': 0.0,
                    'min': 0.0, 'max': 0.0, 'median': 0.0
                }
        return stats

    def _create_success_result(
        self,
        calc_stats: Dict[str, Any],
        cf_statistics: Dict[str, Dict[str, float]],
        duration: float
    ) -> Dict[str, Any]:
        """Create success result dictionary."""
        processed = calc_stats['records_processed']
        return {
            'success': True,
            'output_file': self.config.output_path,
            'summary_file': self.config.summary_path,
            'statistics': {
                'records_processed': processed,
                'records_with_warnings':
                    calc_stats['records_with_warnings'],
                'validation_issues': self.validation_issues,
                'material_match_rate':
                    calc_stats['material_match_rate'],
                'duration_seconds': duration,
                'records_per_second': (
                    processed / duration if duration > 0 else 0
                )
            },
            'carbon_footprint_summary': {
                component: {
                    'mean': s['mean'], 'std': s['std'],
                    'min': s['min'], 'max': s['max']
                }
                for component, s in cf_statistics.items()
            },
            'timestamp': datetime.now().isoformat()
        }

    def _create_error_result(
        self, error_message: str
    ) -> Dict[str, Any]:
        """Create error result dictionary."""
        return {
            'success': False,
            'error': error_message,
            'timestamp': datetime.now().isoformat()
        }

    def _print_summary(self, result: Dict[str, Any]) -> None:
        """Print processing summary to logger."""
        stats = result.get('statistics', {})
        cf_summary = result.get('carbon_footprint_summary', {})

        logger.info(
            "Records processed: %s",
            f"{stats.get('records_processed', 0):,}"
        )
        logger.info(
            "Processing time: %.2f seconds",
            stats.get('duration_seconds', 0)
        )
        logger.info(
            "Processing rate: %.0f records/sec",
            stats.get('records_per_second', 0)
        )
        logger.info(
            "Material match rate: %.1f%%",
            stats.get('material_match_rate', 0) * 100
        )

        logger.info("Carbon Footprint Summary (kgCO2e):")
        for component, summary in cf_summary.items():
            logger.info(
                "  %s: mean=%.3f, std=%.3f, "
                "range=[%.3f, %.3f]",
                component,
                summary['mean'], summary['std'],
                summary['min'], summary['max']
            )

        logger.info("Output files:")
        logger.info(
            "  Training dataset: %s",
            result.get('output_file', '')
        )
        logger.info(
            "  Summary: %s",
            result.get('summary_file', '')
        )
