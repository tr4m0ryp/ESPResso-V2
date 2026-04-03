"""Orchestrator for Layer 7: Water Footprint Calculation.

Coordinates the complete water footprint calculation pipeline:
1. Validate configuration.
2. Join Layer 5 + Layer 4 data (via data_joiner).
3. Load water reference databases and AWARE factors.
4. Calculate water footprints for each record.
5. Write output CSV.
6. Write summary statistics JSON.

Primary classes:
    Layer7Orchestrator -- Pipeline coordinator.

Dependencies:
    enrichment.data_joiner for Layer 5 + Layer 4 join.
    calculator module for water footprint computation.
    writer module for output serialization.
"""

import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from data.data_generation.layer_7.config.config import Layer7Config
from data.data_generation.layer_7.core.calculator import (
    WaterFootprintCalculator,
)
from data.data_generation.layer_7.core.databases import (
    WaterCalculationResult,
)
from data.data_generation.layer_7.enrichment.data_joiner import (
    join_transport_legs,
)
from data.data_generation.layer_7.core._processing import (
    process_records,
)
from data.data_generation.layer_7.io.writer import (
    Layer7OutputWriter,
)

logger = logging.getLogger(__name__)


class Layer7Orchestrator:
    """Orchestrates the Layer 7 water footprint calculation."""

    def __init__(self, config: Optional[Layer7Config] = None):
        """Initialize orchestrator with configuration.

        Args:
            config: Layer 7 configuration (uses defaults if None).
        """
        self.config = config or Layer7Config()
        self.calculator = WaterFootprintCalculator(self.config)
        self.writer = Layer7OutputWriter(self.config)

        self.wf_values: Dict[str, List[float]] = {
            'raw': [], 'processing': [],
            'packaging': [], 'total': [],
        }
        self.validation_issues = 0
        self.start_time = None
        self.end_time = None

    def run(self) -> Dict[str, Any]:
        """Run the complete Layer 7 calculation pipeline.

        Returns:
            Dictionary with processing results and statistics.
        """
        logger.info("=" * 80)
        logger.info(
            "Starting Layer 7 Water Footprint Calculation Pipeline"
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

            logger.info(
                "[Step 2/6] Joining Layer 5 + Layer 4 data..."
            )
            input_df = join_transport_legs(
                self.config.layer5_path,
                self.config.layer4_path,
            )
            logger.info(
                "Joined dataset: %d rows, %d columns",
                len(input_df), len(input_df.columns)
            )

            logger.info(
                "[Step 3/6] Loading water reference databases..."
            )
            if not self.calculator.load_databases():
                logger.error("Failed to load water databases")
                return self._create_error_result(
                    "Failed to load databases"
                )

            logger.info(
                "[Step 4/6] Calculating water footprints..."
            )
            output_df = process_records(
                input_df,
                self.calculator,
                self._track_wf_values,
                self._on_validation_issue,
                self.config.enable_validation,
                self.config.verbose,
            )

            if output_df is None:
                logger.error("Failed to process records")
                return self._create_error_result(
                    "Failed to process records"
                )

            logger.info("[Step 5/6] Writing output...")
            self.writer.write_csv(output_df)

            logger.info("[Step 6/6] Calculating statistics...")
            wf_statistics = self._calculate_wf_statistics()
            calc_stats = self.calculator.get_statistics()
            self.writer.write_summary(calc_stats, wf_statistics)

            self.end_time = time.time()
            duration = self.end_time - self.start_time

            result = self._create_success_result(
                calc_stats, wf_statistics, duration
            )

            logger.info("=" * 80)
            logger.info(
                "Layer 7 Calculation Completed Successfully"
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

    def _track_wf_values(
        self, result: WaterCalculationResult
    ) -> None:
        """Track WF values for statistics calculation."""
        self.wf_values['raw'].append(
            result.wf_raw_materials_m3_world_eq
        )
        self.wf_values['processing'].append(
            result.wf_processing_m3_world_eq
        )
        self.wf_values['packaging'].append(
            result.wf_packaging_m3_world_eq
        )
        self.wf_values['total'].append(
            result.wf_total_m3_world_eq
        )

    def _calculate_wf_statistics(
        self,
    ) -> Dict[str, Dict[str, float]]:
        """Calculate water footprint statistics."""
        import statistics

        stats = {}
        for component, values in self.wf_values.items():
            key = f'wf_{component}_m3_world_eq'
            if values:
                stats[key] = {
                    'mean': statistics.mean(values),
                    'std': (
                        statistics.stdev(values)
                        if len(values) > 1 else 0.0
                    ),
                    'min': min(values),
                    'max': max(values),
                    'median': statistics.median(values),
                }
            else:
                stats[key] = {
                    'mean': 0.0, 'std': 0.0,
                    'min': 0.0, 'max': 0.0, 'median': 0.0,
                }
        return stats

    def _create_success_result(
        self,
        calc_stats: Dict[str, Any],
        wf_statistics: Dict[str, Dict[str, float]],
        duration: float,
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
                ),
            },
            'water_footprint_summary': {
                component: {
                    'mean': s['mean'], 'std': s['std'],
                    'min': s['min'], 'max': s['max'],
                }
                for component, s in wf_statistics.items()
            },
            'timestamp': datetime.now().isoformat(),
        }

    def _create_error_result(
        self, error_message: str
    ) -> Dict[str, Any]:
        """Create error result dictionary."""
        return {
            'success': False,
            'error': error_message,
            'timestamp': datetime.now().isoformat(),
        }

    def _print_summary(self, result: Dict[str, Any]) -> None:
        """Print processing summary to logger."""
        stats = result.get('statistics', {})
        wf_summary = result.get('water_footprint_summary', {})

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

        logger.info("Water Footprint Summary (m3 world-eq):")
        for component, summary in wf_summary.items():
            logger.info(
                "  %s: mean=%.6f, std=%.6f, "
                "range=[%.6f, %.6f]",
                component,
                summary['mean'], summary['std'],
                summary['min'], summary['max']
            )

        logger.info("Output files:")
        logger.info(
            "  Water footprint dataset: %s",
            result.get('output_file', '')
        )
        logger.info(
            "  Summary: %s",
            result.get('summary_file', '')
        )
