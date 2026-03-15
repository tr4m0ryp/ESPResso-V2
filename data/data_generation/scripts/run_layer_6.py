#!/usr/bin/env python3
"""Run script for Layer 6: Carbon Footprint Calculation.

Processes validated records from Layer 4 and calculates complete
carbon footprints using deterministic formulas. Reads and writes
Parquet files. Runs post-calculation enrichment (origin_region from
Layer 3, transport_strategy derivation).

Usage:
    python -m data.data_generation.scripts.run_layer_6 [options]

Options:
    --input PATH    Input Parquet file (Layer 4 output)
    --output PATH   Output Parquet file
    --layer3 PATH   Layer 3 Parquet for origin_region enrichment
    --verbose       Enable verbose logging
    --no-validation Skip validation checks
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from data.data_generation.layer_6.config.config import Layer6Config
from data.data_generation.layer_6.core.orchestrator import (
    Layer6Orchestrator
)


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the script.

    Args:
        verbose: Enable verbose (DEBUG) logging.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    log_dir = (
        project_root / 'data' / 'data_generation' / 'logs'
    )
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'layer6_{timestamp}.log'

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    logging.info("Logging to: %s", log_file)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments namespace.
    """
    default_input = (
        'data/datasets/pre-model/generated/'
        'layer_4/layer_4_complete_dataset.parquet'
    )
    default_output = (
        'data/datasets/pre-model/generated/'
        'layer_6/training_dataset.parquet'
    )
    default_summary = (
        'data/datasets/pre-model/generated/'
        'layer_6/calculation_summary.json'
    )
    default_layer3 = (
        'data/datasets/pre-model/generated/'
        'layer_3/layer_3_transport_scenarios.parquet'
    )

    parser = argparse.ArgumentParser(
        description='Layer 6: Carbon Footprint Calculation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with default settings
    python -m data.data_generation.scripts.run_layer_6

    # Run with custom input
    python -m data.data_generation.scripts.run_layer_6 \\
        --input path/to/input.parquet

    # Run with verbose logging
    python -m data.data_generation.scripts.run_layer_6 --verbose
        """
    )

    parser.add_argument(
        '--input', type=str, default=default_input,
        help='Input Parquet file path'
    )
    parser.add_argument(
        '--output', type=str, default=default_output,
        help='Output Parquet file path'
    )
    parser.add_argument(
        '--summary', type=str, default=default_summary,
        help='Summary JSON file path'
    )
    parser.add_argument(
        '--layer3', type=str, default=default_layer3,
        help='Layer 3 Parquet for origin_region enrichment'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Enable verbose logging'
    )
    parser.add_argument(
        '--no-validation', action='store_true',
        help='Skip validation checks'
    )
    parser.add_argument(
        '--batch-size', type=int, default=10000,
        help='Batch size for progress reporting'
    )

    return parser.parse_args()


def main() -> int:
    """Main entry point for Layer 6 calculation.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    args = parse_arguments()
    setup_logging(args.verbose)

    main_logger = logging.getLogger(__name__)

    main_logger.info("=" * 80)
    main_logger.info(" Layer 6: Carbon Footprint Calculation")
    main_logger.info("=" * 80)

    config = Layer6Config(
        input_path=args.input,
        output_dir=str(Path(args.output).parent),
        output_filename=Path(args.output).name,
        summary_filename=Path(args.summary).name,
        layer3_path=args.layer3,
        enable_validation=not args.no_validation,
        verbose=args.verbose,
        batch_size=args.batch_size
    )

    main_logger.info("Configuration:")
    main_logger.info("  Input: %s", config.input_path)
    main_logger.info("  Output: %s", config.output_path)
    main_logger.info("  Layer 3: %s", config.layer3_path)
    main_logger.info("  Summary: %s", config.summary_path)
    main_logger.info(
        "  Validation: %s",
        'enabled' if config.enable_validation else 'disabled'
    )

    orchestrator = Layer6Orchestrator(config)
    result = orchestrator.run()

    if result.get('success'):
        main_logger.info(
            "Layer 6 calculation completed successfully!"
        )
        main_logger.info("Output: %s", result.get('output_file'))
        return 0
    else:
        main_logger.error(
            "Layer 6 calculation failed: %s",
            result.get('error')
        )
        return 1


if __name__ == '__main__':
    sys.exit(main())
