#!/usr/bin/env python3
"""Run script for Layer 7: Water Footprint Calculation.

Processes validated records from Layer 5 (joined with Layer 4
transport legs) and calculates complete water footprints using
AWARE-weighted consumption factors.

Usage:
    python -m data.data_generation.scripts.run_layer_7 [options]

Options:
    --layer5 PATH   Layer 5 validated CSV file
    --layer4 PATH   Layer 4 parquet file (for transport_legs)
    --output PATH   Output CSV file
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

from data.data_generation.layer_7.config.config import Layer7Config
from data.data_generation.layer_7.core.orchestrator import (
    Layer7Orchestrator,
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
    log_file = log_dir / f'layer7_{timestamp}.log'

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
    default_layer5 = (
        'data/datasets/pre-model/generated/'
        'layer_5/layer_5_validated_dataset.csv'
    )
    default_layer4 = (
        'data/datasets/pre-model/generated/'
        'layer_4/layer_4_complete_dataset.parquet'
    )
    default_output = (
        'data/datasets/pre-model/generated/'
        'layer_7/water_footprint_dataset.csv'
    )
    default_summary = (
        'data/datasets/pre-model/generated/'
        'layer_7/calculation_summary.json'
    )

    parser = argparse.ArgumentParser(
        description='Layer 7: Water Footprint Calculation',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with default settings
    python -m data.data_generation.scripts.run_layer_7

    # Run with verbose logging
    python -m data.data_generation.scripts.run_layer_7 --verbose
        """
    )

    parser.add_argument(
        '--layer5', type=str, default=default_layer5,
        help='Layer 5 validated CSV file path'
    )
    parser.add_argument(
        '--layer4', type=str, default=default_layer4,
        help='Layer 4 parquet file path'
    )
    parser.add_argument(
        '--output', type=str, default=default_output,
        help='Output CSV file path'
    )
    parser.add_argument(
        '--summary', type=str, default=default_summary,
        help='Summary JSON file path'
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
    """Main entry point for Layer 7 calculation.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    args = parse_arguments()
    setup_logging(args.verbose)

    main_logger = logging.getLogger(__name__)

    main_logger.info("=" * 80)
    main_logger.info(" Layer 7: Water Footprint Calculation")
    main_logger.info("=" * 80)

    config = Layer7Config(
        layer5_path=args.layer5,
        layer4_path=args.layer4,
        output_dir=str(Path(args.output).parent),
        output_filename=Path(args.output).name,
        summary_filename=Path(args.summary).name,
        enable_validation=not args.no_validation,
        verbose=args.verbose,
        batch_size=args.batch_size,
    )

    main_logger.info("Configuration:")
    main_logger.info("  Layer 5 input: %s", config.layer5_path)
    main_logger.info("  Layer 4 input: %s", config.layer4_path)
    main_logger.info("  Output: %s", config.output_path)
    main_logger.info("  Summary: %s", config.summary_path)
    main_logger.info(
        "  Validation: %s",
        'enabled' if config.enable_validation else 'disabled'
    )

    orchestrator = Layer7Orchestrator(config)
    result = orchestrator.run()

    if result.get('success'):
        main_logger.info(
            "Layer 7 calculation completed successfully."
        )
        main_logger.info("Output: %s", result.get('output_file'))
        return 0
    else:
        main_logger.error(
            "Layer 7 calculation failed: %s",
            result.get('error')
        )
        return 1


if __name__ == '__main__':
    sys.exit(main())
