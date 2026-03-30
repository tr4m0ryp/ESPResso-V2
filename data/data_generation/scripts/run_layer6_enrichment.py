#!/usr/bin/env python3
"""Run Layer 6 transport distance enrichment.

LLM-based pipeline that reads transport_legs from Layer 4, extracts
per-mode distance totals via Claude Sonnet 4.5, validates against
known totals, and writes an enriched parquet file for the updated
Layer 6 carbon calculation.

Usage:
    python -m data.data_generation.scripts.run_layer6_enrichment [options]

Options:
    --batch-size N           Records per LLM call (default: 20)
    --checkpoint-interval N  Checkpoint every N records (default: 5000)
    --verbose                Enable DEBUG logging
    --resume                 Resume from existing checkpoints (default: on)
    --no-resume              Start fresh, ignoring existing checkpoints
"""

import argparse
import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from data.data_generation.layer_6.enrichment.config import EnrichmentConfig
from data.data_generation.layer_6.enrichment.orchestrator import (
    EnrichmentOrchestrator,
)


def setup_logging(verbose: bool = False) -> None:
    """Configure console and file logging.

    Args:
        verbose: If True, set console level to DEBUG.
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    log_dir = project_root / 'data' / 'data_generation' / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'layer6_enrichment_{timestamp}.log'

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    logging.info("Logging to: %s", log_file)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments namespace.
    """
    parser = argparse.ArgumentParser(
        description='Layer 6 Transport Distance Enrichment',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with defaults (resumes automatically)
    python -m data.data_generation.scripts.run_layer6_enrichment

    # Small batch for testing
    python -m data.data_generation.scripts.run_layer6_enrichment \\
        --batch-size 5 --checkpoint-interval 25 --verbose

    # Fresh run ignoring any existing checkpoints
    python -m data.data_generation.scripts.run_layer6_enrichment --no-resume
        """
    )

    parser.add_argument(
        '--batch-size', type=int, default=20,
        help='Records per LLM call (default: 20)'
    )
    parser.add_argument(
        '--checkpoint-interval', type=int, default=5000,
        help='Write checkpoint every N records (default: 5000)'
    )
    parser.add_argument(
        '--verbose', '-v', action='store_true',
        help='Enable verbose (DEBUG) logging'
    )
    parser.add_argument(
        '--workers', '-w', type=int, default=100,
        help='Number of parallel workers (default: 100)'
    )
    parser.add_argument(
        '--no-resume', action='store_true',
        help='Start fresh, ignoring existing checkpoints'
    )

    return parser.parse_args()


def main() -> int:
    """Entry point for Layer 6 enrichment.

    Returns:
        Exit code: 0 on success, 1 on failure.
    """
    args = parse_arguments()
    setup_logging(args.verbose)

    main_logger = logging.getLogger(__name__)
    main_logger.info("=" * 70)
    main_logger.info(" Layer 6: Transport Distance Enrichment")
    main_logger.info("=" * 70)

    config = EnrichmentConfig(
        batch_size=args.batch_size,
        checkpoint_interval=args.checkpoint_interval,
        num_workers=args.workers,
    )

    main_logger.info("Configuration:")
    main_logger.info("  Layer 5 input: %s", config.layer5_path)
    main_logger.info("  Layer 4 input: %s", config.layer4_path)
    main_logger.info("  Output: %s", config.output_path)
    main_logger.info("  Batch size: %d", config.batch_size)
    main_logger.info("  Workers: %d", config.num_workers)
    main_logger.info("  Checkpoint interval: %d", config.checkpoint_interval)
    main_logger.info("  Model: %s", config.api_model)
    main_logger.info("  API keys: %d", len(config.api_keys))

    # If --no-resume, clear any existing checkpoints before starting
    if args.no_resume:
        ckpt_dir = Path(config.checkpoint_dir)
        if ckpt_dir.exists():
            shutil.rmtree(ckpt_dir)
            main_logger.info("Cleared existing checkpoints (--no-resume)")

    try:
        orchestrator = EnrichmentOrchestrator(config)
        output_path = orchestrator.run()
        print(f"Enriched dataset saved to: {output_path}")
        return 0
    except Exception as exc:
        main_logger.error("Enrichment failed: %s", exc, exc_info=True)
        return 1


if __name__ == '__main__':
    sys.exit(main())
