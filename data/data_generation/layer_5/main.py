#!/usr/bin/env python3
"""Layer 5: Cross-Layer Coherence Checker (V2) -- Entry point."""

import argparse
import logging
import sys

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.core.orchestrator import Layer5Orchestrator

logger = logging.getLogger(__name__)


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments for Layer 5 V2."""
    parser = argparse.ArgumentParser(
        description="Layer 5: Cross-Layer Coherence Checker (V2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline
  python main.py

  # Process limited records
  python main.py --max-records 5000

  # Skip passport verification
  python main.py --no-passport

  # Resume from last checkpoint
  python main.py --resume

  # Test API connection and exit
  python main.py --test-connection
        """,
    )

    # Processing options
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum number of records to process (default: all)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory (default: from config)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Batch size for processing (default: 1000)",
    )

    # V2 coherence and reward options
    parser.add_argument(
        "--coherence-batch-size",
        type=int,
        default=50,
        help="Records per LLM coherence evaluation call (default: 50)",
    )
    parser.add_argument(
        "--reward-sample-rate",
        type=float,
        default=0.03,
        help="Fraction of records to score for reward (default: 0.03 = 3%%)",
    )

    # Stage control
    parser.add_argument(
        "--no-passport",
        action="store_true",
        help="Skip passport verification",
    )
    parser.add_argument(
        "--no-reward-sampling",
        action="store_true",
        help="Skip reward sampling entirely",
    )

    # Resume support
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last checkpoint",
    )

    # Connection testing
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Initialize components, test the API connection, and exit",
    )

    # Logging options
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress non-error output",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point for Layer 5 V2 pipeline."""
    args = parse_arguments()

    # Configure logging level
    if args.verbose:
        log_level = logging.DEBUG
    elif args.quiet:
        log_level = logging.ERROR
    else:
        log_level = logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    logger.info("Layer 5 V2: Cross-Layer Coherence Checker starting")

    # Build configuration
    config = Layer5Config()

    # Apply CLI overrides to config
    if args.output_dir is not None:
        from pathlib import Path
        config._output_dir_override = Path(args.output_dir)

    if args.batch_size is not None:
        object.__setattr__(config, "batch_size", args.batch_size)

    object.__setattr__(config, "coherence_batch_size", args.coherence_batch_size)
    object.__setattr__(config, "reward_sample_rate", args.reward_sample_rate)

    if args.no_passport:
        object.__setattr__(config, "passport_enabled", False)

    if args.no_reward_sampling:
        object.__setattr__(config, "reward_sample_rate", 0.0)

    config.ensure_directories()

    # Initialize orchestrator
    orchestrator = Layer5Orchestrator(config)

    # Test connection and exit if requested
    if args.test_connection:
        success = orchestrator.test_api_connection()
        if success:
            logger.info("API connection test passed")
        else:
            logger.error("API connection test failed")
        sys.exit(0 if success else 1)

    # Run the pipeline
    result = orchestrator.run_pipeline(max_records=args.max_records)

    if result["success"]:
        stats = result.get("statistics", {})
        total = stats.get("total_records_processed", "N/A")
        logger.info("Layer 5 validation completed successfully")
        logger.info("Total records processed: %s", total)
    else:
        logger.error("Pipeline failed: %s", result.get("error", "unknown"))

    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
