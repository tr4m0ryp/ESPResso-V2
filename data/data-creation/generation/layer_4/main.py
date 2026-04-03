#!/usr/bin/env python3
"""Layer 4: Packaging Configuration Generator (V2) -- Entry point."""

import argparse
import logging
import sys

from data.data_generation.layer_4.core.orchestrator import Layer4Orchestrator

logger = logging.getLogger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Layer 4: Packaging Configuration Generator (V2)"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum number of Layer 3 records to process (default: all)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from the last saved checkpoint",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel worker threads (default: config/env)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=None,
        help="Products per API call (default: config/env)",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=None,
        help="Max requests per minute (default: config/env)",
    )
    parser.add_argument(
        "--test-connection",
        action="store_true",
        help="Initialize components, test the API connection, and exit",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    orchestrator = Layer4Orchestrator()
    orchestrator.initialize()

    if args.test_connection:
        print("Connection test passed.")
        return

    success = orchestrator.run_generation(
        max_records=args.max_records,
        resume=args.resume,
        workers=args.workers,
        batch_size=args.batch_size,
        rate_limit=args.rate_limit,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
