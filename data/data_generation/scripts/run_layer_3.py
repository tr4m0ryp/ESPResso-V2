#!/usr/bin/env python3
"""
Run script for Layer 3: Transport Scenario Generator

Generates realistic transport scenarios for Layer 2 preprocessing paths.
Supports parallel processing with rate limiting.

Usage:
    python run_layer_3.py                           # Default: 6 workers, 42 req/min
    python run_layer_3.py --workers 8               # Use 8 parallel workers
    python run_layer_3.py --rate-limit 30           # Limit to 30 requests/minute
    python run_layer_3.py --max-records 1000        # Process only 1000 records
    python run_layer_3.py --sequential              # Disable parallel processing
"""

import sys
import os
import logging
import argparse
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

# Add the project root to the path to make data.data_generation a package
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

# Now import using absolute path
try:
    from data.data_generation.layer_3 import Layer3Orchestrator
except ImportError as e:
    print(f"Import error: {e}")
    print("Attempting alternative import...")
    sys.path.insert(0, str(Path(__file__).parent))
    from layer_3 import Layer3Orchestrator

def setup_logging():
    """Configure logging based on environment."""
    log_level = os.getenv('LOG_LEVEL', 'INFO')
    log_file = os.getenv('LOG_FILE', 'layer_3_generation.log')
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file)
        ]
    )

def parse_args():
    """Parse command line arguments."""
    # Get defaults from environment or use reasonable defaults
    parallel_workers = int(os.getenv('PARALLEL_WORKERS', '6'))
    rate_limit = int(os.getenv('MAX_RATE_LIMIT', '200'))
    
    parser = argparse.ArgumentParser(description='Layer 3 Transport Scenario Generator')
    parser.add_argument('--workers', type=int, default=parallel_workers,
                       help=f'Number of parallel workers (default: {parallel_workers})')
    parser.add_argument('--rate-limit', type=int, default=rate_limit,
                       help=f'API requests per minute (default: {rate_limit})')
    parser.add_argument('--max-records', type=int, default=None,
                       help='Maximum records to process')
    parser.add_argument('--batch-size', type=int, default=None,
                       help='Batch size for processing')
    parser.add_argument('--sequential', action='store_true',
                       help='Disable parallel processing')
    parser.add_argument('--checkpoint', type=str, default=None,
                       help='Resume from checkpoint')
    return parser.parse_args()

def main():
    """Run Layer 3 generation."""
    setup_logging()
    logger = logging.getLogger(__name__)
    args = parse_args()
    
    try:
        logger.info("Starting Layer 3 Transport Scenario Generation")
        logger.info(f"Parallel workers: {args.workers}, Rate limit: {args.rate_limit} req/min")
        
        # Create and run orchestrator
        orchestrator = Layer3Orchestrator()
        success = orchestrator.run_generation(
            batch_size=args.batch_size,
            max_records=args.max_records,
            resume_from_checkpoint=args.checkpoint,
            parallel_workers=1 if args.sequential else args.workers,
            requests_per_minute=args.rate_limit
        )
        
        if success:
            logger.info("Layer 3 generation completed successfully")
            
            # Print summary stats
            stats = orchestrator.get_progress_stats()
            if stats:
                logger.info(f"Generated {stats.get('generated_scenarios', 0)} transport scenarios "
                           f"from {stats.get('processed_records', 0)} records")
        else:
            logger.error("Layer 3 generation failed")
            sys.exit(1)
            
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()