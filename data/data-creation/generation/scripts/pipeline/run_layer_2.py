#!/usr/bin/env python3
"""
Entry point script for Layer 2 generation.

Usage:
    python run_layer_2.py --help
    python run_layer_2.py --health-check
    python run_layer_2.py --dry-run
    python run_layer_2.py --paths-per-product 4
    python run_layer_2.py --start-index 1000 --max-records 500
"""

from layer_2.orchestrator import main

if __name__ == "__main__":
    main()
