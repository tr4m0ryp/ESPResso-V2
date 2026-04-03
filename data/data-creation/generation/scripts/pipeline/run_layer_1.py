#!/usr/bin/env python3
"""
Entry point script for Layer 1 generation.

Usage:
    python run_layer_1.py --help
    python run_layer_1.py --health-check
    python run_layer_1.py --dry-run
    python run_layer_1.py --products-per-category 100
    python run_layer_1.py --categories cl-1-6 cl-4-1 fw-1
"""

from data.data_generation.layer_1.core.orchestrator import main

if __name__ == "__main__":
    main()
