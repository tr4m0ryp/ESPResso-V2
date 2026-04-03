#!/bin/bash

# Layer 3 Optimized Execution Script
# This script runs the optimized Layer 3 generation with all improvements

echo "============================================================"
echo "Layer 3 Transport Scenario Generation (Optimized)"
echo "============================================================"
echo ""

# Change to project directory
cd /home/tr4m0ryp/Projects/carbon_footrpint_model

# Activate virtual environment if needed
# source venv/bin/activate

echo "Configuration:"
echo "  - Input: 14,339 preprocessing paths (cleaned dataset)"
echo "  - Output: 71,695 transport scenarios (5 per path)"
echo "  - Strategies: cost, speed, eco, risk, regional"
echo "  - Generation: Rule-based (no API key required)"
echo ""

# Set rate limiting (adjust based on your system)
export PARALLEL_WORKERS="6"
export RATE_LIMIT_PER_KEY="40"

# Optional: Set API key for higher quality LLM scenarios
# export UVA_API_KEY="uva-local"
# If not set, will fall back to rule-based generation automatically

echo "Starting generation..."
echo "Monitor progress: tail -f data/data_generation/layer_3_generation.log"
echo ""

# Run the generation
cd data/data_generation
python run_layer_3.py \
    --workers "$PARALLEL_WORKERS" \
    --rate-limit "$RATE_LIMIT_PER_KEY" \
    --batch-size 50 \
    --checkpoint-interval 5000

echo ""
echo "============================================================"
echo "Generation Complete!"
echo "============================================================"
echo ""
echo "Output file: data/datasets/pre-model/generated/layer_3/layer_3_transport_scenarios.parquet"
echo ""
echo "To analyze the results:"
echo "  cd /home/tr4m0ryp/Projects/carbon_footrpint_model"
echo "  python analyze_layer3_output.py"
echo ""
echo "Expected results:"
echo "  - 71,695 total scenarios"
echo "  - 5 strategies per preprocessing path"
echo "  - Geographic diversity across 40+ regions"
echo "  - Distance range: 1,500 - 15,000 km"
echo "  - Supply chain types: 20% medium, 80% long haul"
echo ""
