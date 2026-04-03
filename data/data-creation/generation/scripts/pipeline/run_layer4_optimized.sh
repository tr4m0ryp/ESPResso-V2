#!/bin/bash

# Layer 4 Optimized Execution Script
# This script runs the optimized Layer 4 generation with 150 parallel workers

echo "============================================================"
echo "Layer 4 Packaging Configuration Generation (Optimized)"
echo "============================================================"
echo ""

# Change to project directory
cd /home/tr4m0ryp/Projects/carbon_footrpint_model

# Activate virtual environment if needed
# source venv/bin/activate

echo "Configuration:"
echo "  - Input: Layer 3 transport scenarios"
echo "  - Output: Layer 4 complete dataset (Layer 3 + packaging data)"
echo "  - Workers: 150 parallel workers"
echo "  - API Keys: 5 keys with round-robin distribution"
echo "  - Rate Limit: 200 req/min (5 keys × 40 req/min)"
echo ""

# Set parallel processing configuration
export PARALLEL_WORKERS="150"
export RATE_LIMIT_PER_KEY="40"

# Verify API keys are set
api_key_count=1  # UVA uses a single key
echo "API keys configured: $api_key_count"

# Input file (Layer 3 output)
LAYER3_INPUT="/home/tr4m0ryp/Projects/carbon_footrpint_model/data/datasets/pre-model/generated/layer_3/layer_3_transport_scenarios.parquet"

if [ ! -f "$LAYER3_INPUT" ]; then
    echo "ERROR: Layer 3 output file not found: $LAYER3_INPUT"
    echo "Please run Layer 3 generation first."
    exit 1
fi

record_count=$(wc -l < "$LAYER3_INPUT")
echo "Layer 3 records found: $record_count"
echo ""

echo "Starting generation..."
echo "Monitor progress: tail -f layer4_*.log"
echo ""

# Create output directory if it doesn't exist
mkdir -p /home/tr4m0ryp/Projects/carbon_footrpint_model/data/datasets/pre-model/generated/layer_4

# Run the generation with checkpointing
cd data/data_generation
python layer_4/main.py \
    --input "$LAYER3_INPUT" \
    --parallel-workers 150 \
    --checkpoint-interval 500 \
    --verbose

echo ""
echo "============================================================"
echo "Generation Complete!"
echo "============================================================"
echo ""
echo "Output directory: data/datasets/pre-model/generated/layer_4/"
echo ""
echo "Expected results:"
echo "  - Complete dataset with packaging configurations"
echo "  - 2 packaging configs per Layer 3 record"
echo "  - Optimized for cost and protection scenarios"
echo ""
echo "Next steps:"
echo "  - Validate output with: python layer_4/validator.py"
echo "  - Analyze results with custom analysis scripts"
echo ""
