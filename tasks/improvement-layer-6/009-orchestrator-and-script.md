# Task 009: Enrichment Orchestrator and Entry Script

## Objective
Create the main orchestrator that coordinates the LLM enrichment pipeline: loads data, batches records, calls the LLM client, validates results, checkpoints progress, retries failures, and writes the final enriched dataset. Also create the CLI entry script.

## Scope
**Files to create:**
- `data/data_generation/layer_6/enrichment/orchestrator.py`
- `data/data_generation/scripts/run_layer6_enrichment.py`

**Files to read (not modify):**
- `data/data_generation/layer_6/enrichment/config.py` -- EnrichmentConfig (task 001)
- `data/data_generation/layer_6/enrichment/data_joiner.py` -- join_transport_legs (task 002)
- `data/data_generation/layer_6/enrichment/prompt_builder.py` -- prompts (task 003)
- `data/data_generation/layer_6/enrichment/client.py` -- EnrichmentClient (task 005)
- `data/data_generation/layer_6/enrichment/validator.py` -- validation (task 006)
- `data/data_generation/layer_5/core/orchestrator.py` -- reference for checkpoint pattern
- `data/data_generation/layer_5/io/writer_incremental.py` -- reference for temp file pattern
- `tasks/improvement-layer-6/rules.md`

**Out of scope:**
- Modifying any existing Layer 6 calculation code
- The individual enrichment components (tasks 001-006)

## Dependencies
- **Requires:** 002, 003, 005, 006
- **Produces:** The enrichment orchestrator and entry point. Produces the pre_layer6_enriched.parquet file when run.

## Technical Details

### Orchestrator Class

```python
class EnrichmentOrchestrator:
    def __init__(self, config: EnrichmentConfig):
        self.config = config
        self.client = EnrichmentClient(config)
        self.collector = FailedRecordCollector()

    def run(self) -> str:
        """Run full enrichment pipeline. Returns output path."""
```

### Pipeline Flow

1. **Load data**: Call `join_transport_legs()` to get merged DataFrame.
2. **Check for resume**: Scan `{output_dir}/temp_files/` for existing checkpoint files. Load completed record_ids. Filter DataFrame to unprocessed records only.
3. **Batch loop**: Process records in batches of `config.batch_size` (20):
   a. Build batch prompt via `build_batch_prompt()`.
   b. Call `client.extract_transport_distances()`.
   c. Validate each result via `validate_extraction()`.
   d. Store valid results. Add failures to `FailedRecordCollector`.
   e. Every `config.checkpoint_interval` (5000) records, write checkpoint.
4. **Retry pass**: Take all failed records from collector, rebatch, and retry once. Records that fail twice are logged and skipped (fail-open per D4).
5. **Merge checkpoints**: Combine all temp files into single DataFrame.
6. **Add enriched columns**: Merge extracted mode distances (road_km, sea_km, etc.) back into the joined DataFrame.
7. **Write output**: Save as `pre_layer6_enriched.parquet` (gzip compressed).
8. **Write summary**: JSON with stats (total processed, passed validation, failed, retried, skipped, duration).

### Checkpoint Format
Each checkpoint is a CSV in `{output_dir}/temp_files/`:
- Filename: `enrichment_batch_{batch_num}.csv`
- Columns: `record_id, road_km, sea_km, rail_km, air_km, inland_waterway_km, is_valid`

### Resume Logic
On startup:
1. List all `enrichment_batch_*.csv` files in temp_files/
2. Load all, collect record_ids where is_valid=True
3. Filter input DataFrame to exclude already-processed records
4. Log: "Resuming from record {N}, {M} already processed"

### Error Handling
- API call failure after 5 retries: skip entire batch, log record_ids, add to collector
- JSON parse failure: treat as API failure, retry
- Validation failure: add to collector for retry pass
- Unexpected exception: checkpoint current progress, re-raise

### Entry Script (run_layer6_enrichment.py)
Minimal CLI script:
```python
"""Run Layer 6 transport distance enrichment."""
import argparse
import logging
from data.data_generation.layer_6.enrichment.config import EnrichmentConfig
from data.data_generation.layer_6.enrichment.orchestrator import EnrichmentOrchestrator

def main():
    parser = argparse.ArgumentParser(description='Layer 6 Transport Enrichment')
    parser.add_argument('--batch-size', type=int, default=20)
    parser.add_argument('--checkpoint-interval', type=int, default=5000)
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    config = EnrichmentConfig(
        batch_size=args.batch_size,
        checkpoint_interval=args.checkpoint_interval,
    )
    orchestrator = EnrichmentOrchestrator(config)
    output_path = orchestrator.run()
    print(f'Enriched dataset saved to: {output_path}')

if __name__ == '__main__':
    main()
```

## Rules
- Read `tasks/improvement-layer-6/rules.md` before starting
- Only modify files listed in Scope
- No emojis in any output
- Maximum 300 lines per source file -- orchestrator.py will be the largest file. If it approaches 300, split the checkpoint/resume logic into a separate module.
- Use logging throughout, not print (except in entry script for final output path)

## Verification
- [ ] Orchestrator instantiates with default config
- [ ] Entry script parses arguments correctly
- [ ] Checkpoint files written with correct format
- [ ] Resume logic correctly skips already-processed records
- [ ] orchestrator.py stays under 300 lines (split if needed)
- [ ] No files modified outside Scope

## Stop Conditions
- If any dependency module (tasks 002, 003, 005, 006) has a different interface than expected
- If orchestrator exceeds 300 lines and needs splitting (do the split, don't stop)
