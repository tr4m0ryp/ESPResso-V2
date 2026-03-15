Rewrite `core/orchestrator.py` and `main.py` to coordinate all V2 Layer 4 components.

## What to remove

All V1 orchestration logic:
- `core/orchestrator.py` V1 class with multi-key pools and parallel workers
- `main.py` V1 entry point with NVIDIA-specific initialization
- Any references to `PackagingConfigGenerator`, `Nemotron4Client`, `PackagingDatabase`,
  `MultiKeyClientPool`

## core/orchestrator.py -- Layer4Orchestrator class

```python
class Layer4Orchestrator:
    def __init__(self, config: Optional[Layer4Config] = None):
        self.config = config or Layer4Config()
        self._reader: Optional[Layer3Reader] = None
        self._client: Optional[Layer4Client] = None
        self._prompt_builder: Optional[PromptBuilder] = None
        self._generator: Optional[PackagingGenerator] = None
        self._validator: Optional[PackagingValidator] = None
        self._writer: Optional[OutputWriter] = None
```

### initialize() -> None

Lazily initialize all components in order:

1. `self.config.ensure_directories()` -- create output and checkpoint dirs
2. `Layer3Reader(self.config)` -- verify input file exists
3. `Layer4Client(self.config)` -- verify API connection via `test_connection()`
4. `PromptBuilder(self.config)` -- load and cache system prompt
5. `PackagingGenerator(self.config, self._client, self._prompt_builder)`
6. `PackagingValidator(self.config)`
7. `OutputWriter(self.config)`

Log each component initialization. Raise on any failure.

### run_generation(max_records: Optional[int] = None, resume: bool = False) -> bool

Main generation loop. Returns True on success, False on critical failure.

**Resume logic:**
If `resume=True`, check for existing checkpoints via `self._writer.get_last_checkpoint_index()`.
If checkpoints exist, use `self._reader.read_from_checkpoint(last_index)` to skip
already-processed records. Log the resume point.

**Per-record pipeline:**

```
for index, record in enumerate(records):
    1. Generate packaging:
       result = self._generator.generate_for_record(record)
       if result is None:
           log warning, increment skip counter, continue

    2. Validate:
       validation = self._validator.validate(result)
       if not validation.is_valid:
           # Attempt regeneration with feedback
           result = self._generator.regenerate_with_feedback(
               record, validation.errors
           )
           if result is None:
               log warning, increment skip counter, continue
           # Re-validate the corrected result
           validation = self._validator.validate(result)
           if not validation.is_valid:
               log warning, increment skip counter, continue

    3. Collect result:
       batch_buffer.append(result)

    4. Log warnings (if any):
       for warning in validation.warnings:
           logger.warning("Record %d: %s", index, warning)

    5. Checkpoint (if batch_buffer reaches checkpoint_interval):
       self._writer.write_checkpoint(batch_buffer, checkpoint_index)
       batch_buffer.clear()
       checkpoint_index += 1

    6. Progress logging:
       Every 100 records, log: "Processed {n}/{total} records ({n/total:.1%})"
```

**After loop completes:**
1. Write any remaining records in batch_buffer as a final checkpoint.
2. Merge all checkpoints into the final output file via `self._writer.merge_checkpoints()`.
3. Get and log batch validation summary via `self._validator.validate_batch_summary()`.
4. Get and log output summary via `self._writer.get_output_summary()`.
5. Log final stats: total processed, total skipped, total written.

### _log_summary(batch_summary: Dict, output_summary: Dict) -> None

Log a formatted summary of the generation run:
- Records processed / skipped / written
- Duplicate percentage
- Mean packaging ratio
- Category usage percentages
- Distance-mass correlation

## main.py -- Entry point

Simple CLI entry point with argument parsing.

```python
def main():
    parser = argparse.ArgumentParser(
        description="Layer 4: Packaging Configuration Generator (V2)"
    )
    parser.add_argument("--max-records", type=int, default=None,
                        help="Maximum records to process (default: all)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last checkpoint")
    parser.add_argument("--test-connection", action="store_true",
                        help="Test API connection and exit")
    args = parser.parse_args()

    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s"
    )

    orchestrator = Layer4Orchestrator()
    orchestrator.initialize()

    if args.test_connection:
        print("Connection test passed.")
        return

    success = orchestrator.run_generation(
        max_records=args.max_records,
        resume=args.resume
    )
    sys.exit(0 if success else 1)
```

## Design rules

- Sequential processing only. No parallel workers, no thread pools. The pipeline is
  simple enough and API calls have no rate limits.
- Checkpointing is the only concurrency-safety mechanism. If the process is interrupted,
  resume from the last checkpoint.
- The orchestrator owns the retry-after-validation-failure flow: generate -> validate ->
  if invalid, regenerate_with_feedback -> re-validate -> skip if still invalid.
- Log progress every 100 records at INFO level.
- Log skips and validation warnings at WARNING level.
- Log critical failures (API down, input file missing) at ERROR level and return False.
- No emojis.

## Files to modify

- `core/orchestrator.py` -- complete rewrite
- `main.py` -- complete rewrite

## Reference

- Layer 3 orchestrator: `layer_3/core/orchestrator.py` (Layer3Orchestrator)
- Design doc: `layer_4/DESIGN_V2.md` sections 7.4 (Processing Pipeline) and 7.1 (File Structure)
