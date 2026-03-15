# Task 11: Main Entry Point

## Codebase context

ESPResso-V2 Layer 5 V2 simplifies the CLI entry point. Many V1 flags are removed
because per-layer validation is no longer done (no `--no-semantic`, no
`--plausibility-*` thresholds). New flags for coherence batch size and reward
sample rate are added.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Logging via `logging.getLogger(__name__)`
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/main.py` -- Current V1 entry point to rewrite
- `data/data_generation/layer_4/main.py` -- Pattern for simple entry point
- `data/data_generation/layer_3/main.py` -- Pattern for entry point with more options

## Dependencies

- Task 02 (Layer5Config V2)
- Task 10 (Layer5Orchestrator V2)

## The task

Rewrite `main.py` with V2 CLI arguments.

### Remove

- `--plausibility-accept`, `--plausibility-review` flags
- `--reward-accept`, `--reward-review` flags
- `--no-semantic` flag
- `--no-reward` flag
- `--temperature-instruct`, `--temperature-reward` flags
- `--max-tokens-instruct`, `--max-tokens-reward` flags
- `--use-batch-mode` flag (V2 always uses batch mode)
- `--incremental-write` flag (hardcoded to batch_size)
- All V1 configuration override logic for removed flags

### Keep

- `--max-records` flag
- `--test-api` flag (renamed to `--test-connection`)
- `--output-dir` flag
- `--batch-size` flag
- `--verbose` / `--quiet` flags
- `--config` flag (if Layer5Config supports from_file)

### Add

```python
parser.add_argument(
    '--coherence-batch-size', type=int, default=50,
    help='Records per LLM coherence evaluation call (default: 50)'
)
parser.add_argument(
    '--reward-sample-rate', type=float, default=0.03,
    help='Fraction of records to score for reward (default: 0.03 = 3%%)'
)
parser.add_argument(
    '--no-passport', action='store_true',
    help='Skip passport verification (trust upstream validators)'
)
parser.add_argument(
    '--no-reward-sampling', action='store_true',
    help='Skip reward sampling entirely'
)
parser.add_argument(
    '--resume', action='store_true',
    help='Resume from last checkpoint'
)
```

### Main function structure

```python
def main():
    args = parse_arguments()

    # Configure logging
    # ...

    logger.info("Layer 5 V2: Cross-Layer Coherence Checker starting")

    config = Layer5Config()

    # Apply CLI overrides
    if args.output_dir:
        config._output_dir_override = Path(args.output_dir)
    if args.batch_size:
        config.batch_size = args.batch_size
    if args.coherence_batch_size:
        config.coherence_batch_size = args.coherence_batch_size
    if args.reward_sample_rate is not None:
        config.reward_sample_rate = args.reward_sample_rate
    if args.no_passport:
        config.passport_enabled = False
    if args.no_reward_sampling:
        config.reward_sample_rate = 0.0

    orchestrator = Layer5Orchestrator(config)
    orchestrator.initialize()

    if args.test_connection:
        # ...test and exit...

    result = orchestrator.run_pipeline(max_records=args.max_records)
    # ...report results...
    sys.exit(0 if result['success'] else 1)
```

## Acceptance criteria

1. `python main.py --help` shows V2 flags (coherence-batch-size, reward-sample-rate, etc.)
2. `python main.py --help` does NOT show V1 flags (plausibility-accept, no-semantic, etc.)
3. `python main.py --test-connection` tests API and exits
4. `python main.py --max-records 100` processes 100 records
5. `python main.py --coherence-batch-size 25` overrides default batch size
6. `python main.py --no-passport` disables passport verification
7. Entry point logs "Layer 5 V2" (not V1)
8. No emojis in log messages (V1 has emojis in several log lines -- remove them)

## Files to modify

- `main.py` -- complete rewrite

## Reference

- V1 main: `layer_5/main.py` (current file, has emojis in log messages)
- Layer 4 main: `layer_4/main.py`
