# Task 11: Orchestrator

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. The orchestrator coordinates all Layer 3 components:
reading input, generating transport legs, validating (deterministic,
corrective, semantic), writing output, and running statistical validation
on the full batch. It supports parallel processing via the shared
ParallelProcessor.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for configuration
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_3/core/orchestrator.py` -- Current V1
  orchestrator to be rewritten. Note the initialization flow, parallel
  processing, two-pass reality check, and checkpoint handling
- `data/data_generation/shared/parallel_processor.py` -- ParallelProcessor
  class with rate limiting, pause/resume, and progress tracking
- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Section 10 for
  pipeline flow

## Dependencies

- All previous tasks (01-10)

## The task

Rewrite `core/orchestrator.py` in place.

### New Layer3Orchestrator class

```python
class Layer3Orchestrator:
    def __init__(self, config: Optional[Layer3Config] = None):
        self.config = config or Layer3Config()
        # Components (initialized lazily)
        self._layer2_reader = None
        self._api_client = None
        self._generator = None
        self._deterministic_validator = None
        self._semantic_validator = None
        self._statistical_validator = None
        self._prompt_builder = None
        self._output_writer = None
        self._progress_tracker = None

    def initialize(self) -> None:
        """Initialize all V2 components."""
        # 1. Ensure directories
        # 2. Initialize Layer2DataReader
        # 3. Initialize Layer3Client
        # 4. Initialize PromptBuilder
        # 5. Initialize TransportGenerator
        # 6. Initialize DeterministicValidator
        # 7. Initialize SemanticValidator
        # 8. Initialize StatisticalValidator
        # 9. Initialize OutputWriter + ProgressTracker

    def run_generation(self, batch_size=None, max_records=None,
                       resume_from_checkpoint=None,
                       parallel_workers=None,
                       requests_per_minute=None) -> bool:
        """Run the complete V2 pipeline."""
```

### Pipeline flow (per record)

```
1. Read Layer 2 record
2. generator.generate_for_record(record, seed, warehouse)
   -> Layer3Record
3. deterministic_validator.validate_and_correct(record)
   -> ValidationResult
   - If errors: log and skip record
   - If corrections: use corrected_record
4. semantic_validator.validate(record)
   -> SemanticValidationResult
   - If "reject": regenerate_with_feedback(), re-validate
   - If still "reject": discard with justification
5. output_writer.write_records([record])
6. statistical_validator.validate_record(record)
   -> StatisticalValidationResult (logged, not blocking)
```

### Parallel processing

Use `shared/parallel_processor.py` ParallelProcessor:

```python
def _process_single_record(self, record: Layer2Record) -> Optional[Layer3Record]:
    """Process one record through the full pipeline."""
    # Generate
    l3_record = self._generator.generate_for_record(record, seed, warehouse)
    # Deterministic validate + correct
    det_result = self._deterministic_validator.validate_and_correct(l3_record)
    if not det_result.is_valid:
        return None
    record_to_use = det_result.corrected_record or l3_record
    # Semantic validate (two-pass)
    sem_result = self._semantic_validator.validate(record_to_use)
    if sem_result.recommendation == "reject":
        regen = self._generator.regenerate_with_feedback(
            record, sem_result.issues_found
        )
        if regen:
            sem2 = self._semantic_validator.validate(regen)
            if sem2.recommendation != "reject":
                return regen
        return None  # Discard
    return record_to_use
```

Thread-safe output writing with a lock (same pattern as V1).

### Checkpointing

- Write checkpoint after every N records (config.checkpoint_interval)
- Support resuming from a checkpoint
- Checkpoint files use V2 schema

### Statistical validation (after batch)

After all records are written, run statistical_validator.get_batch_summary()
and log the results. Flag any issues found.

### Key changes from V1

- Remove: Layer3Controller, QualityMetrics, V1 RealityChecker integration,
  TransportScenarioValidator, _scenario_to_transport_scenario
- Remove: 5-scenario-per-record logic, scenario ID handling
- Add: DeterministicValidator, SemanticValidator, StatisticalValidator
- Add: PromptBuilder initialization with cached system prompt
- Keep: ParallelProcessor integration, checkpoint flow, progress tracking

## Acceptance criteria

1. `Layer3Orchestrator().initialize()` sets up all V2 components
2. `run_generation()` processes records through the full pipeline
3. Parallel processing with ParallelProcessor works
4. Deterministic validation rejects invalid records
5. Semantic validation triggers regeneration on "reject"
6. Statistical validation runs on the full batch
7. Checkpointing saves/resumes with V2 schema
8. No V1 classes or methods remain (Controller, 5-scenario logic, etc.)
