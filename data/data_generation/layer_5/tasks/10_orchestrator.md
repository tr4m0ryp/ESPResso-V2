# Task 10: Orchestrator

## Codebase context

ESPResso-V2 Layer 5 V2 orchestrator coordinates the new 5-stage pipeline:
passport verification, cross-layer coherence (LLM, 50 records/batch),
statistical quality, sampled reward scoring (3% sample), and final decision.
The V1 orchestrator is a 1225-line file that must be completely rewritten.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere
- Maximum 300 lines per file. If the orchestrator exceeds ~200 lines,
  split into `core/orchestrator.py` (main class) and `core/decision_maker.py`
  (final decision logic)

## Reference files to study

- `data/data_generation/layer_5/core/orchestrator.py` -- V1 orchestrator (1225 lines, to be replaced)
- `data/data_generation/layer_4/core/orchestrator.py` -- Pattern for Layer4Orchestrator
- `data/data_generation/layer_3/core/orchestrator.py` -- Pattern for Layer3Orchestrator

## Dependencies

- Task 01 (all V2 models)
- Task 02 (Layer5Config V2)
- Task 03 (PassportVerifier)
- Task 05 (CoherenceValidator)
- Task 06 (StatisticalValidator V2)
- Task 07 (SampledRewardScorer)
- Task 08 (Layer5Client V2)
- Task 09 (IncrementalValidationOutputWriter V2)

## The task

Rewrite `core/orchestrator.py` and create `core/decision_maker.py`.

### core/orchestrator.py -- Layer5Orchestrator class

```python
class Layer5Orchestrator:
    def __init__(self, config: Optional[Layer5Config] = None):
        self.config = config or Layer5Config()
        self._client: Optional[Layer5Client] = None
        self._passport_verifier: Optional[PassportVerifier] = None
        self._coherence_validator: Optional[CoherenceValidator] = None
        self._statistical_validator: Optional[StatisticalValidator] = None
        self._reward_scorer: Optional[SampledRewardScorer] = None
        self._decision_maker: Optional[DecisionMaker] = None
        self._writer: Optional[IncrementalValidationOutputWriter] = None
        self.stats = ValidationPipelineStats()
```

### initialize() -> None

Lazily initialize all components in order:

1. `self.config.ensure_directories()`
2. `Layer5Client(self.config)` -- verify API via `test_connection()`
3. `PassportVerifier(self.config)`
4. `CoherenceValidator(self.config, self._client)`
5. `StatisticalValidator(self.config)`
6. `SampledRewardScorer(self.config, self._client)`
7. `DecisionMaker(self.config)`
8. `IncrementalValidationOutputWriter(self.config)`

Log each initialization step. Raise on any failure.

### run_pipeline(max_records: Optional[int] = None) -> Dict[str, Any]

Main pipeline method. Returns result dict with success flag and statistics.

**Data loading:**
Load Layer 4 output (complete dataset) via pandas. Parse JSON fields.
Create `CompleteProductRecord` objects.

**Processing loop (batch-based):**

```
for batch_start in range(0, total_records, config.batch_size):
    batch = records[batch_start:batch_start + config.batch_size]

    # Stage 1: Passport Verification (parallel, fast)
    passport_results = self._passport_verifier.verify_batch(batch)

    # Stage 2: Cross-Layer Coherence (LLM, batched 50)
    # Chunk batch into groups of config.coherence_batch_size
    coherence_results = {}
    for chunk_start in range(0, len(batch), config.coherence_batch_size):
        chunk = batch[chunk_start:chunk_start + config.coherence_batch_size]
        chunk_results = self._coherence_validator.validate_batch(chunk)
        coherence_results.update(chunk_results)

    # Stage 3: Statistical Quality (sequential, per-record)
    statistical_results = {}
    for record in batch:
        stat_result = self._statistical_validator.validate_record(record)
        statistical_results[record.subcategory_id] = stat_result

    # Stage 4: Sampled Reward Scoring (per-record, most skipped)
    reward_results = {}
    for i, record in enumerate(batch):
        global_index = batch_start + i
        reward_result = self._reward_scorer.score_if_sampled(
            record, global_index, total_records
        )
        reward_results[record.subcategory_id] = reward_result

    # Stage 5: Final Decision
    batch_validation_results = []
    for record in batch:
        rid = record.subcategory_id
        result = self._decision_maker.decide(
            record=record,
            passport=passport_results.get(rid),
            coherence=coherence_results.get(rid),
            statistical=statistical_results.get(rid),
            reward=reward_results.get(rid)
        )
        batch_validation_results.append(result)

    # Write batch results
    self._writer.write_batch(batch_validation_results, batch_num)

    # Update stats
    self._update_stats(batch_validation_results)

    # Progress logging every batch
    self._log_progress(batch_num, total_batches)
```

**After loop:**
1. Merge temp files via `self._writer.merge_final_outputs(summary)`
2. Write validation summary
3. Log final statistics
4. Return result dict

### test_api_connection() -> bool

Test API connection and return True/False.

### _update_stats() and _log_progress()

Update ValidationPipelineStats counters. Log progress every batch.

---

### core/decision_maker.py -- DecisionMaker class

```python
class DecisionMaker:
    def __init__(self, config: Layer5Config):
        self.config = config

    def decide(
        self,
        record: CompleteProductRecord,
        passport: Optional[PassportVerificationResult],
        coherence: Optional[CrossLayerCoherenceResult],
        statistical: Optional[StatisticalQualityResult],
        reward: Optional[SampledRewardResult]
    ) -> CompleteValidationResult:
        """Make final accept/review/reject decision for a record.

        Decision logic:
        1. REJECT if passport verification failed (any layer hash invalid)
        2. REJECT if coherence overall_coherence_score < config.coherence_review_threshold (0.70)
        3. REJECT if record is a duplicate
        4. ACCEPT if coherence >= config.coherence_accept_threshold (0.85)
           AND no statistical issues AND passport valid
        5. REVIEW otherwise

        Final score = overall_coherence_score (or 0.0 if no coherence result)

        Args:
            record: The product record
            passport: Passport verification result (or None)
            coherence: Cross-layer coherence result (or None)
            statistical: Statistical quality result (or None)
            reward: Sampled reward result (or None)

        Returns:
            CompleteValidationResult with final decision
        """
```

### Decision factors

Build a `decision_factors: List[str]` explaining why the decision was made:
- "passport_valid" or "passport_failed:layer_3"
- "coherence_score:0.92" or "coherence_below_threshold:0.65"
- "duplicate_detected" or "outlier:weight"
- "sampled_reward:0.85" (only if sampled)

### Record ID generation

Use `f"{record.subcategory_id}_{record.preprocessing_path_id}"` as record_id.

## Acceptance criteria

1. `Layer5Orchestrator(config).initialize()` creates all components without error
2. `run_pipeline(max_records=100)` processes 100 records through all 5 stages
3. Results are written to accepted/review/rejected CSV files
4. Coherence evaluation uses batches of 50 (not 10)
5. Reward scoring samples ~3% of records
6. Passport failures cause immediate rejection
7. Duplicates cause immediate rejection
8. orchestrator.py is under 300 lines
9. decision_maker.py is under 150 lines

## Files to create

- `core/orchestrator.py` -- complete rewrite
- `core/decision_maker.py` -- new file

## Files to remove

None directly (the old orchestrator is overwritten).

## Reference

- V1 orchestrator: `layer_5/core/orchestrator.py` (1225 lines, being replaced)
- Layer 4 orchestrator: `layer_4/core/orchestrator.py`
