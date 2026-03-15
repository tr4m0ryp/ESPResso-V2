# Task 07: Sampled Reward Scorer

## Codebase context

ESPResso-V2 Layer 5 V2 replaces per-record reward scoring with sampled scoring.
Instead of making 1M LLM API calls (one per record), we score only 1-5% of
records and use the sample distribution to estimate dataset-level quality. This
cuts the biggest bottleneck from ~28 hours to ~1 hour for 1M records.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/clients/api_client.py` -- `generate_reward_score()` method
  and `_build_reward_prompt()` for V1 reward scoring logic
- `data/data_generation/layer_5/config/config.py` -- V2 config with `reward_sample_rate`
  and `should_sample_for_reward()` (from task 02)
- `data/data_generation/layer_5/models/models.py` -- SampledRewardResult (from task 01)

## Dependencies

- Task 01 (SampledRewardResult model)
- Task 02 (Layer5Config with reward sampling settings)

## The task

Create `core/sampled_reward_scorer.py` with a `SampledRewardScorer` class.

### SampledRewardScorer class

```python
class SampledRewardScorer:
    def __init__(self, config: Layer5Config, api_client: Layer5Client):
        self.config = config
        self.api_client = api_client
        self.sampled_scores: List[float] = []
        self.total_records_seen: int = 0
        self.total_records_sampled: int = 0

    def score_if_sampled(self, record: CompleteProductRecord, record_index: int,
                         total_records: int) -> SampledRewardResult:
        """Score a record if it falls within the sample.

        Uses config.should_sample_for_reward() to determine if this record
        should be scored. If not sampled, returns a SampledRewardResult with
        was_sampled=False and the current dataset quality estimate.

        Args:
            record: Complete product record
            record_index: Index of this record in the dataset (0-based)
            total_records: Total number of records in the dataset

        Returns:
            SampledRewardResult with score (if sampled) or estimate (if not)
        """

    def _score_record(self, record: CompleteProductRecord) -> float:
        """Score a single record via the LLM reward API.

        Builds a reward prompt, calls api_client.generate_reward_score(),
        and returns the score. Uses infinite retry (handled by api_client).

        Args:
            record: Record to score

        Returns:
            Reward score between 0.0 and 1.0
        """

    def _build_reward_context(self, record: CompleteProductRecord) -> str:
        """Build compact context string for reward scoring.

        Format:
        Product: {subcategory_name} ({category_name})
        Weight: {total_weight_kg}kg
        Materials: {mat1} ({pct1}%), {mat2} ({pct2}%), ...
        Processing: {step1}, {step2}, ...
        Transport: {distance}km ({chain_type})
        Packaging: {mass}kg ({categories})

        Args:
            record: Record to format

        Returns:
            Compact context string
        """

    def get_dataset_quality_estimate(self) -> Optional[float]:
        """Get current dataset quality estimate from sampled scores.

        Returns the mean of all sampled scores, or None if no records
        have been sampled yet.
        """

    def get_quality_interpretation(self, score: float) -> str:
        """Interpret a reward score.

        Returns:
            "High quality" if score >= 0.8
            "Acceptable" if score >= 0.6
            "Marginal" if score >= 0.4
            "Low quality" if score < 0.4
        """

    def get_sampling_summary(self) -> Dict[str, Any]:
        """Get summary of sampling results.

        Returns dict with:
            total_records_seen: int
            total_records_sampled: int
            sample_rate_actual: float (sampled/seen)
            mean_score: float
            median_score: float
            stdev_score: float
            min_score: float
            max_score: float
            quality_distribution: Dict[str, int]  # counts per quality tier
        """
```

### Implementation notes

- `score_if_sampled()` increments `total_records_seen` every call
- If sampled: call `_score_record()`, append to `sampled_scores`, increment `total_records_sampled`
- If not sampled: return result with `was_sampled=False`, `dataset_estimated_quality=self.get_dataset_quality_estimate()`
- The reward prompt reuses the V1 prompt logic from `api_client._build_reward_prompt()`
  but the context is built by `_build_reward_context()`

## Acceptance criteria

1. With `reward_sample_rate=0.03`, approximately 3% of records are scored
2. `score_if_sampled(record, 0, 1000).was_sampled` is True (first record always sampled)
3. `score_if_sampled(record, 1, 1000).was_sampled` is False
4. `get_dataset_quality_estimate()` returns None before any sampling
5. After 10 sampled records, `get_dataset_quality_estimate()` returns mean of scores
6. `get_sampling_summary()` returns correct statistics
7. Non-sampled records get `dataset_estimated_quality` populated from current estimate

## Files to create

- `core/sampled_reward_scorer.py`

## Reference

- V1 reward scoring: `layer_5/clients/api_client.py` methods `generate_reward_score()`,
  `_build_reward_prompt()`, `_extract_reward_score()`
