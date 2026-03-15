# Task 12: Integration Test

## Codebase context

ESPResso-V2 Layer 5 V2 needs an integration test that exercises the full 5-stage
pipeline with mock data. The test verifies that all components wire together
correctly: passport verification, coherence evaluation, statistical validation,
sampled reward scoring, and final decision making.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Use `pytest` for test framework
- Use `unittest.mock.patch` for API mocking
- Logging via `logging.getLogger(__name__)`
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_3/tests/` -- Pattern for integration tests (if exists)
- `data/data_generation/layer_5/models/models.py` -- V2 models (from task 01)
- `data/data_generation/layer_5/core/orchestrator.py` -- V2 orchestrator (from task 10)
- `data/data_generation/layer_5/core/decision_maker.py` -- V2 decision maker (from task 10)

## Dependencies

- All previous tasks (01-11)

## The task

Create `tests/test_integration.py` with end-to-end tests for the V2 pipeline.

### Test fixtures

```python
@pytest.fixture
def sample_records() -> List[CompleteProductRecord]:
    """Create 10 sample product records covering different scenarios."""
    # Record 1: Valid, coherent product (cotton t-shirt)
    # Record 2: Valid, coherent product (polyester jacket)
    # Record 3: Contradictory (silk + injection molding)
    # Record 4: Duplicate of Record 1
    # Record 5-10: Valid products with varying quality
```

Create records with all 22 fields populated realistically. Include passport
hash fields (compute them correctly for valid records, leave None for one record
to test missing passport handling).

### Test cases

```python
class TestPassportVerifier:
    def test_valid_passports_accepted(self, sample_records):
        """Records with correct passport hashes pass verification."""

    def test_missing_passport_flagged(self, sample_records):
        """Records with None passport hash are flagged."""

    def test_tampered_passport_rejected(self, sample_records):
        """Records with incorrect passport hash fail verification."""

    def test_disabled_passport_skips(self, sample_records):
        """When passport_enabled=False, all records pass."""


class TestCoherenceValidator:
    @patch('data.data_generation.layer_5.clients.api_client.Layer5Client')
    def test_batch_of_50_records(self, mock_client, sample_records):
        """50 records are sent in a single LLM call."""

    @patch('data.data_generation.layer_5.clients.api_client.Layer5Client')
    def test_api_failure_returns_defaults(self, mock_client, sample_records):
        """API failure returns default results (no exception)."""


class TestStatisticalValidator:
    def test_duplicate_detection(self, sample_records):
        """Duplicate records are flagged."""

    def test_outlier_detection(self, sample_records):
        """Statistical outliers are flagged after sufficient data."""

    def test_cross_layer_correlation(self, sample_records):
        """Cross-layer correlations are computed after 100+ records."""


class TestSampledRewardScorer:
    @patch('data.data_generation.layer_5.clients.api_client.Layer5Client')
    def test_sampling_rate(self, mock_client, sample_records):
        """Only ~3% of records are actually scored."""

    def test_non_sampled_get_estimate(self, sample_records):
        """Non-sampled records get dataset quality estimate."""


class TestDecisionMaker:
    def test_passport_failure_rejects(self):
        """Failed passport causes rejection."""

    def test_high_coherence_accepts(self):
        """Coherence >= 0.85 with no issues accepts."""

    def test_low_coherence_rejects(self):
        """Coherence < 0.70 rejects."""

    def test_duplicate_rejects(self):
        """Duplicate record is rejected."""

    def test_review_range(self):
        """Coherence 0.70-0.85 goes to review."""


class TestEndToEnd:
    @patch('data.data_generation.layer_5.clients.api_client.Layer5Client')
    def test_full_pipeline(self, mock_client, sample_records, tmp_path):
        """Full pipeline processes records through all 5 stages."""
        # Mock the API client to return coherence JSON
        # Run orchestrator with max_records=10
        # Verify output files exist
        # Verify accepted/review/rejected counts make sense
```

### Mock API responses

Create helper functions that return realistic LLM responses:

```python
def mock_coherence_response(record_ids: List[str]) -> str:
    """Generate a mock JSON response for coherence evaluation."""
    result = {}
    for rid in record_ids:
        result[rid] = {
            "lifecycle_coherence_score": 0.88,
            "cross_layer_contradiction_score": 0.92,
            "overall_coherence_score": 0.90,
            "contradictions_found": [],
            "recommendation": "accept"
        }
    return json.dumps(result)

def mock_reward_response() -> str:
    """Generate a mock reward score response."""
    return "Score: 0.82\nJustification: Realistic textile product data."
```

## Acceptance criteria

1. `pytest tests/test_integration.py` passes
2. All 5 pipeline stages are tested
3. Decision logic is tested for all 3 outcomes (accept, review, reject)
4. API calls are mocked (no actual LLM calls during tests)
5. Duplicate detection is tested
6. Passport verification is tested (valid, missing, tampered, disabled)
7. Sampling rate is verified (~3%)
8. End-to-end test produces output files

## Files to create

- `tests/__init__.py`
- `tests/test_integration.py`

## Reference

- pytest documentation: standard pytest patterns
- unittest.mock: standard mock patterns
