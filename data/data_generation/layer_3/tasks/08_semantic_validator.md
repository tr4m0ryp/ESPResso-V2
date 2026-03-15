# Task 08: Semantic Validator

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. The semantic validator uses an LLM to evaluate
whether generated transport legs are geographically and logistically
plausible. It catches errors that pass structural checks but are
geographically wrong. This is Stage 3 of the validation pipeline
(after deterministic and corrective).

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/core/semantic_validator.py` -- If it
  exists, study the pattern. Otherwise use the SemanticValidationResult
  in layer_5/models/models.py as a reference
- `data/data_generation/shared/reality_checker.py` -- RealityChecker
  class with two-pass check_batch() flow
- `data/data_generation/layer_3/prompts/reality_check_prompts.py` --
  Current V1 reality check prompts to be updated for V2 leg format
- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Section 9.4 for
  semantic validation design

## Dependencies

- Task 01 (Layer3Record, SemanticValidationResult models)
- Task 05 (Layer3Client for LLM calls)

## The task

Create `core/semantic_validator.py` with a `SemanticValidator` class.

### SemanticValidator class

```python
class SemanticValidator:
    def __init__(self, config: Layer3Config, api_client: Layer3Client):
        self.config = config
        self.api_client = api_client

    def validate(self, record: Layer3Record) -> SemanticValidationResult:
        """
        Evaluate plausibility of a record's transport legs using LLM.

        Checks:
        - Location plausibility (is this step realistic for this city?)
        - Route plausibility (does the route make geographic sense?)
        - Mode plausibility (are modes appropriate for distance/geography?)

        Returns SemanticValidationResult with scores and recommendation.
        """

    def validate_batch(
        self, records: List[Layer3Record]
    ) -> List[SemanticValidationResult]:
        """Validate a batch of records."""
```

### Validation prompt

Build a prompt that presents the transport legs to the LLM and asks it
to evaluate three aspects:

1. **Location plausibility** (0.0-1.0): Are the assigned locations
   realistic for the processing steps and materials? (e.g., silk spinning
   in a city known for silk production is plausible; silk spinning in a
   desert city is not)

2. **Route plausibility** (0.0-1.0): Do the transport routes make
   geographic sense? (e.g., shipping from China to Vietnam via the
   Atlantic is implausible)

3. **Mode plausibility** (0.0-1.0): Are the transport modes appropriate
   for the distances and geography? (e.g., inland_waterway for a route
   with no navigable rivers is implausible)

The LLM should return a JSON object with scores and issues found.

### Recommendation logic

```python
def _compute_recommendation(self, result: SemanticValidationResult) -> str:
    avg_score = (
        result.location_plausibility_score +
        result.route_plausibility_score +
        result.mode_plausibility_score
    ) / 3
    if avg_score >= self.config.semantic_accept_threshold:
        return "accept"
    elif avg_score >= self.config.semantic_review_threshold:
        return "review"
    else:
        return "reject"
```

### Update reality_check_prompts.py

Update `prompts/reality_check_prompts.py` to work with the V2 leg
format instead of V1 scenario format. The format_batch() function
should serialize transport legs, not flat scenario fields.

## Acceptance criteria

1. `validate(record)` returns a SemanticValidationResult
2. Result includes scores for location, route, and mode plausibility
3. Recommendation is "accept", "review", or "reject" based on thresholds
4. Issues found are listed as strings in the result
5. Config thresholds (semantic_accept_threshold, semantic_review_threshold)
   are used
6. Handles API errors gracefully (returns "review" recommendation on failure)
