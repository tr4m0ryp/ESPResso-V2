# Task 06: Generator

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. Layer 3's generator is the core module that takes a
Layer 2 record and produces a Layer 3 record with transport legs. V2
generates one record with a leg array (not 5 scenario variants). It also
supports regeneration with feedback for the two-pass validation flow.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_3/core/generator.py` -- Current V1 generator
  to be rewritten. Note the TransportScenarioGenerator class structure,
  generate_scenarios_for_record(), and regenerate_with_feedback()
- `data/data_generation/layer_3/models/models.py` -- V2 TransportLeg and
  Layer3Record dataclasses (from task 01)
- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Section 5 for
  generation design, section 4.2 for leg structure

## Dependencies

- Task 01 (TransportLeg, Layer3Record models)
- Task 04 (PromptBuilder for prompt assembly)
- Task 05 (Layer3Client for API calls)

## The task

Rewrite `core/generator.py` in place.

### New TransportGenerator class

```python
class TransportGenerator:
    def __init__(self, config: Layer3Config, api_client: Layer3Client,
                 prompt_builder: PromptBuilder):
        self.config = config
        self.api_client = api_client
        self.prompt_builder = prompt_builder
        self._system_prompt = prompt_builder.get_system_prompt()

    def generate_for_record(
        self,
        record: Layer2Record,
        seed: int = 0,
        warehouse: str = "EU"
    ) -> Layer3Record:
        """
        Generate transport legs for a single Layer 2 record.

        Returns a Layer3Record with transport_legs and total_distance_km.
        Retries indefinitely on API failure (no rule-based fallback).
        """
        # 1. Build user prompt
        # 2. Call API with cached system prompt + user prompt
        # 3. Parse response into List[TransportLeg]
        # 4. Compute total_distance_km
        # 5. Assemble and return Layer3Record

    def regenerate_with_feedback(
        self,
        record: Layer2Record,
        failures: List[str],
        seed: int = 0,
        warehouse: str = "EU"
    ) -> Optional[Layer3Record]:
        """
        Regenerate transport legs with correction feedback.
        Used in the two-pass validation flow.
        """
        # 1. Build correction prompt with failure details
        # 2. Call API
        # 3. Parse and return new Layer3Record
        # Return None if regeneration fails after max retries
```

### Key changes from V1

- **Single record output**: `generate_for_record()` returns ONE Layer3Record
  (not a list of 5 TransportScenarios)
- **Leg parsing**: Parse raw API response dicts into TransportLeg objects
  using `TransportLeg.from_dict()`
- **Distance computation**: `total_distance_km = sum(leg.distance_km for leg in legs)`
  computed by the generator, not stored in API response
- **No V1 classes**: Remove TransportScenario, TransportScenarioGenerator,
  _api_scenario_to_transport_scenario, _create_layer3_record, validate_scenario,
  generate_batch, get_generation_stats
- **Feedback regeneration**: `regenerate_with_feedback()` takes a list of
  failure description strings (not RecordCheckResult objects)

### Leg parsing logic

```python
def _parse_legs(self, raw_legs: List[Dict]) -> List[TransportLeg]:
    """Parse raw API response into TransportLeg objects."""
    legs = []
    for raw in raw_legs:
        try:
            leg = TransportLeg.from_dict(raw)
            legs.append(leg)
        except (KeyError, ValueError, TypeError) as e:
            logger.warning("Failed to parse leg: %s", e)
    return legs
```

## Acceptance criteria

1. `generate_for_record(record)` returns a Layer3Record with transport_legs
2. `Layer3Record.total_distance_km` equals sum of leg distances
3. `regenerate_with_feedback(record, failures)` includes failure text in prompt
4. No V1 classes or methods remain
5. API retry with exponential backoff works (infinite retry for generation)
6. Leg parsing handles missing/malformed fields gracefully
