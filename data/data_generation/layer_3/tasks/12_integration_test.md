# Task 12: Integration Test

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. This task creates integration tests for the Layer 3
V2 pipeline. Tests verify that all components work together correctly
without requiring live API calls (use mocks for LLM calls).

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use pytest for testing
- Mock API calls, never hit real endpoints in tests
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_3/models/models.py` -- All V2 dataclasses
- `data/data_generation/layer_3/core/deterministic_validator.py` -- Validator
  to test
- `data/data_generation/layer_3/core/generator.py` -- Generator to test
- `data/data_generation/layer_3/prompts/builder.py` -- Prompt builder to test
- `data/data_generation/layer_3/io/output.py` -- Output writer to test

## Dependencies

- All previous tasks (01-11)

## The task

Create `tests/test_layer3_v2.py` with comprehensive tests.

### Test fixtures

```python
@pytest.fixture
def sample_transport_leg():
    """A valid transport leg for testing."""
    return TransportLeg(
        leg_index=0,
        material="cotton",
        from_step="raw_material",
        to_step="spinning",
        from_location="Suzhou, China",
        to_location="Shanghai, China",
        from_lat=31.30,
        from_lon=120.62,
        to_lat=31.23,
        to_lon=121.47,
        distance_km=85.0,
        transport_modes=["road"],
        reasoning="Direct road transport 85km via G15 expressway from Suzhou textile district to Shanghai processing facility."
    )

@pytest.fixture
def sample_layer3_record(sample_transport_leg):
    """A valid Layer3Record with transport legs."""
    # Include 3-4 legs showing a complete material chain
    # Include convergence at assembly step
    # Include warehouse terminus

@pytest.fixture
def sample_layer2_record():
    """A Layer2Record for generation testing."""
    return Layer2Record(
        category_id="CAT001",
        category_name="T-shirts",
        subcategory_id="SUB001",
        subcategory_name="Basic T-shirt",
        materials=["cotton", "polyester"],
        material_weights_kg=[0.15, 0.05],
        material_percentages=[75, 25],
        total_weight_kg=0.20,
        preprocessing_path_id="PP001",
        preprocessing_steps=["spinning", "weaving", "dyeing", "cutting", "sewing"],
        step_material_mapping={
            "cotton": ["spinning", "weaving", "dyeing", "cutting", "sewing"],
            "polyester": ["spinning", "weaving", "cutting", "sewing"]
        }
    )
```

### Test categories

**1. Model serialization (4 tests):**
- `test_transport_leg_roundtrip` -- to_dict/from_dict preserves all fields
- `test_layer3_record_roundtrip` -- to_dict/from_dict preserves all fields
  including nested transport_legs
- `test_compute_total_distance` -- sum of leg distances matches
- `test_layer3_record_json_serialization` -- transport_legs serialized as
  JSON string in to_dict()

**2. Deterministic validator (6 tests):**
- `test_valid_record_passes` -- well-formed record returns is_valid=True
- `test_missing_material_fails` -- record with a material not in any leg
  returns error
- `test_invalid_coordinates_fails` -- lat=200 returns error
- `test_distance_bounds_fails` -- leg with distance_km=0 returns error
- `test_leg_continuity_fails` -- gap in leg chain returns error
- `test_correction_recomputes_distance` -- validate_and_correct fixes
  mismatched total_distance_km

**3. Prompt builder (3 tests):**
- `test_system_prompt_loads` -- get_system_prompt() returns non-empty string
- `test_system_prompt_cached` -- second call returns same object (is check)
- `test_user_prompt_contains_materials` -- build_user_prompt() includes all
  material names from the record

**4. Output writer (2 tests):**
- `test_csv_output_schema` -- write_records() produces CSV with exactly 13
  columns in the correct order
- `test_transport_legs_json_in_csv` -- transport_legs column contains valid
  JSON that can be parsed back to leg dicts

**5. Generator with mock API (2 tests):**
- `test_generate_for_record_mock` -- mock API response, verify Layer3Record
  is assembled correctly with computed total_distance_km
- `test_regenerate_with_feedback_mock` -- mock API, verify correction prompt
  includes failure text

**6. Statistical validator (2 tests):**
- `test_duplicate_detection` -- same record twice flags is_duplicate=True
- `test_distance_outlier_detection` -- one extreme record after 20 normal
  ones flags is_outlier=True

### Mock API pattern

```python
@pytest.fixture
def mock_api_response():
    """Mock API response with valid transport legs."""
    return [
        {
            "leg_index": 0,
            "material": "cotton",
            "from_step": "raw_material",
            "to_step": "spinning",
            "from_location": "Suzhou, China",
            "to_location": "Shanghai, China",
            "from_lat": 31.30,
            "from_lon": 120.62,
            "to_lat": 31.23,
            "to_lon": 121.47,
            "distance_km": 85.0,
            "transport_modes": ["road"],
            "reasoning": "Direct road transport via G15 expressway."
        }
        # ... more legs
    ]
```

Use `unittest.mock.patch` to mock `Layer3Client.generate_transport_legs`.

## Acceptance criteria

1. All tests pass with `pytest tests/test_layer3_v2.py`
2. No tests require live API calls (all LLM calls are mocked)
3. At least 19 test functions covering all 6 categories
4. Fixtures are reusable and well-documented
5. Test names clearly describe what is being tested
6. No emojis in test names, docstrings, or assertions
