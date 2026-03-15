Create `tests/test_layer4_v2.py` with pytest-based tests covering all V2 Layer 4 components.

Create the `tests/` directory with `__init__.py`.

## Fixtures

### sample_layer3_record() -> Dict[str, Any]

A valid Layer 3 record dict with realistic values:

```python
{
    "category_id": "cat-001",
    "category_name": "Tops",
    "subcategory_id": "sub-001",
    "subcategory_name": "T-Shirts",
    "materials": json.dumps(["Cotton", "Polyester"]),
    "material_weights_kg": json.dumps([0.15, 0.05]),
    "material_percentages": json.dumps([75.0, 25.0]),
    "total_weight_kg": 0.20,
    "preprocessing_path_id": "pp-001",
    "preprocessing_steps": json.dumps(["Spinning", "Weaving", "Dyeing"]),
    "step_material_mapping": json.dumps({"Cotton": ["Spinning", "Weaving"], "Polyester": ["Spinning"]}),
    "transport_legs": json.dumps([
        {
            "leg_index": 0,
            "material": "Cotton",
            "from_step": "Spinning",
            "to_step": "Weaving",
            "from_location": "Dhaka, Bangladesh",
            "to_location": "Chittagong, Bangladesh",
            "from_lat": 23.81,
            "from_lon": 90.41,
            "to_lat": 22.36,
            "to_lon": 91.78,
            "distance_km": 250.0,
            "transport_modes": ["road"],
            "reasoning": "Local road transport between processing facilities."
        },
        {
            "leg_index": 1,
            "material": "Cotton",
            "from_step": "Weaving",
            "to_step": "Warehouse",
            "from_location": "Chittagong Port, Bangladesh",
            "to_location": "Rotterdam, Netherlands",
            "from_lat": 22.33,
            "from_lon": 91.80,
            "to_lat": 51.92,
            "to_lon": 4.48,
            "distance_km": 14500.0,
            "transport_modes": ["sea", "road"],
            "reasoning": "Sea freight from Chittagong to Rotterdam via Suez Canal."
        }
    ]),
    "total_distance_km": 14750.0,
}
```

### sample_packaging_result() -> PackagingResult

```python
PackagingResult(
    paper_cardboard_kg=0.012,
    plastic_kg=0.006,
    other_kg=0.001,
    reasoning="Sea freight requires moisture barrier (polybag) and cardboard protection."
)
```

### sample_layer4_record(sample_layer3_record, sample_packaging_result)

Construct via `Layer4Record.from_layer3(sample_layer3_record, sample_packaging_result)`.

## Test categories

### 1. Model serialization (4 tests)

- `test_packaging_result_to_output_lists`: Verify `to_output_lists()` returns correct
  category names in fixed order and matching mass values.
- `test_packaging_result_total_mass`: Verify `total_mass_kg()` returns sum of 3 masses.
- `test_packaging_result_from_dict`: Verify `from_dict()` parses a dict correctly and
  rounds to 4 decimal places.
- `test_layer4_record_roundtrip`: Verify `to_dict()` produces JSON-serialized list/dict
  fields and all 16 keys are present.

### 2. Configuration (2 tests)

- `test_config_defaults`: Verify default values: temperature=0.3, max_tokens=300,
  min_packaging_ratio=0.005, max_packaging_ratio=0.15.
- `test_config_env_override`: Set `LAYER4_MIN_PKG_RATIO=0.01` in env, verify config
  picks it up. Clean up env after test.

### 3. Prompt builder (4 tests)

- `test_system_prompt_loads`: Verify `get_system_prompt()` returns a non-empty string
  containing "Paper/Cardboard" and "Plastic" and "Other".
- `test_system_prompt_cached`: Verify second call returns the same object (identity check
  with `is`).
- `test_user_prompt_includes_product`: Verify `build_user_prompt(record)` output contains
  "T-Shirts", "Tops", "0.2", "Cotton".
- `test_user_prompt_includes_transport`: Verify user prompt contains transport info
  extracted from legs: "sea", "road", "14750", "Dhaka", "Rotterdam".

### 4. Validator (6 tests)

- `test_valid_record_passes`: A well-formed Layer4Record passes validation with
  `is_valid=True` and empty errors.
- `test_negative_mass_fails`: Set `packaging_masses_kg=[-0.01, 0.006, 0.001]`, verify
  validation returns `is_valid=False` with error about negative mass.
- `test_zero_total_mass_fails`: Set all masses to 0.0, verify error about zero total.
- `test_wrong_category_count_fails`: Set `packaging_categories=["Paper/Cardboard", "Plastic"]`
  (only 2), verify error.
- `test_high_packaging_ratio_warns`: Set masses to 50% of product weight. Verify
  `is_valid=True` but warnings list is non-empty.
- `test_batch_summary_structure`: Process 5 records through validator, call
  `validate_batch_summary()`, verify all expected keys are present and types are correct.

### 5. Generator with mock API (3 tests)

Use `unittest.mock.patch` to mock `Layer4Client.generate_packaging`.

- `test_generate_for_record_success`: Mock returns valid JSON dict. Verify returned
  Layer4Record has correct packaging masses and all Layer 3 fields preserved.
- `test_generate_for_record_api_failure`: Mock raises exception. Verify `generate_for_record`
  returns None after retries.
- `test_regenerate_with_feedback`: Mock returns valid dict. Verify
  `regenerate_with_feedback` passes failure strings to `build_correction_prompt`.

### 6. IO layer (3 tests)

Use `tmp_path` fixture for temporary file operations.

- `test_writer_output_schema`: Write 3 records, read back the Parquet, verify 16 columns
  in correct order matching HEADERS.
- `test_writer_checkpoint_and_merge`: Write 2 checkpoints, call `merge_checkpoints()`,
  verify final output contains all records from both checkpoints.
- `test_reader_record_count`: Write a small Parquet with known row count, verify
  `get_record_count()` returns correct value.

## Mock API pattern

```python
@pytest.fixture
def mock_api_response():
    return {
        "paper_cardboard_kg": 0.012,
        "plastic_kg": 0.006,
        "other_kg": 0.001,
        "reasoning": "Standard packaging for lightweight apparel with sea transport."
    }

def test_generate_for_record_success(sample_layer3_record, mock_api_response):
    config = Layer4Config()
    with patch.object(Layer4Client, 'generate_packaging', return_value=mock_api_response):
        client = Layer4Client(config)
        builder = PromptBuilder(config)
        generator = PackagingGenerator(config, client, builder)
        result = generator.generate_for_record(sample_layer3_record)
        assert result is not None
        assert result.packaging_masses_kg == [0.012, 0.006, 0.001]
```

## Design rules

- No live API calls in tests. All API interactions are mocked.
- Use `tmp_path` for file IO tests to avoid polluting the filesystem.
- Use `monkeypatch` for environment variable tests to ensure cleanup.
- No emojis in test names, assertions, or comments.
- Import from full package paths: `from data.data_generation.layer_4.models.models import ...`

## Files to create

- `tests/__init__.py`
- `tests/test_layer4_v2.py`

## Reference

- Layer 3 test pattern: `layer_3/tests/test_layer3_v2.py`
- Design doc: `layer_4/DESIGN_V2.md` (all sections)
