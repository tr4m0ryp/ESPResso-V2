"""Integration tests for Layer 3 V2 pipeline. All LLM calls mocked."""
import copy, csv, json, sys
from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

_ROOT = Path(__file__).resolve().parents[5]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from data.data_generation.layer_3.models.models import Layer3Record, TransportLeg
from data.data_generation.layer_3.config.config import Layer3Config
from data.data_generation.layer_3.core.deterministic_validator import DeterministicValidator
from data.data_generation.layer_3.core.statistical_validator import StatisticalValidator
from data.data_generation.layer_3.io.layer2_reader import Layer2Record

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts" / "system"


def _leg(idx, mat, fs, ts, floc, tloc, flat, flon, tlat, tlon, dist, modes=None):
    """Helper to build a TransportLeg with less boilerplate."""
    return TransportLeg(
        leg_index=idx, material=mat, from_step=fs, to_step=ts,
        from_location=floc, to_location=tloc,
        from_lat=flat, from_lon=flon, to_lat=tlat, to_lon=tlon,
        distance_km=dist, transport_modes=modes or ["road"],
        reasoning="Test reasoning text that is long enough to pass the minimum length check easily.",
    )


# -- Fixtures ---------------------------------------------------------------

@pytest.fixture
def sample_transport_leg():
    return _leg(0, "cotton", "raw_material", "spinning",
                "Suzhou, China", "Shanghai, China", 31.30, 120.62, 31.23, 121.47, 85.0)


@pytest.fixture
def sample_layer3_record(sample_transport_leg):
    legs = [
        sample_transport_leg,
        _leg(1, "cotton", "spinning", "sewing",
             "Shanghai, China", "Hangzhou, China", 31.23, 121.47, 30.27, 120.15, 170.0),
        _leg(0, "polyester", "raw_material", "spinning",
             "Ningbo, China", "Hangzhou, China", 29.87, 121.55, 30.27, 120.15, 150.0),
        _leg(1, "polyester", "spinning", "sewing",
             "Hangzhou, China", "Hangzhou, China", 30.27, 120.15, 30.27, 120.15, 5.0),
    ]
    return Layer3Record(
        category_id="CAT001", category_name="T-shirts",
        subcategory_id="SUB001", subcategory_name="Basic T-shirt",
        materials=["cotton", "polyester"],
        material_weights_kg=[0.15, 0.05], material_percentages=[75, 25],
        total_weight_kg=0.20, preprocessing_path_id="PP001",
        preprocessing_steps=["spinning", "sewing"],
        step_material_mapping={"cotton": ["spinning", "sewing"],
                               "polyester": ["spinning", "sewing"]},
        transport_legs=legs, total_distance_km=sum(l.distance_km for l in legs),
    )


@pytest.fixture
def sample_layer2_record():
    return Layer2Record(
        category_id="CAT001", category_name="T-shirts",
        subcategory_id="SUB001", subcategory_name="Basic T-shirt",
        materials=["cotton", "polyester"],
        material_weights_kg=[0.15, 0.05], material_percentages=[75, 25],
        total_weight_kg=0.20, preprocessing_path_id="PP001",
        preprocessing_steps=["spinning", "weaving", "dyeing", "cutting", "sewing"],
        step_material_mapping={"cotton": ["spinning", "weaving", "dyeing", "cutting", "sewing"],
                               "polyester": ["spinning", "weaving", "cutting", "sewing"]},
    )


@pytest.fixture
def layer3_config():
    """Mock config with attributes used by validators."""
    cfg = MagicMock(spec=Layer3Config)
    cfg.min_leg_distance_km = 1.0
    cfg.max_leg_distance_km = 25000.0
    cfg.min_reasoning_length = 50
    cfg.coordinate_decimal_places = 2
    cfg.location_diversity_threshold = 0.30
    cfg.distance_outlier_zscore = 3.0
    cfg.mode_max_single_percentage = 0.80
    return cfg


@pytest.fixture
def mock_api_response():
    return [
        {"leg_index": 0, "material": "cotton", "from_step": "raw_material",
         "to_step": "spinning", "from_location": "Suzhou, China",
         "to_location": "Shanghai, China", "from_lat": 31.30, "from_lon": 120.62,
         "to_lat": 31.23, "to_lon": 121.47, "distance_km": 85.0,
         "transport_modes": ["road"],
         "reasoning": "Direct road transport via G15 expressway from Suzhou to Shanghai."},
        {"leg_index": 1, "material": "cotton", "from_step": "spinning",
         "to_step": "sewing", "from_location": "Shanghai, China",
         "to_location": "Hangzhou, China", "from_lat": 31.23, "from_lon": 121.47,
         "to_lat": 30.27, "to_lon": 120.15, "distance_km": 170.0,
         "transport_modes": ["road"],
         "reasoning": "Road transport via G60 highway from Shanghai to Hangzhou."},
    ]


# -- 1. Model serialization (4 tests) --------------------------------------

def test_transport_leg_roundtrip(sample_transport_leg):
    d = sample_transport_leg.to_dict()
    restored = TransportLeg.from_dict(d)
    assert restored.to_dict() == d


def test_layer3_record_roundtrip(sample_layer3_record):
    d = sample_layer3_record.to_dict()
    restored = Layer3Record.from_dict(d)
    assert len(restored.transport_legs) == len(sample_layer3_record.transport_legs)
    assert restored.total_distance_km == sample_layer3_record.total_distance_km
    for o, r in zip(sample_layer3_record.transport_legs, restored.transport_legs):
        assert r.to_dict() == o.to_dict()


def test_compute_total_distance(sample_layer3_record):
    expected = sum(l.distance_km for l in sample_layer3_record.transport_legs)
    assert sample_layer3_record.compute_total_distance() == expected


def test_layer3_record_json_serialization(sample_layer3_record):
    d = sample_layer3_record.to_dict()
    raw = d["transport_legs"]
    assert isinstance(raw, str)
    parsed = json.loads(raw)
    assert isinstance(parsed, list) and len(parsed) == len(sample_layer3_record.transport_legs)


# -- 2. Deterministic validator (6 tests) -----------------------------------

def test_valid_record_passes(sample_layer3_record, layer3_config):
    r = DeterministicValidator(layer3_config).validate(sample_layer3_record)
    assert r.is_valid is True and r.errors == []


def test_missing_material_fails(sample_layer3_record, layer3_config):
    sample_layer3_record.materials.append("silk")
    r = DeterministicValidator(layer3_config).validate(sample_layer3_record)
    assert r.is_valid is False
    assert any("silk" in e for e in r.errors)


def test_invalid_coordinates_fails(sample_layer3_record, layer3_config):
    sample_layer3_record.transport_legs[0].from_lat = 200.0
    r = DeterministicValidator(layer3_config).validate(sample_layer3_record)
    assert r.is_valid is False and any("from_lat" in e for e in r.errors)


def test_distance_bounds_fails(sample_layer3_record, layer3_config):
    sample_layer3_record.transport_legs[0].distance_km = 0.0
    r = DeterministicValidator(layer3_config).validate(sample_layer3_record)
    assert r.is_valid is False and any("distance_km" in e for e in r.errors)


def test_leg_continuity_fails(sample_layer3_record, layer3_config):
    sample_layer3_record.transport_legs[1].from_location = "Beijing, China"
    r = DeterministicValidator(layer3_config).validate(sample_layer3_record)
    assert r.is_valid is False and any("discontinuity" in e for e in r.errors)


def test_correction_recomputes_distance(sample_layer3_record, layer3_config):
    sample_layer3_record.total_distance_km = 999999.0
    # Short reasoning triggers a warning, which gates correction logic.
    sample_layer3_record.transport_legs[0].reasoning = "Short."
    r = DeterministicValidator(layer3_config).validate_and_correct(sample_layer3_record)
    assert r.corrected_record is not None
    expected = sum(l.distance_km for l in sample_layer3_record.transport_legs)
    assert abs(r.corrected_record.total_distance_km - expected) < 0.01
    assert any("Recomputed" in c for c in r.corrections_applied)


# -- 3. Prompt builder (3 tests) -------------------------------------------

def _make_builder():
    from data.data_generation.layer_3.prompts.builder import PromptBuilder
    cfg = MagicMock()
    cfg.system_prompts_dir = _PROMPTS_DIR
    return PromptBuilder(cfg)


def test_system_prompt_loads():
    prompt = _make_builder().get_system_prompt()
    assert isinstance(prompt, str) and len(prompt) > 0


def test_system_prompt_cached():
    b = _make_builder()
    assert b.get_system_prompt() is b.get_system_prompt()


def test_user_prompt_contains_materials(sample_layer2_record):
    prompt = _make_builder().build_user_prompt(sample_layer2_record)
    for mat in sample_layer2_record.materials:
        assert mat in prompt


# -- 4. Output writer (2 tests) --------------------------------------------

def test_csv_output_schema(sample_layer3_record, tmp_path):
    from data.data_generation.layer_3.io.output import OutputWriter, HEADERS
    cfg = MagicMock()
    cfg.output_path = tmp_path / "out.csv"
    cfg.checkpoint_dir = tmp_path / "ckpt"
    cfg.output_dir = tmp_path
    OutputWriter(cfg).write_records([sample_layer3_record], mode="write")
    with open(cfg.output_path, encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        assert rdr.fieldnames == HEADERS and len(rdr.fieldnames) == 13
        assert len(list(rdr)) == 1


def test_transport_legs_json_in_csv(sample_layer3_record, tmp_path):
    from data.data_generation.layer_3.io.output import OutputWriter
    cfg = MagicMock()
    cfg.output_path = tmp_path / "out.csv"
    cfg.checkpoint_dir = tmp_path / "ckpt"
    cfg.output_dir = tmp_path
    OutputWriter(cfg).write_records([sample_layer3_record], mode="write")
    with open(cfg.output_path, encoding="utf-8") as f:
        row = next(csv.DictReader(f))
    legs = json.loads(row["transport_legs"])
    assert isinstance(legs, list) and len(legs) == len(sample_layer3_record.transport_legs)
    for ld in legs:
        assert "leg_index" in ld and "material" in ld and "distance_km" in ld


# -- 5. Generator with mock API (2 tests) ----------------------------------

def test_generate_for_record_mock(sample_layer2_record, mock_api_response):
    from data.data_generation.layer_3.core.generator import TransportGenerator
    client = MagicMock()
    client.generate_transport_legs.return_value = mock_api_response
    builder = MagicMock()
    builder.get_system_prompt.return_value = "sys"
    builder.build_user_prompt.return_value = "usr"
    result = TransportGenerator(MagicMock(), client, builder).generate_for_record(sample_layer2_record)
    assert isinstance(result, Layer3Record)
    assert abs(result.total_distance_km - sum(l["distance_km"] for l in mock_api_response)) < 0.01
    assert len(result.transport_legs) == len(mock_api_response)


def test_regenerate_with_feedback_mock(sample_layer2_record, mock_api_response):
    from data.data_generation.layer_3.core.generator import TransportGenerator
    client = MagicMock()
    client.generate_transport_legs.return_value = mock_api_response
    builder = MagicMock()
    builder.get_system_prompt.return_value = "sys"
    builder.build_correction_prompt.return_value = "correction"
    cfg = MagicMock()
    cfg.max_retries = 3
    failures = ["Material 'silk' has no transport legs"]
    result = TransportGenerator(cfg, client, builder).regenerate_with_feedback(
        sample_layer2_record, failures)
    assert result is not None and isinstance(result, Layer3Record)
    builder.build_correction_prompt.assert_called_once()
    assert builder.build_correction_prompt.call_args[0][1] == failures


# -- 6. Statistical validator (2 tests) ------------------------------------

def test_duplicate_detection(sample_layer3_record, layer3_config):
    v = StatisticalValidator(layer3_config)
    assert v.validate_record(sample_layer3_record).is_duplicate is False
    assert v.validate_record(sample_layer3_record).is_duplicate is True


def test_distance_outlier_detection(sample_layer3_record, layer3_config):
    v = StatisticalValidator(layer3_config)
    cities = ["Tokyo", "Osaka", "Seoul", "Busan", "Bangkok", "Hanoi",
              "Jakarta", "Manila", "Taipei", "Singapore", "Dhaka",
              "Colombo", "Karachi", "Delhi", "Mumbai", "Cairo",
              "Nairobi", "Accra", "Lagos", "Casablanca"]
    for i, city in enumerate(cities):
        rec = copy.deepcopy(sample_layer3_record)
        # Add small variance so stdev is non-zero.
        rec.total_distance_km = 400.0 + float(i)
        for leg in rec.transport_legs:
            leg.from_location = "%s_%d" % (city, i)
            leg.to_location = "%s_dest_%d" % (city, i)
        v.validate_record(rec)
    outlier = copy.deepcopy(sample_layer3_record)
    outlier.total_distance_km = 99999.0
    for leg in outlier.transport_legs:
        leg.from_location = "Outlier_City"
        leg.to_location = "Outlier_Dest"
    r = v.validate_record(outlier)
    assert r.is_outlier is True and r.outlier_type == "distance"
