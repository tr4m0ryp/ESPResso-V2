"""
Tests for Layer 4 V2 -- Packaging Material Estimation pipeline.

Covers:
    1. Model serialization   (4 tests)
    2. Configuration         (2 tests)
    3. Prompt builder        (4 tests)
    4. Validator             (6 tests)
    5. Generator with mocks  (3 tests)
    6. IO layer              (3 tests)

No live API calls are made.  All API interactions are mocked via
unittest.mock.patch.  File I/O uses pytest's tmp_path fixture.
"""

import json
import sys
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Import path / project root setup
# ---------------------------------------------------------------------------
# Ensure the ESPResso-V2 project root is on sys.path so that absolute
# data.data_generation.* imports resolve correctly.
_PROJECT_ROOT = Path(__file__).resolve().parents[5]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ESPResso-V2 has no .git marker, so PipelinePaths._find_project_root raises
# RuntimeError when called.  We work around this by constructing PipelinePaths
# with an explicit root (its frozen-dataclass constructor accepts root=...).
_ESPRESSO_ROOT = Path(__file__).resolve().parents[4]  # .../ESPResso-V2

from data.data_generation.shared.paths import PipelinePaths  # noqa: E402

_FIXED_PATHS = PipelinePaths(root=_ESPRESSO_ROOT)

from data.data_generation.layer_4.models.models import (  # noqa: E402
    Layer4Record,
    PackagingResult,
    ValidationResult,
)
from data.data_generation.layer_4.config.config import Layer4Config  # noqa: E402
from data.data_generation.layer_4.prompts.builder import PromptBuilder  # noqa: E402
from data.data_generation.layer_4.core.validator import PackagingValidator  # noqa: E402
from data.data_generation.layer_4.core.generator import PackagingGenerator  # noqa: E402
from data.data_generation.layer_4.io.writer import OutputWriter, HEADERS  # noqa: E402
from data.data_generation.layer_4.io.input_reader import Layer3Reader  # noqa: E402


def _make_config(**env_overrides) -> Layer4Config:
    """Construct a Layer4Config that does not require a .git marker.

    Passes a pre-built PipelinePaths so that _find_project_root is never
    called.  Any keyword arguments are applied as environment variables
    before construction and cleaned up afterwards.
    """
    import os

    old_values = {}
    for key, val in env_overrides.items():
        old_values[key] = os.environ.get(key)
        os.environ[key] = val

    try:
        cfg = Layer4Config(_paths=_FIXED_PATHS)
    finally:
        for key, old in old_values.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old

    return cfg


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_layer3_record() -> Dict[str, Any]:
    """Valid Layer 3 record with realistic textile product values.

    Represents a T-Shirts/Tops item made of Cotton and Polyester with a
    two-leg transport route (road from Dhaka to Chittagong, sea freight
    to Rotterdam).
    """
    transport_legs = [
        {
            "from_location": "Dhaka",
            "to_location": "Chittagong",
            "distance_km": 250.0,
            "transport_modes": ["road"],
        },
        {
            "from_location": "Chittagong",
            "to_location": "Rotterdam",
            "distance_km": 14500.0,
            "transport_modes": ["sea"],
        },
    ]
    return {
        "category_id": "CAT_001",
        "category_name": "T-Shirts",
        "subcategory_id": "SUB_001",
        "subcategory_name": "Tops",
        "materials": json.dumps(["Cotton", "Polyester"]),
        "material_weights_kg": json.dumps([0.16, 0.04]),
        "material_percentages": json.dumps([80.0, 20.0]),
        "total_weight_kg": 0.20,
        "preprocessing_path_id": "PATH_001",
        "preprocessing_steps": json.dumps(["spinning", "weaving", "dyeing"]),
        "step_material_mapping": json.dumps(
            {
                "spinning": ["Cotton", "Polyester"],
                "weaving": ["Cotton", "Polyester"],
                "dyeing": ["Cotton"],
            }
        ),
        "transport_legs": json.dumps(transport_legs),
        "total_distance_km": 14750.0,
    }


@pytest.fixture
def sample_packaging_result() -> PackagingResult:
    """PackagingResult with sea-freight-appropriate values for a lightweight top."""
    return PackagingResult(
        paper_cardboard_kg=0.012,
        plastic_kg=0.006,
        other_kg=0.001,
        reasoning=(
            "Sea freight journey of 14750 km requires moisture-barrier polybag "
            "and corrugated cardboard outer box; lightweight t-shirt uses minimal "
            "foam inserts."
        ),
    )


@pytest.fixture
def sample_layer4_record(
    sample_layer3_record: Dict[str, Any],
    sample_packaging_result: PackagingResult,
) -> Layer4Record:
    """Layer4Record constructed via Layer4Record.from_layer3()."""
    return Layer4Record.from_layer3(sample_layer3_record, sample_packaging_result)


@pytest.fixture
def config() -> Layer4Config:
    """Layer4Config with defaults built from a fixed PipelinePaths."""
    return _make_config()


# ---------------------------------------------------------------------------
# 1. Model serialization (4 tests)
# ---------------------------------------------------------------------------


class TestPackagingResultOutputLists:
    """test_packaging_result_to_output_lists"""

    def test_packaging_result_to_output_lists(
        self, sample_packaging_result: PackagingResult
    ) -> None:
        categories, masses = sample_packaging_result.to_output_lists()

        assert categories == ["Paper/Cardboard", "Plastic", "Other/Unspecified"]
        assert len(masses) == 3
        assert masses[0] == pytest.approx(0.012)
        assert masses[1] == pytest.approx(0.006)
        assert masses[2] == pytest.approx(0.001)


class TestPackagingResultTotalMass:
    """test_packaging_result_total_mass"""

    def test_packaging_result_total_mass(
        self, sample_packaging_result: PackagingResult
    ) -> None:
        total = sample_packaging_result.total_mass_kg()
        assert total == pytest.approx(0.012 + 0.006 + 0.001)


class TestPackagingResultFromDict:
    """test_packaging_result_from_dict"""

    def test_packaging_result_from_dict(self) -> None:
        data = {
            "paper_cardboard_kg": "0.01234567",
            "plastic_kg": 0.00567891,
            "other_kg": 0.001,
            "reasoning": "Test reasoning string.",
        }
        result = PackagingResult.from_dict(data)

        assert isinstance(result, PackagingResult)
        # from_dict rounds to 4 decimal places
        assert result.paper_cardboard_kg == pytest.approx(round(0.01234567, 4))
        assert result.plastic_kg == pytest.approx(round(0.00567891, 4))
        assert result.other_kg == pytest.approx(0.001)
        assert result.reasoning == "Test reasoning string."

    def test_packaging_result_from_dict_missing_key_raises(self) -> None:
        data = {
            "paper_cardboard_kg": 0.01,
            "plastic_kg": 0.005,
            # other_kg intentionally omitted
            "reasoning": "Missing other_kg",
        }
        with pytest.raises(ValueError, match="other_kg"):
            PackagingResult.from_dict(data)


class TestLayer4RecordRoundtrip:
    """test_layer4_record_roundtrip"""

    def test_layer4_record_roundtrip(
        self, sample_layer4_record: Layer4Record
    ) -> None:
        record_dict = sample_layer4_record.to_dict()

        # Must expose exactly 16 keys matching HEADERS
        assert set(record_dict.keys()) == set(HEADERS)
        assert len(record_dict) == 16

        # JSON-encoded list/dict fields must be serialized as strings
        json_fields = [
            "materials",
            "material_weights_kg",
            "material_percentages",
            "preprocessing_steps",
            "step_material_mapping",
            "transport_legs",
            "packaging_categories",
            "packaging_masses_kg",
        ]
        for field_name in json_fields:
            value = record_dict[field_name]
            assert isinstance(value, str), (
                "Expected field '%s' to be a JSON string, got %r"
                % (field_name, type(value))
            )
            parsed = json.loads(value)
            assert isinstance(parsed, (list, dict))

        # Scalar fields must retain their native Python types
        assert isinstance(record_dict["total_weight_kg"], float)
        assert isinstance(record_dict["total_distance_km"], float)
        assert isinstance(record_dict["packaging_reasoning"], str)


# ---------------------------------------------------------------------------
# 2. Configuration (2 tests)
# ---------------------------------------------------------------------------


class TestConfigDefaults:
    """test_config_defaults"""

    def test_config_defaults(self, config: Layer4Config) -> None:
        assert config.temperature == pytest.approx(0.3)
        assert config.max_tokens == 300
        assert config.min_packaging_ratio == pytest.approx(0.005)
        assert config.max_packaging_ratio == pytest.approx(0.15)
        assert config.min_reasoning_length == 20


class TestConfigEnvOverride:
    """test_config_env_override"""

    def test_config_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LAYER4_MIN_PKG_RATIO", "0.01")
        monkeypatch.setenv("LAYER4_MAX_PKG_RATIO", "0.15")

        # Build config after the env var is set
        cfg = Layer4Config(_paths=_FIXED_PATHS)
        assert cfg.min_packaging_ratio == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# 3. Prompt builder (4 tests)
# ---------------------------------------------------------------------------


class TestSystemPromptLoads:
    """test_system_prompt_loads"""

    def test_system_prompt_loads(self, config: Layer4Config) -> None:
        builder = PromptBuilder(config)
        prompt = builder.get_system_prompt()

        assert isinstance(prompt, str)
        assert len(prompt) > 0
        assert "Paper/Cardboard" in prompt
        assert "Plastic" in prompt
        assert "Other" in prompt


class TestSystemPromptCached:
    """test_system_prompt_cached: second call returns the same object (identity)."""

    def test_system_prompt_cached(self, config: Layer4Config) -> None:
        builder = PromptBuilder(config)
        first = builder.get_system_prompt()
        second = builder.get_system_prompt()

        # Must be the exact same object in memory -- cache hit
        assert first is second


class TestUserPromptIncludesProduct:
    """test_user_prompt_includes_product"""

    def test_user_prompt_includes_product(
        self,
        config: Layer4Config,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        builder = PromptBuilder(config)
        prompt = builder.build_user_prompt(sample_layer3_record)

        # subcategory_name and/or category_name must appear
        assert "T-Shirts" in prompt or "Tops" in prompt
        # Product weight 0.20 kg formatted by %.4g gives "0.2"
        assert "0.2" in prompt
        assert "Cotton" in prompt


class TestUserPromptIncludesTransport:
    """test_user_prompt_includes_transport"""

    def test_user_prompt_includes_transport(
        self,
        config: Layer4Config,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        builder = PromptBuilder(config)
        prompt = builder.build_user_prompt(sample_layer3_record)

        # Both transport modes present in the two legs
        assert "sea" in prompt.lower()
        assert "road" in prompt.lower()

        # Total journey distance -- PromptBuilder formats with %.4g, which renders
        # 14750.0 as "1.475e+04" in Python's %g formatting.  Assert the significant
        # digits appear in the prompt regardless of notation.
        assert "1.475" in prompt or "14750" in prompt

        # Origin city (first leg from_location) and destination (last to_location)
        assert "Dhaka" in prompt
        assert "Rotterdam" in prompt


# ---------------------------------------------------------------------------
# 4. Validator (6 tests)
# ---------------------------------------------------------------------------


def _record_with_masses(
    base: Layer4Record,
    masses: list,
    categories: list = None,
) -> Layer4Record:
    """Return a shallow copy of *base* with replaced packaging fields."""
    if categories is None:
        categories = ["Paper/Cardboard", "Plastic", "Other/Unspecified"]
    return replace(base, packaging_masses_kg=masses, packaging_categories=categories)


class TestValidRecordPasses:
    """test_valid_record_passes"""

    def test_valid_record_passes(
        self,
        config: Layer4Config,
        sample_layer4_record: Layer4Record,
    ) -> None:
        validator = PackagingValidator(config)
        result = validator.validate(sample_layer4_record)

        assert result.is_valid is True
        assert result.errors == []


class TestNegativeMassFails:
    """test_negative_mass_fails"""

    def test_negative_mass_fails(
        self,
        config: Layer4Config,
        sample_layer4_record: Layer4Record,
    ) -> None:
        bad = _record_with_masses(sample_layer4_record, masses=[-0.01, 0.006, 0.001])
        validator = PackagingValidator(config)
        result = validator.validate(bad)

        assert result.is_valid is False
        assert any("negative" in err.lower() for err in result.errors)


class TestZeroTotalMassFails:
    """test_zero_total_mass_fails"""

    def test_zero_total_mass_fails(
        self,
        config: Layer4Config,
        sample_layer4_record: Layer4Record,
    ) -> None:
        bad = _record_with_masses(sample_layer4_record, masses=[0.0, 0.0, 0.0])
        validator = PackagingValidator(config)
        result = validator.validate(bad)

        assert result.is_valid is False
        assert any(
            "zero" in err.lower() or "positive" in err.lower()
            for err in result.errors
        )


class TestWrongCategoryCountFails:
    """test_wrong_category_count_fails"""

    def test_wrong_category_count_fails(
        self,
        config: Layer4Config,
        sample_layer4_record: Layer4Record,
    ) -> None:
        # Two categories and two masses instead of the required three
        bad = _record_with_masses(
            sample_layer4_record,
            masses=[0.012, 0.006],
            categories=["Paper/Cardboard", "Plastic"],
        )
        validator = PackagingValidator(config)
        result = validator.validate(bad)

        assert result.is_valid is False
        assert any("3" in err or "exactly" in err.lower() for err in result.errors)


class TestHighPackagingRatioWarns:
    """test_high_packaging_ratio_warns: masses sum to 50% of product weight."""

    def test_high_packaging_ratio_warns(
        self,
        config: Layer4Config,
        sample_layer4_record: Layer4Record,
    ) -> None:
        # product_weight = 0.20 kg; 50% = 0.10 kg total packaging (ratio 0.5)
        # max_packaging_ratio = 0.15, so a warning must fire
        heavy = _record_with_masses(
            sample_layer4_record,
            masses=[0.05, 0.04, 0.01],
        )
        validator = PackagingValidator(config)
        result = validator.validate(heavy)

        # Structurally valid -- three categories, all positive masses
        assert result.is_valid is True
        assert len(result.warnings) > 0
        assert any(
            "ratio" in w.lower() or "maximum" in w.lower() for w in result.warnings
        )


class TestBatchSummaryStructure:
    """test_batch_summary_structure: validate 5 records, verify all expected keys."""

    def test_batch_summary_structure(
        self,
        config: Layer4Config,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        validator = PackagingValidator(config)

        for i in range(5):
            packaging = PackagingResult(
                paper_cardboard_kg=0.010 + i * 0.001,
                plastic_kg=0.005,
                other_kg=0.001,
                reasoning=(
                    "Sea freight batch record %d with a sufficiently long reasoning." % i
                ),
            )
            record = Layer4Record.from_layer3(sample_layer3_record, packaging)
            validator.validate(record)

        summary = validator.validate_batch_summary()

        expected_keys = {
            "total_records",
            "records_with_warnings",
            "duplicate_count",
            "duplicate_percentage",
            "category_usage",
            "mean_packaging_ratio",
            "distance_mass_correlation",
            "zero_mass_count",
        }
        assert expected_keys.issubset(set(summary.keys()))
        assert summary["total_records"] == 5
        assert isinstance(summary["category_usage"], dict)


# ---------------------------------------------------------------------------
# 5. Generator with mock API (3 tests)
# ---------------------------------------------------------------------------


def _valid_api_response() -> Dict[str, Any]:
    """Minimal dict that PackagingResult.from_dict() will accept."""
    return {
        "paper_cardboard_kg": 0.012,
        "plastic_kg": 0.006,
        "other_kg": 0.001,
        "reasoning": "Sea freight with a long journey requires moisture barriers.",
    }


class TestGenerateForRecordSuccess:
    """test_generate_for_record_success"""

    def test_generate_for_record_success(
        self,
        config: Layer4Config,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        mock_client = MagicMock()
        mock_client.generate_packaging.return_value = _valid_api_response()

        mock_builder = MagicMock(spec=PromptBuilder)
        mock_builder.get_system_prompt.return_value = "system prompt text"
        mock_builder.build_user_prompt.return_value = "user prompt text"

        generator = PackagingGenerator(config, mock_client, mock_builder)
        result = generator.generate_for_record(sample_layer3_record)

        assert result is not None
        assert isinstance(result, Layer4Record)
        assert result.packaging_categories == [
            "Paper/Cardboard",
            "Plastic",
            "Other/Unspecified",
        ]
        assert result.packaging_masses_kg == pytest.approx([0.012, 0.006, 0.001])
        mock_client.generate_packaging.assert_called_once()


class TestGenerateForRecordApiFailure:
    """test_generate_for_record_api_failure: API raises, verify returns None."""

    def test_generate_for_record_api_failure(
        self,
        config: Layer4Config,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        mock_client = MagicMock()
        mock_client.generate_packaging.side_effect = RuntimeError("API unreachable")

        mock_builder = MagicMock(spec=PromptBuilder)
        mock_builder.get_system_prompt.return_value = "system prompt text"
        mock_builder.build_user_prompt.return_value = "user prompt text"

        # Single retry, zero delay so the test completes instantly
        object.__setattr__(config, "max_retries", 1)
        object.__setattr__(config, "retry_delay", 0.0)

        generator = PackagingGenerator(config, mock_client, mock_builder)

        with patch("data.data_generation.layer_4.core.generator.time.sleep"):
            result = generator.generate_for_record(sample_layer3_record)

        assert result is None


class TestRegenerateWithFeedback:
    """test_regenerate_with_feedback"""

    def test_regenerate_with_feedback(
        self,
        config: Layer4Config,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        mock_client = MagicMock()
        mock_client.generate_packaging.return_value = _valid_api_response()

        mock_builder = MagicMock(spec=PromptBuilder)
        mock_builder.get_system_prompt.return_value = "system prompt text"
        mock_builder.build_correction_prompt.return_value = "correction prompt text"

        generator = PackagingGenerator(config, mock_client, mock_builder)
        failures = ["packaging_masses_kg must sum to a positive value"]
        result = generator.regenerate_with_feedback(sample_layer3_record, failures)

        assert result is not None
        assert isinstance(result, Layer4Record)
        mock_builder.build_correction_prompt.assert_called_once_with(
            sample_layer3_record, failures
        )
        mock_client.generate_packaging.assert_called_once()


# ---------------------------------------------------------------------------
# 6. IO layer (3 tests)
# ---------------------------------------------------------------------------


def _output_writer_for_tmp(tmp_path: Path) -> OutputWriter:
    """Construct an OutputWriter that writes entirely within *tmp_path*."""

    base_cfg = _make_config()

    class _LocalConfig(Layer4Config):
        @property
        def output_path(self) -> Path:
            return tmp_path / "layer_4" / "layer_4_complete_dataset.parquet"

        @property
        def output_dir(self) -> Path:
            return tmp_path / "layer_4"

        @property
        def checkpoint_dir(self) -> Path:
            return tmp_path / "layer_4" / "checkpoints"

    local_cfg = object.__new__(_LocalConfig)
    local_cfg.__dict__.update(base_cfg.__dict__)
    return OutputWriter(local_cfg)


def _build_records(layer3_record: Dict[str, Any], count: int) -> list:
    """Return *count* distinct Layer4Record instances with varying masses."""
    records = []
    for i in range(count):
        result = PackagingResult(
            paper_cardboard_kg=0.010 + i * 0.001,
            plastic_kg=0.005,
            other_kg=0.001,
            reasoning="Sea freight journey test record %d with sufficient text." % i,
        )
        records.append(Layer4Record.from_layer3(layer3_record, result))
    return records


class TestWriterOutputSchema:
    """test_writer_output_schema: write 3 records, read back, verify 16 columns."""

    def test_writer_output_schema(
        self,
        tmp_path: Path,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        writer = _output_writer_for_tmp(tmp_path)
        records = _build_records(sample_layer3_record, 3)

        writer.write_records(records)

        assert writer.output_path.exists()
        df = pd.read_parquet(writer.output_path)
        assert len(df) == 3
        assert list(df.columns) == HEADERS
        assert len(df.columns) == 16


class TestWriterCheckpointAndMerge:
    """test_writer_checkpoint_and_merge: 2 checkpoints merged -> all rows present."""

    def test_writer_checkpoint_and_merge(
        self,
        tmp_path: Path,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        writer = _output_writer_for_tmp(tmp_path)

        batch_a = _build_records(sample_layer3_record, 2)
        batch_b = _build_records(sample_layer3_record, 3)

        writer.write_checkpoint(batch_a, checkpoint_index=2)
        writer.write_checkpoint(batch_b, checkpoint_index=5)

        before = list(writer.checkpoint_dir.glob("checkpoint_*.parquet"))
        assert len(before) == 2

        writer.merge_checkpoints()

        assert writer.output_path.exists()
        df = pd.read_parquet(writer.output_path)
        assert len(df) == 5  # 2 + 3

        # Checkpoint parquet files must be removed after a successful merge
        after = list(writer.checkpoint_dir.glob("checkpoint_*.parquet"))
        assert len(after) == 0


class TestReaderRecordCount:
    """test_reader_record_count: write parquet with known rows, verify get_record_count()."""

    def test_reader_record_count(
        self,
        tmp_path: Path,
        sample_layer3_record: Dict[str, Any],
    ) -> None:
        # Write 7 records into a temp parquet file via OutputWriter
        writer = _output_writer_for_tmp(tmp_path)
        records = _build_records(sample_layer3_record, 7)
        writer.write_records(records)
        parquet_path = writer.output_path
        assert parquet_path.exists()

        # Wire a Layer3Reader to that file via a config subclass
        base_cfg = _make_config()

        class _LocalReaderConfig(Layer4Config):
            @property
            def layer3_output_path(self) -> Path:
                return parquet_path

        reader_cfg = object.__new__(_LocalReaderConfig)
        reader_cfg.__dict__.update(base_cfg.__dict__)

        reader = Layer3Reader(reader_cfg)
        assert reader.get_record_count() == 7
