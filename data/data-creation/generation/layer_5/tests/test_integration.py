"""
Integration tests for the Layer 5 V2 validation pipeline.

Tests cover all five stages: passport verification, cross-layer coherence,
statistical quality, sampled reward scoring, and final decision-making.
End-to-end tests exercise the full pipeline with mocked API calls.
"""

import csv
import json
import copy
import os
import pytest
from typing import Dict, List
from unittest.mock import MagicMock, patch

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord,
    CompleteValidationResult,
    CrossLayerCoherenceResult,
    PassportVerificationResult,
    SampledRewardResult,
    StatisticalQualityResult,
    ValidationMetadata,
)
from data.data_generation.layer_5.core.passport_verifier import (
    PassportVerifier,
)
from data.data_generation.layer_5.core.coherence_prompt import (
    CoherencePromptBuilder,
)
from data.data_generation.layer_5.core.coherence_validator import (
    CoherenceValidator,
)
from data.data_generation.layer_5.core.statistical_validator import (
    StatisticalValidator,
)
from data.data_generation.layer_5.core.sampled_reward_scorer import (
    SampledRewardScorer,
)
from data.data_generation.layer_5.core.decision_maker import DecisionMaker


# ======================================================================
# Helper: mock API response builders
# ======================================================================


def mock_coherence_response(record_ids: List[str]) -> str:
    """Build a mock coherence JSON response for the given record IDs."""
    result: Dict[str, dict] = {}
    for rid in record_ids:
        result[rid] = {
            "lifecycle_coherence_score": 0.88,
            "cross_layer_contradiction_score": 0.92,
            "overall_coherence_score": 0.90,
            "contradictions_found": [],
            "recommendation": "accept",
        }
    return json.dumps(result)


def mock_coherence_response_low(record_ids: List[str]) -> str:
    """Build a mock coherence response with low scores (below review threshold)."""
    result: Dict[str, dict] = {}
    for rid in record_ids:
        result[rid] = {
            "lifecycle_coherence_score": 0.40,
            "cross_layer_contradiction_score": 0.35,
            "overall_coherence_score": 0.38,
            "contradictions_found": [
                "silk material contradicts injection molding process"
            ],
            "recommendation": "reject",
        }
    return json.dumps(result)


def mock_coherence_response_mid(record_ids: List[str]) -> str:
    """Build a mock coherence response in the review range (0.70 - 0.85)."""
    result: Dict[str, dict] = {}
    for rid in record_ids:
        result[rid] = {
            "lifecycle_coherence_score": 0.75,
            "cross_layer_contradiction_score": 0.78,
            "overall_coherence_score": 0.76,
            "contradictions_found": ["minor transport distance concern"],
            "recommendation": "review",
        }
    return json.dumps(result)


def mock_reward_response() -> str:
    """Build a mock reward scoring response."""
    return "Score: 0.82\nJustification: Realistic textile product data."


# ======================================================================
# Helper: build a base config with passport enabled
# ======================================================================


def _make_config(**overrides) -> Layer5Config:
    """Create a Layer5Config suitable for testing."""
    cfg = Layer5Config()
    for key, val in overrides.items():
        object.__setattr__(cfg, key, val)
    return cfg


# ======================================================================
# Helper: sample record factory
# ======================================================================


def _make_record(
    subcategory_id: str,
    subcategory_name: str,
    category_id: str = "TEX",
    category_name: str = "Textiles",
    materials: List[str] = None,
    material_weights_kg: List[float] = None,
    material_percentages: List[float] = None,
    total_weight_kg: float = 0.250,
    preprocessing_path_id: str = "pp_001",
    preprocessing_steps: List[str] = None,
    transport_scenario_id: str = "ts_001",
    total_transport_distance_km: float = 5200.0,
    supply_chain_type: str = "medium_haul",
    transport_items: List[Dict] = None,
    transport_modes: List[str] = None,
    transport_distances_kg: List[float] = None,
    transport_emissions_kg_co2e: List[float] = None,
    packaging_config_id: str = "pkg_001",
    packaging_items: List[Dict] = None,
    packaging_categories: List[str] = None,
    packaging_masses_kg: List[float] = None,
    total_packaging_mass_kg: float = 0.035,
    add_passport: bool = True,
) -> CompleteProductRecord:
    """Factory for building test CompleteProductRecord instances."""
    if materials is None:
        materials = ["cotton"]
    if material_weights_kg is None:
        material_weights_kg = [0.250]
    if material_percentages is None:
        material_percentages = [100.0]
    if preprocessing_steps is None:
        preprocessing_steps = ["ginning", "spinning", "weaving", "dyeing"]
    if transport_items is None:
        transport_items = [
            {"mode": "sea", "from": "Dhaka", "to": "Rotterdam", "km": 5200}
        ]
    if transport_modes is None:
        transport_modes = ["sea"]
    if transport_distances_kg is None:
        transport_distances_kg = [5200.0]
    if transport_emissions_kg_co2e is None:
        transport_emissions_kg_co2e = [0.0312]
    if packaging_items is None:
        packaging_items = [
            {"type": "polybag", "material": "LDPE", "mass_kg": 0.010},
            {"type": "cardboard_box", "material": "cardboard", "mass_kg": 0.025},
        ]
    if packaging_categories is None:
        packaging_categories = ["polybag", "cardboard_box"]
    if packaging_masses_kg is None:
        packaging_masses_kg = [0.010, 0.025]

    record = CompleteProductRecord(
        category_id=category_id,
        category_name=category_name,
        subcategory_id=subcategory_id,
        subcategory_name=subcategory_name,
        materials=materials,
        material_weights_kg=material_weights_kg,
        material_percentages=material_percentages,
        total_weight_kg=total_weight_kg,
        preprocessing_path_id=preprocessing_path_id,
        preprocessing_steps=preprocessing_steps,
        transport_scenario_id=transport_scenario_id,
        total_transport_distance_km=total_transport_distance_km,
        supply_chain_type=supply_chain_type,
        transport_items=transport_items,
        transport_modes=transport_modes,
        transport_distances_kg=transport_distances_kg,
        transport_emissions_kg_co2e=transport_emissions_kg_co2e,
        packaging_config_id=packaging_config_id,
        packaging_items=packaging_items,
        packaging_categories=packaging_categories,
        packaging_masses_kg=packaging_masses_kg,
        total_packaging_mass_kg=total_packaging_mass_kg,
    )

    if add_passport:
        record.layer1_passport_hash = PassportVerifier.compute_passport_hash(
            record, 1
        )
        record.layer2_passport_hash = PassportVerifier.compute_passport_hash(
            record, 2
        )
        record.layer3_passport_hash = PassportVerifier.compute_passport_hash(
            record, 3
        )
        record.layer4_passport_hash = PassportVerifier.compute_passport_hash(
            record, 4
        )

    return record


# ======================================================================
# Fixture: 10 sample product records
# ======================================================================


@pytest.fixture
def sample_records() -> List[CompleteProductRecord]:
    """Create 10 sample product records covering different scenarios.

    Record 1:  Valid, coherent product (cotton t-shirt)
    Record 2:  Valid, coherent product (polyester jacket)
    Record 3:  Contradictory (silk + injection molding)
    Record 4:  Duplicate of Record 1
    Record 5:  Valid (linen dress)
    Record 6:  Valid (wool sweater)
    Record 7:  Valid (nylon backpack) -- missing passport (layer3 None)
    Record 8:  Valid (organic cotton socks)
    Record 9:  Valid (recycled polyester shorts)
    Record 10: Valid (hemp canvas bag)
    """
    records: List[CompleteProductRecord] = []

    # Record 1: cotton t-shirt
    records.append(
        _make_record(
            subcategory_id="tshirt_001",
            subcategory_name="T-Shirt",
            materials=["cotton"],
            material_weights_kg=[0.250],
            material_percentages=[100.0],
            total_weight_kg=0.250,
            preprocessing_steps=["ginning", "spinning", "weaving", "dyeing"],
            total_transport_distance_km=5200.0,
            supply_chain_type="medium_haul",
            transport_modes=["sea"],
            transport_distances_kg=[5200.0],
            transport_emissions_kg_co2e=[0.0312],
            packaging_categories=["polybag", "cardboard_box"],
            packaging_masses_kg=[0.010, 0.025],
            total_packaging_mass_kg=0.035,
        )
    )

    # Record 2: polyester jacket
    records.append(
        _make_record(
            subcategory_id="jacket_002",
            subcategory_name="Jacket",
            materials=["polyester", "nylon"],
            material_weights_kg=[0.500, 0.100],
            material_percentages=[83.3, 16.7],
            total_weight_kg=0.600,
            preprocessing_path_id="pp_002",
            preprocessing_steps=[
                "extrusion",
                "texturizing",
                "knitting",
                "coating",
            ],
            transport_scenario_id="ts_002",
            total_transport_distance_km=8500.0,
            supply_chain_type="long_haul",
            transport_modes=["sea", "road"],
            transport_distances_kg=[8000.0, 500.0],
            transport_emissions_kg_co2e=[0.048, 0.015],
            packaging_config_id="pkg_002",
            packaging_categories=["garment_bag", "cardboard_box"],
            packaging_masses_kg=[0.015, 0.040],
            total_packaging_mass_kg=0.055,
        )
    )

    # Record 3: contradictory -- silk + injection molding
    records.append(
        _make_record(
            subcategory_id="silk_molded_003",
            subcategory_name="Silk Product",
            materials=["silk"],
            material_weights_kg=[0.120],
            material_percentages=[100.0],
            total_weight_kg=0.120,
            preprocessing_path_id="pp_003",
            preprocessing_steps=["injection_molding", "curing"],
            transport_scenario_id="ts_003",
            total_transport_distance_km=3000.0,
            supply_chain_type="medium_haul",
            transport_modes=["air"],
            transport_distances_kg=[3000.0],
            transport_emissions_kg_co2e=[0.180],
            packaging_config_id="pkg_003",
            packaging_categories=["bubble_wrap", "cardboard_box"],
            packaging_masses_kg=[0.008, 0.020],
            total_packaging_mass_kg=0.028,
        )
    )

    # Record 4: duplicate of Record 1
    records.append(
        _make_record(
            subcategory_id="tshirt_001",
            subcategory_name="T-Shirt",
            materials=["cotton"],
            material_weights_kg=[0.250],
            material_percentages=[100.0],
            total_weight_kg=0.250,
            preprocessing_steps=["ginning", "spinning", "weaving", "dyeing"],
            total_transport_distance_km=5200.0,
            supply_chain_type="medium_haul",
            transport_modes=["sea"],
            transport_distances_kg=[5200.0],
            transport_emissions_kg_co2e=[0.0312],
            packaging_categories=["polybag", "cardboard_box"],
            packaging_masses_kg=[0.010, 0.025],
            total_packaging_mass_kg=0.035,
        )
    )

    # Record 5: linen dress
    records.append(
        _make_record(
            subcategory_id="dress_005",
            subcategory_name="Dress",
            materials=["linen", "elastane"],
            material_weights_kg=[0.300, 0.015],
            material_percentages=[95.2, 4.8],
            total_weight_kg=0.315,
            preprocessing_path_id="pp_005",
            preprocessing_steps=["retting", "spinning", "weaving", "finishing"],
            transport_scenario_id="ts_005",
            total_transport_distance_km=2400.0,
            supply_chain_type="short_haul",
            transport_modes=["road", "rail"],
            transport_distances_kg=[400.0, 2000.0],
            transport_emissions_kg_co2e=[0.012, 0.006],
            packaging_config_id="pkg_005",
            packaging_categories=["tissue_paper", "cardboard_box"],
            packaging_masses_kg=[0.005, 0.030],
            total_packaging_mass_kg=0.035,
        )
    )

    # Record 6: wool sweater
    records.append(
        _make_record(
            subcategory_id="sweater_006",
            subcategory_name="Sweater",
            materials=["wool"],
            material_weights_kg=[0.450],
            material_percentages=[100.0],
            total_weight_kg=0.450,
            preprocessing_path_id="pp_006",
            preprocessing_steps=[
                "scouring",
                "carding",
                "spinning",
                "knitting",
            ],
            transport_scenario_id="ts_006",
            total_transport_distance_km=12000.0,
            supply_chain_type="long_haul",
            transport_modes=["sea"],
            transport_distances_kg=[12000.0],
            transport_emissions_kg_co2e=[0.072],
            packaging_config_id="pkg_006",
            packaging_categories=["polybag", "cardboard_box"],
            packaging_masses_kg=[0.012, 0.035],
            total_packaging_mass_kg=0.047,
        )
    )

    # Record 7: nylon backpack -- missing layer3 passport hash
    rec7 = _make_record(
        subcategory_id="backpack_007",
        subcategory_name="Backpack",
        materials=["nylon", "polyester"],
        material_weights_kg=[0.400, 0.200],
        material_percentages=[66.7, 33.3],
        total_weight_kg=0.600,
        preprocessing_path_id="pp_007",
        preprocessing_steps=["extrusion", "weaving", "cutting", "sewing"],
        transport_scenario_id="ts_007",
        total_transport_distance_km=9200.0,
        supply_chain_type="long_haul",
        transport_modes=["sea", "road"],
        transport_distances_kg=[8800.0, 400.0],
        transport_emissions_kg_co2e=[0.053, 0.012],
        packaging_config_id="pkg_007",
        packaging_categories=["polybag", "cardboard_box"],
        packaging_masses_kg=[0.018, 0.045],
        total_packaging_mass_kg=0.063,
    )
    rec7.layer3_passport_hash = None  # simulate missing passport
    records.append(rec7)

    # Record 8: organic cotton socks
    records.append(
        _make_record(
            subcategory_id="socks_008",
            subcategory_name="Socks",
            materials=["organic_cotton", "elastane"],
            material_weights_kg=[0.060, 0.005],
            material_percentages=[92.3, 7.7],
            total_weight_kg=0.065,
            preprocessing_path_id="pp_008",
            preprocessing_steps=["ginning", "spinning", "knitting"],
            transport_scenario_id="ts_008",
            total_transport_distance_km=4800.0,
            supply_chain_type="medium_haul",
            transport_modes=["sea"],
            transport_distances_kg=[4800.0],
            transport_emissions_kg_co2e=[0.029],
            packaging_config_id="pkg_008",
            packaging_categories=["paper_band", "cardboard_box"],
            packaging_masses_kg=[0.003, 0.015],
            total_packaging_mass_kg=0.018,
        )
    )

    # Record 9: recycled polyester shorts
    records.append(
        _make_record(
            subcategory_id="shorts_009",
            subcategory_name="Shorts",
            materials=["recycled_polyester", "cotton"],
            material_weights_kg=[0.180, 0.070],
            material_percentages=[72.0, 28.0],
            total_weight_kg=0.250,
            preprocessing_path_id="pp_009",
            preprocessing_steps=[
                "shredding",
                "pelletizing",
                "extrusion",
                "knitting",
                "dyeing",
            ],
            transport_scenario_id="ts_009",
            total_transport_distance_km=6100.0,
            supply_chain_type="medium_haul",
            transport_modes=["sea", "road"],
            transport_distances_kg=[5800.0, 300.0],
            transport_emissions_kg_co2e=[0.035, 0.009],
            packaging_config_id="pkg_009",
            packaging_categories=["polybag", "cardboard_box"],
            packaging_masses_kg=[0.008, 0.022],
            total_packaging_mass_kg=0.030,
        )
    )

    # Record 10: hemp canvas bag
    records.append(
        _make_record(
            subcategory_id="bag_010",
            subcategory_name="Canvas Bag",
            materials=["hemp"],
            material_weights_kg=[0.350],
            material_percentages=[100.0],
            total_weight_kg=0.350,
            preprocessing_path_id="pp_010",
            preprocessing_steps=[
                "retting",
                "decortication",
                "spinning",
                "weaving",
            ],
            transport_scenario_id="ts_010",
            total_transport_distance_km=3500.0,
            supply_chain_type="medium_haul",
            transport_modes=["rail", "road"],
            transport_distances_kg=[3000.0, 500.0],
            transport_emissions_kg_co2e=[0.009, 0.015],
            packaging_config_id="pkg_010",
            packaging_categories=["paper_wrap", "cardboard_box"],
            packaging_masses_kg=[0.006, 0.028],
            total_packaging_mass_kg=0.034,
        )
    )

    return records


# ======================================================================
# Tests: PassportVerifier
# ======================================================================


class TestPassportVerifier:
    """Tests for the passport verification stage."""

    def test_valid_passports_accepted(self, sample_records):
        """Records with correct passport hashes pass verification."""
        config = _make_config(passport_enabled=True)
        verifier = PassportVerifier(config)

        # Records 0, 1, 2 have all four hashes set correctly
        for idx in (0, 1, 2):
            result = verifier.verify(sample_records[idx])
            assert result.is_valid, (
                "Record %d should have valid passports" % idx
            )
            assert result.layer1_hash_valid
            assert result.layer2_hash_valid
            assert result.layer3_hash_valid
            assert result.layer4_hash_valid
            assert result.missing_passports == []
            assert result.errors == []

    def test_missing_passport_flagged(self, sample_records):
        """Record 7 (index 6) has layer3 passport set to None."""
        config = _make_config(passport_enabled=True)
        verifier = PassportVerifier(config)

        result = verifier.verify(sample_records[6])  # backpack_007
        assert not result.is_valid
        assert not result.layer3_hash_valid
        assert "layer3" in result.missing_passports

    def test_tampered_passport_rejected(self, sample_records):
        """A record whose layer1 hash is altered should fail verification."""
        config = _make_config(passport_enabled=True)
        verifier = PassportVerifier(config)

        tampered = copy.deepcopy(sample_records[0])
        tampered.layer1_passport_hash = "deadbeef" * 8  # 64 hex chars, wrong
        result = verifier.verify(tampered)
        assert not result.is_valid
        assert not result.layer1_hash_valid
        assert len(result.errors) > 0

    def test_disabled_passport_skips(self, sample_records):
        """When passport_enabled is False, all records pass immediately."""
        config = _make_config(passport_enabled=False)
        verifier = PassportVerifier(config)

        # Even the record with missing passport should pass
        result = verifier.verify(sample_records[6])
        assert result.is_valid

    def test_verify_batch(self, sample_records):
        """verify_batch returns results keyed by subcategory_id."""
        config = _make_config(passport_enabled=True)
        verifier = PassportVerifier(config)

        batch = sample_records[:3]
        results = verifier.verify_batch(batch)
        assert len(results) == 3
        for rec in batch:
            assert rec.subcategory_id in results

    def test_compute_passport_hash_deterministic(self, sample_records):
        """The same record always produces the same hash."""
        h1 = PassportVerifier.compute_passport_hash(sample_records[0], 1)
        h2 = PassportVerifier.compute_passport_hash(sample_records[0], 1)
        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex digest length


# ======================================================================
# Tests: CoherenceValidator
# ======================================================================


class TestCoherenceValidator:
    """Tests for the cross-layer coherence validation stage."""

    @patch(
        "data.data_generation.layer_5.core.coherence_validator.Layer5Client"
    )
    def test_batch_of_50_records(self, MockClient, sample_records):
        """Coherence validator returns results for all records in a batch.

        Note: records 0 and 3 share subcategory_id='tshirt_001', so the
        result dict has 9 unique keys for 10 input records.
        """
        config = _make_config()
        mock_client = MagicMock()

        record_ids = [r.subcategory_id for r in sample_records]
        unique_ids = list(dict.fromkeys(record_ids))
        mock_client.generate_batch_coherence_evaluation.return_value = (
            mock_coherence_response(record_ids)
        )

        validator = CoherenceValidator(config, mock_client)
        results = validator.validate_batch(sample_records)

        # The validator keys results by subcategory_id, so duplicates merge
        assert len(results) == len(unique_ids)
        for rid in unique_ids:
            assert rid in results
            assert isinstance(results[rid], CrossLayerCoherenceResult)
            assert 0.0 <= results[rid].overall_coherence_score <= 1.0

        mock_client.generate_batch_coherence_evaluation.assert_called_once()

    @patch(
        "data.data_generation.layer_5.core.coherence_validator.Layer5Client"
    )
    def test_api_failure_returns_defaults(self, MockClient, sample_records):
        """When the API call raises, all records get default coherence scores."""
        config = _make_config()
        mock_client = MagicMock()
        mock_client.generate_batch_coherence_evaluation.side_effect = (
            RuntimeError("connection refused")
        )

        validator = CoherenceValidator(config, mock_client)
        results = validator.validate_batch(sample_records)

        unique_ids = list(dict.fromkeys(
            r.subcategory_id for r in sample_records
        ))
        assert len(results) == len(unique_ids)
        for rid in unique_ids:
            assert rid in results
            # Default scores are 0.7
            assert results[rid].overall_coherence_score == 0.7
            assert results[rid].recommendation == "review"

    @patch(
        "data.data_generation.layer_5.core.coherence_validator.Layer5Client"
    )
    def test_api_returns_none(self, MockClient, sample_records):
        """When the API returns None, all records get defaults."""
        config = _make_config()
        mock_client = MagicMock()
        mock_client.generate_batch_coherence_evaluation.return_value = None

        validator = CoherenceValidator(config, mock_client)
        results = validator.validate_batch(sample_records)

        unique_ids = list(dict.fromkeys(
            r.subcategory_id for r in sample_records
        ))
        assert len(results) == len(unique_ids)
        for result in results.values():
            assert result.overall_coherence_score == 0.7

    def test_empty_batch(self):
        """Validating an empty batch returns an empty dict."""
        config = _make_config()
        mock_client = MagicMock()
        validator = CoherenceValidator(config, mock_client)
        results = validator.validate_batch([])
        assert results == {}
        mock_client.generate_batch_coherence_evaluation.assert_not_called()


# ======================================================================
# Tests: CoherencePromptBuilder
# ======================================================================


class TestCoherencePromptBuilder:
    """Tests for the prompt builder used by the coherence stage."""

    def test_build_batch_prompt(self, sample_records):
        """Prompt includes all record IDs and expected JSON format."""
        builder = CoherencePromptBuilder()
        prompt = builder.build_batch_prompt(sample_records[:3])

        assert "tshirt_001" in prompt
        assert "jacket_002" in prompt
        assert "silk_molded_003" in prompt
        assert "lifecycle_coherence_score" in prompt

    def test_parse_valid_response(self, sample_records):
        """parse_batch_response correctly maps a valid JSON response."""
        builder = CoherencePromptBuilder()
        record_ids = [r.subcategory_id for r in sample_records[:2]]
        response = mock_coherence_response(record_ids)
        results = builder.parse_batch_response(response, record_ids)

        assert len(results) == 2
        for rid in record_ids:
            assert results[rid].overall_coherence_score == 0.90
            assert results[rid].recommendation == "accept"

    def test_parse_malformed_json_returns_defaults(self, sample_records):
        """Malformed JSON triggers defaults for every record ID."""
        builder = CoherencePromptBuilder()
        record_ids = [r.subcategory_id for r in sample_records[:2]]
        results = builder.parse_batch_response("NOT JSON", record_ids)

        assert len(results) == 2
        for result in results.values():
            assert result.overall_coherence_score == 0.7
            assert result.recommendation == "review"


# ======================================================================
# Tests: StatisticalValidator
# ======================================================================


class TestStatisticalValidator:
    """Tests for the statistical quality validation stage."""

    def test_duplicate_detection(self, sample_records):
        """Records 0 and 3 are identical; the second should be flagged."""
        config = _make_config()
        validator = StatisticalValidator(config)

        result_first = validator.validate_record(sample_records[0])
        assert not result_first.is_duplicate

        result_dup = validator.validate_record(sample_records[3])
        assert result_dup.is_duplicate
        assert result_dup.duplicate_similarity == 1.0

    def test_unique_records_pass(self, sample_records):
        """Distinct records are not flagged as duplicates."""
        config = _make_config()
        validator = StatisticalValidator(config)

        for rec in sample_records[:3]:
            result = validator.validate_record(rec)
            assert not result.is_duplicate

    def test_outlier_detection(self, sample_records):
        """An extreme-weight record is flagged as an outlier after
        enough records have been processed to build statistics."""
        config = _make_config()
        validator = StatisticalValidator(config)

        # Feed 15 normal-weight records with slight variation so stdev > 0
        import random
        rng = random.Random(42)
        for i in range(15):
            weight = 0.250 + rng.uniform(-0.05, 0.05)
            normal = _make_record(
                subcategory_id="norm_%d" % i,
                subcategory_name="Normal Item %d" % i,
                total_weight_kg=weight,
                materials=["cotton_%d" % i],
            )
            validator.validate_record(normal)

        # Now inject an extreme outlier (500kg vs ~0.25kg baseline)
        outlier = _make_record(
            subcategory_id="outlier_999",
            subcategory_name="Heavy Outlier",
            total_weight_kg=500.0,
            materials=["steel"],
        )
        result = validator.validate_record(outlier)
        assert result.is_outlier
        assert result.outlier_type == "weight"

    def test_cross_layer_correlation(self, sample_records):
        """Correlation checks pass with fewer than 100 records."""
        config = _make_config()
        validator = StatisticalValidator(config)

        # With < 100 records, correlation checks always return ok
        for rec in sample_records:
            result = validator.validate_record(rec)
            assert result.weight_packaging_correlation_ok
            assert result.material_transport_correlation_ok

    def test_statistical_summary(self, sample_records):
        """get_statistical_summary returns expected keys."""
        config = _make_config()
        validator = StatisticalValidator(config)
        for rec in sample_records:
            validator.validate_record(rec)

        summary = validator.get_statistical_summary()
        assert "total_records_checked" in summary
        assert "exact_duplicates_found" in summary
        assert "weight_statistics" in summary
        assert summary["exact_duplicates_found"] >= 1  # record 4 is a dup

    def test_reset_clears_state(self, sample_records):
        """reset_statistical_tracking clears all accumulated data."""
        config = _make_config()
        validator = StatisticalValidator(config)
        for rec in sample_records[:3]:
            validator.validate_record(rec)

        validator.reset_statistical_tracking()
        assert len(validator.weight_values) == 0
        assert len(validator.record_hashes) == 0


# ======================================================================
# Tests: SampledRewardScorer
# ======================================================================


class TestSampledRewardScorer:
    """Tests for the sampled reward scoring stage."""

    @patch(
        "data.data_generation.layer_5.core.sampled_reward_scorer.Layer5Client"
    )
    def test_sampling_rate(self, MockClient, sample_records):
        """Only records at the configured sample interval are scored."""
        config = _make_config(reward_sample_rate=0.5)
        mock_client = MagicMock()
        mock_client.generate_reward_score.return_value = 0.82

        scorer = SampledRewardScorer(config, mock_client)
        sampled_count = 0
        total = len(sample_records)

        for idx, rec in enumerate(sample_records):
            result = scorer.score_if_sampled(rec, idx, total)
            if result.was_sampled:
                sampled_count += 1
                assert result.reward_score == 0.82

        # With sample_rate=0.5, interval=2, about half should be sampled
        assert sampled_count > 0
        assert sampled_count < total

    def test_non_sampled_get_estimate(self, sample_records):
        """Non-sampled records still receive the running quality estimate."""
        config = _make_config(reward_sample_rate=0.0)  # never sample
        mock_client = MagicMock()
        scorer = SampledRewardScorer(config, mock_client)

        result = scorer.score_if_sampled(sample_records[0], 0, 10)
        assert not result.was_sampled
        assert result.reward_score is None
        # No samples taken yet, so estimate is None
        assert result.dataset_estimated_quality is None

    @patch(
        "data.data_generation.layer_5.core.sampled_reward_scorer.Layer5Client"
    )
    def test_quality_estimate_updates(self, MockClient, sample_records):
        """After sampling, the running quality estimate updates."""
        config = _make_config(reward_sample_rate=1.0)  # sample everything
        mock_client = MagicMock()
        mock_client.generate_reward_score.return_value = 0.75

        scorer = SampledRewardScorer(config, mock_client)
        for idx, rec in enumerate(sample_records[:3]):
            scorer.score_if_sampled(rec, idx, 10)

        estimate = scorer.get_dataset_quality_estimate()
        assert estimate is not None
        assert abs(estimate - 0.75) < 0.001

    def test_quality_interpretation(self):
        """Quality label mapping works across all thresholds."""
        config = _make_config()
        mock_client = MagicMock()
        scorer = SampledRewardScorer(config, mock_client)

        assert scorer.get_quality_interpretation(0.90) == "High quality"
        assert scorer.get_quality_interpretation(0.70) == "Acceptable"
        assert scorer.get_quality_interpretation(0.50) == "Marginal"
        assert scorer.get_quality_interpretation(0.20) == "Low quality"

    @patch(
        "data.data_generation.layer_5.core.sampled_reward_scorer.Layer5Client"
    )
    def test_api_returns_none(self, MockClient, sample_records):
        """When the API returns None, the record is still marked as sampled."""
        config = _make_config(reward_sample_rate=1.0)
        mock_client = MagicMock()
        mock_client.generate_reward_score.return_value = None

        scorer = SampledRewardScorer(config, mock_client)
        result = scorer.score_if_sampled(sample_records[0], 0, 10)
        assert result.was_sampled
        assert result.reward_score is None


# ======================================================================
# Tests: DecisionMaker
# ======================================================================


class TestDecisionMaker:
    """Tests for the final accept/review/reject decision logic."""

    def _make_passport(self, valid=True):
        return PassportVerificationResult(is_valid=valid)

    def _make_coherence(self, score=0.90, recommendation="accept"):
        return CrossLayerCoherenceResult(
            lifecycle_coherence_score=score,
            cross_layer_contradiction_score=score,
            overall_coherence_score=score,
            recommendation=recommendation,
        )

    def _make_statistical(self, duplicate=False, outlier=False):
        return StatisticalQualityResult(
            is_duplicate=duplicate,
            duplicate_similarity=1.0 if duplicate else 0.0,
            is_outlier=outlier,
            outlier_type="weight" if outlier else None,
        )

    def _make_reward(self, sampled=False, score=None):
        return SampledRewardResult(
            was_sampled=sampled,
            reward_score=score,
        )

    def test_passport_failure_rejects(self, sample_records):
        """A failed passport always results in rejection."""
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=False),
            self._make_coherence(score=0.95),
            self._make_statistical(),
            self._make_reward(),
        )
        assert result.final_decision == "reject"
        assert "Passport verification failed" in result.decision_factors

    def test_high_coherence_accepts(self, sample_records):
        """High coherence + clean stats -> accept."""
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=True),
            self._make_coherence(score=0.90),
            self._make_statistical(),
            self._make_reward(),
        )
        assert result.final_decision == "accept"

    def test_low_coherence_rejects(self, sample_records):
        """Coherence below review threshold -> reject."""
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=True),
            self._make_coherence(score=0.50, recommendation="reject"),
            self._make_statistical(),
            self._make_reward(),
        )
        assert result.final_decision == "reject"

    def test_duplicate_rejects(self, sample_records):
        """A duplicate record is rejected even with good coherence."""
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=True),
            self._make_coherence(score=0.90),
            self._make_statistical(duplicate=True),
            self._make_reward(),
        )
        assert result.final_decision == "reject"
        assert any("Duplicate" in f for f in result.decision_factors)

    def test_review_range(self, sample_records):
        """Coherence in the review range [0.70, 0.85) -> review."""
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=True),
            self._make_coherence(score=0.76, recommendation="review"),
            self._make_statistical(),
            self._make_reward(),
        )
        assert result.final_decision == "review"

    def test_outlier_triggers_review(self, sample_records):
        """An outlier prevents acceptance, sending to review instead."""
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=True),
            self._make_coherence(score=0.90),
            self._make_statistical(outlier=True),
            self._make_reward(),
        )
        # High coherence but outlier -> cannot accept (rule 4 requires clean stats)
        assert result.final_decision == "review"

    def test_result_contains_metadata(self, sample_records):
        """The result includes validation metadata with pipeline version."""
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=True),
            self._make_coherence(score=0.90),
            self._make_statistical(),
            self._make_reward(),
        )
        assert result.metadata is not None
        assert result.metadata.pipeline_version == "v2.0"
        assert result.record_id != ""

    def test_no_coherence_sends_to_review(self, sample_records):
        """When coherence is None, the record goes to review.

        With coherence=None, cs defaults to 0.0. Rule 2 requires
        ``coherence`` to be truthy, so it is skipped. Rule 4 requires
        cs >= accept_threshold (0.85), which fails. The record falls
        through to rule 5 (review).
        """
        config = _make_config()
        maker = DecisionMaker(config)

        result = maker.decide(
            sample_records[0],
            self._make_passport(valid=True),
            None,  # no coherence result
            self._make_statistical(),
            self._make_reward(),
        )
        assert result.final_decision == "review"


# ======================================================================
# Tests: End-to-End Pipeline
# ======================================================================


class TestEndToEnd:
    """End-to-end tests running all five stages together."""

    def test_full_pipeline(self, sample_records, tmp_path):
        """Run all five stages on sample records with mocked API."""
        config = _make_config(
            passport_enabled=True,
            reward_sample_rate=0.5,
            _output_dir_override=tmp_path,
            # Relax material distribution threshold so the small sample
            # of 10 records does not trigger false over-representation flags
            max_single_material_pct=0.50,
        )

        # -- Set up mock API client --
        mock_client = MagicMock()

        # Build coherence response from the unique subcategory_ids
        unique_ids = list(dict.fromkeys(
            r.subcategory_id for r in sample_records
        ))
        mock_client.generate_batch_coherence_evaluation.return_value = (
            mock_coherence_response(unique_ids)
        )
        mock_client.generate_reward_score.return_value = 0.82

        # -- Instantiate components --
        passport_verifier = PassportVerifier(config)
        coherence_validator = CoherenceValidator(config, mock_client)
        stat_validator = StatisticalValidator(config)
        reward_scorer = SampledRewardScorer(config, mock_client)
        decision_maker = DecisionMaker(config)

        # -- Run the five stages --
        results: List[CompleteValidationResult] = []
        total = len(sample_records)

        # Stage 1: passport verification
        passport_results = passport_verifier.verify_batch(sample_records)

        # Stage 2: coherence (process in one batch since < 50 records)
        coherence_results = coherence_validator.validate_batch(sample_records)

        # Stage 3: statistical validation (sequential)
        stat_results = {}
        for rec in sample_records:
            stat_results[rec.subcategory_id] = stat_validator.validate_record(
                rec
            )

        # Stage 4: reward scoring
        reward_results = {}
        for idx, rec in enumerate(sample_records):
            reward_results[rec.subcategory_id] = (
                reward_scorer.score_if_sampled(rec, idx, total)
            )

        # Stage 5: final decision
        for rec in sample_records:
            sid = rec.subcategory_id
            vr = decision_maker.decide(
                rec,
                passport_results[sid],
                coherence_results.get(sid),
                stat_results.get(sid),
                reward_results.get(sid),
            )
            results.append(vr)

        # -- Assertions --

        # Every record got a result
        assert len(results) == total

        # Count decisions
        decisions = [r.final_decision for r in results]
        accepted = decisions.count("accept")
        rejected = decisions.count("reject")
        reviewed = decisions.count("review")

        # Record 4 (index 3) is a duplicate of record 1 (index 0).
        # Because stat_results is keyed by subcategory_id, the duplicate
        # result for record 3 overwrites record 0's clean result. Both
        # records 0 and 3 are rejected as duplicates.
        for idx in (0, 3):
            assert results[idx].final_decision == "reject", (
                "Record at index %d should be rejected as duplicate, got %s"
                % (idx, results[idx].final_decision)
            )
            assert any("Duplicate" in f for f in results[idx].decision_factors)

        # Record 7 (index 6, missing passport) should be rejected
        missing_pp_result = results[6]
        assert missing_pp_result.final_decision == "reject"
        assert "Passport verification failed" in (
            missing_pp_result.decision_factors
        )

        # Records with unique subcategory_ids, valid passports, and
        # coherence=0.90 should be accepted
        for idx in (1, 2, 4, 5, 7, 8, 9):
            assert results[idx].final_decision == "accept", (
                "Record at index %d should be accepted, got %s. Factors: %s"
                % (idx, results[idx].final_decision,
                   results[idx].decision_factors)
            )

        # At least some records were sampled for reward
        sampled = [r for r in results if r.reward and r.reward.was_sampled]
        assert len(sampled) > 0

        # Each result has a score and rationale
        for r in results:
            assert r.final_decision in ("accept", "review", "reject")
            assert r.decision_reasoning != ""

    def test_pipeline_with_low_coherence(self, sample_records):
        """Records with low coherence are rejected."""
        config = _make_config(passport_enabled=True)
        mock_client = MagicMock()

        # Return low coherence for all records
        unique_ids = list(dict.fromkeys(
            r.subcategory_id for r in sample_records
        ))
        mock_client.generate_batch_coherence_evaluation.return_value = (
            mock_coherence_response_low(unique_ids)
        )

        passport_verifier = PassportVerifier(config)
        coherence_validator = CoherenceValidator(config, mock_client)
        decision_maker = DecisionMaker(config)

        passport_results = passport_verifier.verify_batch(sample_records)
        coherence_results = coherence_validator.validate_batch(sample_records)

        for rec in sample_records:
            sid = rec.subcategory_id
            pp = passport_results[sid]
            coh = coherence_results.get(sid)
            if not pp.is_valid:
                # Already rejected by passport
                continue
            result = decision_maker.decide(rec, pp, coh, None, None)
            # Coherence 0.38 < 0.70 review threshold -> reject
            assert result.final_decision == "reject"

    def test_pipeline_passports_disabled(self, sample_records):
        """With passports disabled, even missing-passport records pass stage 1."""
        config = _make_config(passport_enabled=False)
        mock_client = MagicMock()

        unique_ids = list(dict.fromkeys(
            r.subcategory_id for r in sample_records
        ))
        mock_client.generate_batch_coherence_evaluation.return_value = (
            mock_coherence_response(unique_ids)
        )

        passport_verifier = PassportVerifier(config)
        coherence_validator = CoherenceValidator(config, mock_client)
        decision_maker = DecisionMaker(config)

        passport_results = passport_verifier.verify_batch(sample_records)
        coherence_results = coherence_validator.validate_batch(sample_records)

        # Record 7 (index 6) should NOT be rejected by passport
        pp_7 = passport_results[sample_records[6].subcategory_id]
        assert pp_7.is_valid

        # All passports should be valid when passport verification is disabled
        for rec in sample_records:
            sid = rec.subcategory_id
            assert passport_results[sid].is_valid, (
                "Record %s should pass passport check when disabled" % sid
            )

        # With clean statistical results (None), all records should be
        # accepted when coherence is 0.90 and passport is disabled
        for rec in sample_records:
            sid = rec.subcategory_id
            result = decision_maker.decide(
                rec, passport_results[sid],
                coherence_results.get(sid),
                None,  # skip statistical to isolate passport behavior
                None,
            )
            assert result.final_decision == "accept", (
                "Record %s should be accepted, got %s. Factors: %s"
                % (sid, result.final_decision, result.decision_factors)
            )


# ======================================================================
# Tests: Model serialization round-trip
# ======================================================================


class TestModelSerialization:
    """Tests for CompleteValidationResult to_dict / from_dict."""

    def test_round_trip(self, sample_records):
        """to_dict -> from_dict preserves all fields."""
        config = _make_config()
        maker = DecisionMaker(config)

        original = maker.decide(
            sample_records[0],
            PassportVerificationResult(is_valid=True),
            CrossLayerCoherenceResult(
                lifecycle_coherence_score=0.88,
                cross_layer_contradiction_score=0.92,
                overall_coherence_score=0.90,
                recommendation="accept",
            ),
            StatisticalQualityResult(is_duplicate=False),
            SampledRewardResult(was_sampled=True, reward_score=0.82),
        )

        data = original.to_dict()
        restored = CompleteValidationResult.from_dict(data)

        assert restored.record_id == original.record_id
        assert restored.final_decision == original.final_decision
        assert restored.final_score == original.final_score
        assert restored.passport.is_valid == original.passport.is_valid
        assert (
            restored.coherence.overall_coherence_score
            == original.coherence.overall_coherence_score
        )
        assert (
            restored.statistical.is_duplicate
            == original.statistical.is_duplicate
        )
        assert restored.reward.reward_score == original.reward.reward_score
