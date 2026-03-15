"""
Tests for scientifically accurate material proxy mappings.

Verifies that materials with no valid proxy in the reference database are
flagged as uncorrectable rather than silently mapped to chemically incorrect
substitutes. Also verifies that scientifically correct mappings still work.

Background (2026-03 review):
- Elastane/spandex/lycra (polyurethane-based) were previously mapped to nylon 6-6
  (polyamide), overstating carbon footprint by ~72%. No PU fibre in DB.
- Acrylic (polyacrylonitrile) was mapped to polyester, introducing ~20-40% CF error.
- Cashmere/mohair (goat fibres) were mapped to sheep fleece, drastically
  underestimating their actual carbon footprint.
- Tencel/lyocell/modal -> cellulose fibre is correct (regenerated cellulose).
"""

import importlib.util
import os
import sys

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Load the corrector module without triggering package-level imports
# ---------------------------------------------------------------------------
_CORRECTOR_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    "layer_1",
    "models",
    "material_corrector.py",
)

_spec = importlib.util.spec_from_file_location("material_corrector", _CORRECTOR_PATH)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

correct_material_name = _mod.correct_material_name
correct_material_list = _mod.correct_material_list
MATERIAL_NAME_CORRECTIONS = _mod.MATERIAL_NAME_CORRECTIONS
UNCORRECTABLE_PROXY_MATERIALS = _mod.UNCORRECTABLE_PROXY_MATERIALS

# ---------------------------------------------------------------------------
# Load valid material names from the reference parquet
# ---------------------------------------------------------------------------
_PARQUET_PATH = os.path.join(
    os.path.dirname(__file__),
    os.pardir,
    os.pardir,
    "datasets",
    "pre-model",
    "final",
    "base_materials.parquet",
)

_df = pd.read_parquet(_PARQUET_PATH)
VALID_NAMES = set(_df["material_name"].unique())


# ===================================================================
# 1. Elastane / spandex / lycra must be uncorrectable (not nylon 6-6)
# ===================================================================

class TestElastaneUncorrectable:
    """Elastane is polyurethane-based. No PU fibre exists in DB."""

    @pytest.mark.parametrize("name", [
        "fibre, elastane",
        "fibre, spandex",
        "fibre, lycra",
        "fibre, Lycra",
        "lycra",
        "spandex",
        "elastane",
        "fibre, recycled elastane",
        "yarn, elastane",
        "textile, elastane",
        "textile, spandex",
    ])
    def test_elastane_not_mapped_to_nylon(self, name):
        result, was_corrected = correct_material_name(name, VALID_NAMES)
        assert not was_corrected, (
            f"'{name}' should NOT be corrected (no valid proxy), "
            f"but was mapped to '{result}'"
        )
        assert result not in VALID_NAMES, (
            f"'{name}' should be uncorrectable, but resolved to "
            f"valid name '{result}'"
        )
        # Specifically: must NOT map to nylon 6-6
        assert result != "nylon 6-6", (
            f"'{name}' must not map to 'nylon 6-6' (polyamide != polyurethane)"
        )

    def test_elastane_batch_uncorrectable(self):
        inputs = ["fibre, elastane", "spandex", "lycra"]
        _, _, uncorrectable = correct_material_list(inputs, VALID_NAMES)
        for name in inputs:
            assert name in uncorrectable, (
                f"'{name}' should appear in uncorrectable list"
            )


# ===================================================================
# 2. Acrylic must be uncorrectable (not polyester)
# ===================================================================

class TestAcrylicUncorrectable:
    """Acrylic (polyacrylonitrile) has no DB entry."""

    @pytest.mark.parametrize("name", [
        "acrylic",
        "fibre, acrylic",
        "yarn, acrylic",
        "textile, woven acrylic",
    ])
    def test_acrylic_not_mapped_to_polyester(self, name):
        result, was_corrected = correct_material_name(name, VALID_NAMES)
        assert not was_corrected, (
            f"'{name}' should NOT be corrected, but was mapped to '{result}'"
        )
        assert result != "fibre, polyester", (
            f"'{name}' must not map to 'fibre, polyester' "
            "(polyacrylonitrile != polyester)"
        )

    def test_acrylic_batch_uncorrectable(self):
        inputs = ["acrylic", "fibre, acrylic"]
        _, _, uncorrectable = correct_material_list(inputs, VALID_NAMES)
        for name in inputs:
            assert name in uncorrectable, (
                f"'{name}' should appear in uncorrectable list"
            )


# ===================================================================
# 3. Cashmere / mohair must be uncorrectable (not sheep fleece)
# ===================================================================

class TestCashmereUncorrectable:
    """Cashmere (goat) and mohair (Angora goat) have no DB match."""

    @pytest.mark.parametrize("name", [
        "fibre, cashmere",
        "fibre, mohair",
        "textile, cashmere",
        "yarn, cashmere",
        "yarn, cashmere (not listed, assumed similar to silk)",
    ])
    def test_cashmere_not_mapped_to_sheep(self, name):
        result, was_corrected = correct_material_name(name, VALID_NAMES)
        assert not was_corrected, (
            f"'{name}' should NOT be corrected, but was mapped to '{result}'"
        )
        assert result != "sheep fleece in the grease", (
            f"'{name}' must not map to 'sheep fleece in the grease' "
            "(goat fibre != sheep fleece)"
        )
        # Also must not map to yarn, silk or textile, silk
        assert result != "yarn, silk", (
            f"'{name}' must not silently map to 'yarn, silk'"
        )
        assert result != "textile, silk", (
            f"'{name}' must not silently map to 'textile, silk'"
        )

    def test_cashmere_batch_uncorrectable(self):
        inputs = ["fibre, cashmere", "fibre, mohair"]
        _, _, uncorrectable = correct_material_list(inputs, VALID_NAMES)
        for name in inputs:
            assert name in uncorrectable, (
                f"'{name}' should appear in uncorrectable list"
            )


# ===================================================================
# 4. Tencel / lyocell / modal -> cellulose fibre (correct mapping)
# ===================================================================

class TestTencelCorrectMapping:
    """Tencel/lyocell/modal are regenerated cellulose -- mapping is valid."""

    @pytest.mark.parametrize("name", [
        "fibre, tencel",
        "fibre, Tencel",
        "fibre, TENCEL",
        "fibre, lyocell",
        "lyocell",
        "fibre, modal",
        "Tencel",
        "tencel",
    ])
    def test_tencel_maps_to_cellulose(self, name):
        result, was_corrected = correct_material_name(name, VALID_NAMES)
        assert was_corrected, (
            f"'{name}' should be corrected to 'cellulose fibre'"
        )
        assert result == "cellulose fibre", (
            f"'{name}' should map to 'cellulose fibre', got '{result}'"
        )
        assert result in VALID_NAMES


# ===================================================================
# 5. Structural integrity: no overlap between corrections and uncorrectable
# ===================================================================

class TestStructuralIntegrity:
    """Verify no material appears in both the correction map and the
    uncorrectable set, which would create ambiguous behavior."""

    def test_no_overlap(self):
        overlap = set(MATERIAL_NAME_CORRECTIONS.keys()) & UNCORRECTABLE_PROXY_MATERIALS
        assert not overlap, (
            f"Materials in BOTH corrections and uncorrectable: {overlap}"
        )

    def test_uncorrectable_not_in_valid_names(self):
        in_valid = UNCORRECTABLE_PROXY_MATERIALS & VALID_NAMES
        assert not in_valid, (
            f"Uncorrectable materials should not be valid DB names: {in_valid}"
        )

    def test_all_corrections_resolve_to_valid_names(self):
        invalid_targets = {
            k: v for k, v in MATERIAL_NAME_CORRECTIONS.items()
            if v not in VALID_NAMES
        }
        assert not invalid_targets, (
            f"Correction targets not in valid names: {invalid_targets}"
        )


# ===================================================================
# 6. Regression: normal corrections still work
# ===================================================================

class TestNormalCorrectionsStillWork:
    """Ensure the changes did not break existing valid corrections."""

    @pytest.mark.parametrize("input_name,expected", [
        ("cotton", "fibre, cotton"),
        ("nylon", "nylon 6"),
        ("polyester", "fibre, polyester"),
        ("fibre, wool", "wool, conventional, at farm gate"),
        ("fibre, hemp", "cottonized fibre, hemp"),
        ("linen", "fibre, flax"),
        ("viscose", "fibre, viscose"),
        ("leather", "cowhide, from beef, at slaughterhouse"),
    ])
    def test_standard_corrections(self, input_name, expected):
        result, was_corrected = correct_material_name(input_name, VALID_NAMES)
        assert was_corrected
        assert result == expected
        assert result in VALID_NAMES
