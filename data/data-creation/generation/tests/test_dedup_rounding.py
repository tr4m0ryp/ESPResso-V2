"""
Tests that fingerprint-based deduplication preserves meaningful percentage
differences. Previously, percentages were rounded to the nearest 10, which
collapsed compositions like 85% cotton and 84% cotton into the same bucket.
"""

import sys
import types
import unittest
import importlib.util
from pathlib import Path

# ---------------------------------------------------------------------------
# Locate the generator module on disk and load it in isolation.
# We cannot rely on the normal package import chain because the layer_1
# __init__.py pulls in heavy dependencies (API clients, config, etc.) that
# are irrelevant to this unit test.
# ---------------------------------------------------------------------------

_GENERATOR_PATH = (
    Path(__file__).resolve().parents[1]
    / "layer_1" / "core" / "generator.py"
)

# Provide lightweight stubs for every module that generator.py imports so
# that the import succeeds without the real dependencies.
_STUB_MODULES = {
    "data": types.ModuleType("data"),
    "data.data_generation": types.ModuleType("data.data_generation"),
    "data.data_generation.layer_1": types.ModuleType("data.data_generation.layer_1"),
    "data.data_generation.layer_1.config": types.ModuleType("data.data_generation.layer_1.config"),
    "data.data_generation.layer_1.config.config": types.ModuleType("data.data_generation.layer_1.config.config"),
    "data.data_generation.layer_1.models": types.ModuleType("data.data_generation.layer_1.models"),
    "data.data_generation.layer_1.models.materials": types.ModuleType("data.data_generation.layer_1.models.materials"),
    "data.data_generation.layer_1.models.material_corrector": types.ModuleType("data.data_generation.layer_1.models.material_corrector"),
    "data.data_generation.layer_1.models.taxonomy": types.ModuleType("data.data_generation.layer_1.models.taxonomy"),
    "data.data_generation.layer_1.clients": types.ModuleType("data.data_generation.layer_1.clients"),
    "data.data_generation.layer_1.clients.api_client": types.ModuleType("data.data_generation.layer_1.clients.api_client"),
    "data.data_generation.layer_1.prompts": types.ModuleType("data.data_generation.layer_1.prompts"),
    "data.data_generation.layer_1.prompts.prompts": types.ModuleType("data.data_generation.layer_1.prompts.prompts"),
    "data.data_generation.shared": types.ModuleType("data.data_generation.shared"),
    "data.data_generation.shared.api_client": types.ModuleType("data.data_generation.shared.api_client"),
    "data.data_generation.shared.reality_check_models": types.ModuleType("data.data_generation.shared.reality_check_models"),
}

# Populate stubs with placeholder attributes that generator.py expects.
for _name, _mod in _STUB_MODULES.items():
    _mod.Layer1Config = type("Layer1Config", (), {})
    _mod.MaterialDatabase = type("MaterialDatabase", (), {})
    _mod.MaterialCategoryMapper = type("MaterialCategoryMapper", (), {})
    _mod.Material = type("Material", (), {})
    _mod.correct_material_list = lambda *a, **kw: ([], [], [])
    _mod.TaxonomyLoader = type("TaxonomyLoader", (), {})
    _mod.TaxonomyItem = type("TaxonomyItem", (), {})
    _mod.Layer1Client = type("Layer1Client", (), {})
    _mod.PromptBuilder = type("PromptBuilder", (), {})
    _mod.APIError = type("APIError", (Exception,), {})
    _mod.RecordCheckResult = type("RecordCheckResult", (), {})

# Inject all stubs before loading the generator.
_saved = {}
for _name, _mod in _STUB_MODULES.items():
    _saved[_name] = sys.modules.get(_name)
    sys.modules[_name] = _mod

try:
    _spec = importlib.util.spec_from_file_location(
        "data.data_generation.layer_1.core.generator",
        str(_GENERATOR_PATH),
    )
    _generator_mod = importlib.util.module_from_spec(_spec)
    sys.modules[_spec.name] = _generator_mod
    _spec.loader.exec_module(_generator_mod)
finally:
    # Restore original sys.modules state for anything we overwrote.
    for _name, _original in _saved.items():
        if _original is None:
            sys.modules.pop(_name, None)
        else:
            sys.modules[_name] = _original

ProductComposition = _generator_mod.ProductComposition
composition_fingerprint = _generator_mod.composition_fingerprint
deduplicate_batch = _generator_mod.deduplicate_batch


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_composition(
    materials: list,
    percentages: list,
) -> ProductComposition:
    """Helper to build a minimal ProductComposition for testing."""
    weights = [p / 100.0 for p in percentages]
    return ProductComposition(
        category_id="CAT-001",
        category_name="T-Shirts",
        subcategory_id="SUB-001",
        subcategory_name="Casual T-Shirts",
        materials=materials,
        material_weights_kg=weights,
        material_percentages=percentages,
        total_weight_kg=sum(weights),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestCompositionFingerprint(unittest.TestCase):
    """Verify that exact percentages are used in fingerprints."""

    def test_different_percentages_produce_different_fingerprints(self):
        """85% cotton/15% polyester != 84% cotton/16% polyester."""
        comp_a = _make_composition(["Cotton", "Polyester"], [85, 15])
        comp_b = _make_composition(["Cotton", "Polyester"], [84, 16])

        fp_a = composition_fingerprint(comp_a)
        fp_b = composition_fingerprint(comp_b)

        self.assertNotEqual(
            fp_a,
            fp_b,
            "Compositions with 85% vs 84% cotton must have distinct fingerprints",
        )

    def test_identical_compositions_share_fingerprint(self):
        """Two truly identical compositions should still deduplicate."""
        comp_a = _make_composition(["Cotton", "Polyester"], [85, 15])
        comp_b = _make_composition(["Cotton", "Polyester"], [85, 15])

        fp_a = composition_fingerprint(comp_a)
        fp_b = composition_fingerprint(comp_b)

        self.assertEqual(
            fp_a,
            fp_b,
            "Identical compositions must share the same fingerprint",
        )

    def test_material_order_does_not_affect_fingerprint(self):
        """Fingerprint must be order-independent."""
        comp_a = _make_composition(["Cotton", "Polyester"], [85, 15])
        comp_b = _make_composition(["Polyester", "Cotton"], [15, 85])

        fp_a = composition_fingerprint(comp_a)
        fp_b = composition_fingerprint(comp_b)

        self.assertEqual(
            fp_a,
            fp_b,
            "Material ordering should not change the fingerprint",
        )


class TestDeduplicateBatch(unittest.TestCase):
    """Verify that deduplicate_batch respects exact fingerprints."""

    def test_close_percentages_not_deduplicated(self):
        """85% cotton and 84% cotton are distinct and must both survive."""
        comp_a = _make_composition(["Cotton", "Polyester"], [85, 15])
        comp_b = _make_composition(["Cotton", "Polyester"], [84, 16])

        seen = set()
        unique, removed = deduplicate_batch([comp_a, comp_b], seen)

        self.assertEqual(len(unique), 2, "Both compositions must be kept")
        self.assertEqual(removed, 0, "No duplicates should be removed")

    def test_true_duplicates_still_deduplicated(self):
        """Exact duplicates must still be removed."""
        comp_a = _make_composition(["Cotton", "Polyester"], [85, 15])
        comp_b = _make_composition(["Cotton", "Polyester"], [85, 15])

        seen = set()
        unique, removed = deduplicate_batch([comp_a, comp_b], seen)

        self.assertEqual(len(unique), 1, "Only one copy should remain")
        self.assertEqual(removed, 1, "One duplicate should be removed")

    def test_cross_batch_dedup_with_exact_match(self):
        """A fingerprint already in seen_fingerprints must be rejected."""
        comp_a = _make_composition(["Cotton", "Polyester"], [85, 15])

        seen = set()
        seen.add(composition_fingerprint(comp_a))

        comp_b = _make_composition(["Cotton", "Polyester"], [85, 15])
        unique, removed = deduplicate_batch([comp_b], seen)

        self.assertEqual(len(unique), 0, "Cross-batch duplicate must be caught")
        self.assertEqual(removed, 1)

    def test_cross_batch_no_false_positive(self):
        """A close-but-different fingerprint must not be rejected cross-batch."""
        comp_a = _make_composition(["Cotton", "Polyester"], [85, 15])

        seen = set()
        seen.add(composition_fingerprint(comp_a))

        comp_b = _make_composition(["Cotton", "Polyester"], [84, 16])
        unique, removed = deduplicate_batch([comp_b], seen)

        self.assertEqual(
            len(unique), 1,
            "84/16 must not be rejected when only 85/15 is in seen set",
        )
        self.assertEqual(removed, 0)


if __name__ == "__main__":
    unittest.main()
