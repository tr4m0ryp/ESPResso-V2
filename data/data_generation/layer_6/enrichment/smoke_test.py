#!/usr/bin/env python3
"""Integration smoke test for Layer 6 enrichment pipeline.

Verifies all enrichment and modified core components work together
using mock data only -- no API calls, no file I/O on real datasets.

Run:
    python3 -m data.data_generation.layer_6.enrichment.smoke_test
    python3 data/data_generation/layer_6/enrichment/smoke_test.py

Exit code 0 if all tests pass, 1 if any fail.
"""

import json
import logging
import sys
from pathlib import Path
from typing import List, Tuple

_project_root = Path(__file__).resolve().parent.parent.parent.parent.parent
sys.path.insert(0, str(_project_root))
logging.basicConfig(level=logging.WARNING, format="%(levelname)s: %(message)s")

_results: List[Tuple[str, bool, str]] = []


def _record(name: str, passed: bool, detail: str = "") -> None:
    _results.append((name, passed, detail))
    status = "PASS" if passed else "FAIL"
    msg = f"  [{status}] {name}"
    if detail:
        msg += f" -- {detail}"
    print(msg)


def _check(name: str, checks: List[Tuple[str, bool]]) -> None:
    """Run a list of (label, bool) checks and record result."""
    all_ok = all(ok for _, ok in checks)
    failed = [lbl for lbl, ok in checks if not ok]
    _record(name, all_ok, "; ".join(failed))


def test_imports() -> None:
    try:
        from data.data_generation.layer_6.enrichment.config import EnrichmentConfig
        from data.data_generation.layer_6.enrichment.data_joiner import (
            join_transport_legs, extract_pp_id)
        from data.data_generation.layer_6.enrichment.prompt_builder import (
            get_system_prompt, build_batch_prompt, strip_leg_fields)
        from data.data_generation.layer_6.enrichment.validator import (
            validate_extraction, ValidationResult, FailedRecordCollector)
        from data.data_generation.layer_6.enrichment.checkpoint import CheckpointManager
        from data.data_generation.layer_6.enrichment.orchestrator import EnrichmentOrchestrator
        from data.data_generation.layer_6.enrichment.client import EnrichmentClient
        from data.data_generation.layer_6.config.config import (
            Layer6Config, TRANSPORT_EMISSION_FACTORS,
            TRANSPORT_DISTANCES_COL, TRANSPORT_FRACTIONS_COL, TRANSPORT_EF_COL)
        from data.data_generation.layer_6.core.components import (
            calculate_transport_from_actuals, calculate_transport_logit)
        from data.data_generation.layer_6.core.databases import CalculationResult
        from data.data_generation.layer_6.core.transport_model import TransportModeModel
        _record("imports", True)
    except Exception as exc:
        _record("imports", False, str(exc))


def test_enrichment_config() -> None:
    from data.data_generation.layer_6.enrichment.config import EnrichmentConfig
    cfg = EnrichmentConfig()
    _check("enrichment_config_defaults", [
        ("batch_size > 0", cfg.batch_size > 0),
        ("checkpoint_interval > 0", cfg.checkpoint_interval > 0),
        ("max_retries > 0", cfg.max_retries > 0),
        ("distance_tolerance in (0,1)", 0 < cfg.distance_tolerance < 1),
        ("output_path is str", isinstance(cfg.output_path, str)),
        ("checkpoint_dir is str", isinstance(cfg.checkpoint_dir, str)),
        ("api_key returns str", isinstance(cfg.api_key, str)),
    ])


def test_layer6_config() -> None:
    from data.data_generation.layer_6.config.config import (
        Layer6Config, TRANSPORT_DISTANCES_COL, TRANSPORT_FRACTIONS_COL,
        TRANSPORT_EF_COL)
    cfg = Layer6Config()
    _check("layer6_config_new_fields", [
        ("has enriched_input_path", hasattr(cfg, "enriched_input_path")),
        ("has use_enriched_transport", hasattr(cfg, "use_enriched_transport")),
        ("use_enriched_transport default True", cfg.use_enriched_transport is True),
        ("TRANSPORT_DISTANCES_COL", TRANSPORT_DISTANCES_COL == "transport_mode_distances_km"),
        ("TRANSPORT_FRACTIONS_COL", TRANSPORT_FRACTIONS_COL == "transport_mode_fractions"),
        ("TRANSPORT_EF_COL", TRANSPORT_EF_COL == "effective_ef_g_co2e_tkm"),
    ])


def test_extract_pp_id() -> None:
    from data.data_generation.layer_6.enrichment.data_joiner import extract_pp_id
    cases = [
        ("cl-2-3_pp-015810", "pp-015810"),
        ("cl-0-1_pp-000001", "pp-000001"),
        ("no-match-here", ""),
        ("", ""),
        (None, ""),
    ]
    all_ok = True
    parts = []
    for inp, expected in cases:
        got = extract_pp_id(inp)
        if got != expected:
            all_ok = False
            parts.append(f"{inp!r}->{got!r} (want {expected!r})")
    _record("extract_pp_id", all_ok, "; ".join(parts))


def test_prompt_builder() -> None:
    from data.data_generation.layer_6.enrichment.prompt_builder import (
        get_system_prompt, build_batch_prompt, strip_leg_fields)
    sp = get_system_prompt()
    sp_ok = len(sp) > 100 and "transport" in sp.lower()
    full_leg = {
        "transport_modes": ["road"], "distance_km": 500.0,
        "reasoning": "Trucked 500 km", "from_coordinates": [10, 20],
        "to_coordinates": [30, 40], "from_location": "A",
        "to_location": "B", "leg_index": 1,
    }
    stripped = strip_leg_fields(full_leg)
    strip_ok = set(stripped.keys()) == {"transport_modes", "distance_km", "reasoning"}
    records = [
        {"record_id": f"cl-0-{i}_pp-00000{i}", "total_distance_km": 1000.0 * i,
         "transport_legs": json.dumps([{
             "transport_modes": ["road", "sea"], "distance_km": 500.0 * i,
             "reasoning": f"Road {200*i} km then sea {300*i} km",
             "from_coordinates": [0, 0], "to_coordinates": [1, 1]}])}
        for i in range(1, 4)]
    prompt = build_batch_prompt(records)
    ids_ok = all(f"pp-00000{i}" in prompt for i in range(1, 4))
    no_coords = "from_coordinates" not in prompt
    all_ok = sp_ok and strip_ok and ids_ok and no_coords
    parts = []
    if not sp_ok:
        parts.append("system prompt invalid")
    if not strip_ok:
        parts.append(f"strip kept wrong keys: {set(stripped.keys())}")
    if not ids_ok:
        parts.append("batch prompt missing record ids")
    if not no_coords:
        parts.append("batch prompt still contains coordinates")
    _record("prompt_builder", all_ok, "; ".join(parts))


def test_validator() -> None:
    from data.data_generation.layer_6.enrichment.validator import (
        validate_extraction, FailedRecordCollector)
    _modes_zero = {"rail_km": 0.0, "air_km": 0.0, "inland_waterway_km": 0.0}
    # Case 1: passes (exact match, 0% discrepancy)
    good = {"id": "rec-pass", "road_km": 600.0, "sea_km": 400.0, **_modes_zero}
    r1 = validate_extraction(good, total_distance_km=1000.0)
    pass_ok = r1.is_valid and r1.discrepancy_pct == 0.0
    # Case 2: fails (1200/1000 = 20% discrepancy)
    bad = {"id": "rec-fail", "road_km": 800.0, "sea_km": 400.0, **_modes_zero}
    r2 = validate_extraction(bad, total_distance_km=1000.0)
    fail_ok = not r2.is_valid and r2.discrepancy_pct > 0.01
    # Case 3: zero total, zero extraction -> valid
    z_ok = {"id": "rec-z-ok", "road_km": 0.0, "sea_km": 0.0, **_modes_zero}
    r3 = validate_extraction(z_ok, total_distance_km=0.0)
    zero_pass = r3.is_valid and r3.discrepancy_pct == 0.0
    # Case 4: zero total, non-zero extraction -> invalid
    z_bad = {"id": "rec-z-bad", "road_km": 100.0, "sea_km": 0.0, **_modes_zero}
    r4 = validate_extraction(z_bad, total_distance_km=0.0)
    zero_fail = not r4.is_valid and r4.discrepancy_pct == 1.0
    # FailedRecordCollector
    c = FailedRecordCollector()
    c.record_pass()
    c.add_failure({"record_id": "r1"}, r2)
    s = c.summary()
    col_ok = (s["total_validated"] == 2 and s["passed"] == 1
              and s["failed"] == 1 and len(c.get_retry_batch()) == 1)
    all_ok = pass_ok and fail_ok and zero_pass and zero_fail and col_ok
    parts = []
    if not pass_ok:
        parts.append("good record flagged invalid")
    if not fail_ok:
        parts.append("bad record not flagged")
    if not zero_pass:
        parts.append("zero/zero edge not valid")
    if not zero_fail:
        parts.append("zero-total non-zero extraction not invalid")
    if not col_ok:
        parts.append(f"collector summary wrong: {s}")
    _record("validator", all_ok, "; ".join(parts))


def test_transport_from_actuals() -> None:
    from data.data_generation.layer_6.core.components import calculate_transport_from_actuals
    from data.data_generation.layer_6.config.config import TRANSPORT_EMISSION_FACTORS
    dists = {"road": 1000.0, "sea": 5000.0, "rail": 0.0, "air": 0.0,
             "inland_waterway": 0.0}
    footprint, _, fracs, eff_ef = calculate_transport_from_actuals(
        weight_kg=1.0, mode_distances_km=dists,
        emission_factors=TRANSPORT_EMISSION_FACTORS)
    # road: (1/1000)*1000*(74/1000)=0.074; sea: (1/1000)*5000*(10.3/1000)=0.0515
    exp_fp = 0.1255
    fp_ok = abs(footprint - exp_fp) < 1e-6
    exp_rd = 1000.0 / 6000.0
    exp_sea = 5000.0 / 6000.0
    frac_ok = (abs(fracs.get("road", -1) - exp_rd) < 1e-4
               and abs(fracs.get("sea", -1) - exp_sea) < 1e-4)
    exp_ef = exp_rd * 74.0 + exp_sea * 10.3
    ef_ok = abs(eff_ef - exp_ef) < 0.01
    parts = []
    if not fp_ok:
        parts.append(f"footprint {footprint} != {exp_fp}")
    if not frac_ok:
        parts.append(f"fractions wrong: {fracs}")
    if not ef_ok:
        parts.append(f"effective_ef {eff_ef:.4f} != {exp_ef:.4f}")
    _record("transport_from_actuals", fp_ok and frac_ok and ef_ok, "; ".join(parts))


def test_transport_logit() -> None:
    from data.data_generation.layer_6.core.components import calculate_transport_logit
    from data.data_generation.layer_6.core.transport_model import TransportModeModel
    model = TransportModeModel()
    fp, probs, wef = calculate_transport_logit(2.0, 3000.0, model)
    _check("transport_logit_backward_compat", [
        ("footprint >= 0", fp >= 0),
        ("probs sum ~1", abs(sum(probs.values()) - 1.0) < 1e-6),
        ("weighted_ef > 0", wef > 0),
        ("has 5 modes", len(probs) == 5),
    ])


def test_calculation_result_fields() -> None:
    from data.data_generation.layer_6.core.databases import CalculationResult
    r = CalculationResult()
    _check("calculation_result_fields", [
        ("has transport_mode_fractions", hasattr(r, "transport_mode_fractions")),
        ("fractions is dict", isinstance(r.transport_mode_fractions, dict)),
        ("has transport_mode_probabilities", hasattr(r, "transport_mode_probabilities")),
        ("has weighted_ef", hasattr(r, "weighted_ef_g_co2e_tkm")),
    ])


def test_output_column_constants() -> None:
    from data.data_generation.layer_6.config.config import (
        TRANSPORT_DISTANCES_COL, TRANSPORT_FRACTIONS_COL, TRANSPORT_EF_COL)
    _check("output_column_constants", [
        ("distances", TRANSPORT_DISTANCES_COL == "transport_mode_distances_km"),
        ("fractions", TRANSPORT_FRACTIONS_COL == "transport_mode_fractions"),
        ("ef", TRANSPORT_EF_COL == "effective_ef_g_co2e_tkm"),
    ])


def main() -> int:
    print("=" * 60)
    print(" Layer 6 Enrichment -- Integration Smoke Test")
    print("=" * 60)
    print()
    test_imports()
    test_enrichment_config()
    test_layer6_config()
    test_extract_pp_id()
    test_prompt_builder()
    test_validator()
    test_transport_from_actuals()
    test_transport_logit()
    test_calculation_result_fields()
    test_output_column_constants()
    print()
    print("-" * 60)
    passed = sum(1 for _, ok, _ in _results if ok)
    failed = sum(1 for _, ok, _ in _results if not ok)
    print(f"Results: {passed}/{len(_results)} passed, {failed} failed")
    if failed > 0:
        print("\nFailed tests:")
        for name, ok, detail in _results:
            if not ok:
                print(f"  - {name}: {detail}")
        print("\nSMOKE TEST FAILED")
        return 1
    print("\nALL TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
