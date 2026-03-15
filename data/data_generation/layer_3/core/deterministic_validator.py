"""
Deterministic Validator for Layer 3: Stage 1 Validation

Implements 12 code-based checks on every Layer 3 record without LLM calls,
plus corrective auto-fixes. See LAYER3_DESIGN.md sections 9.2 and 9.3.
"""

import copy
import logging
import re
from typing import Dict, List, Set, Tuple

from data.data_generation.layer_3.config.config import (
    ALLOWED_TRANSPORT_MODES, Layer3Config,
)
from data.data_generation.layer_3.models.models import (
    Layer3Record, TransportLeg, ValidationResult,
)

logger = logging.getLogger(__name__)

# 13 required TransportLeg fields and their expected types.
_LEG_FIELDS: List[Tuple[str, type]] = [
    ("leg_index", int), ("material", str), ("from_step", str),
    ("to_step", str), ("from_location", str), ("to_location", str),
    ("from_lat", float), ("from_lon", float), ("to_lat", float),
    ("to_lon", float), ("distance_km", float), ("transport_modes", list),
    ("reasoning", str),
]

_Pair = Tuple[List[str], List[str]]  # (errors, warnings)


class DeterministicValidator:
    """Runs 12 deterministic checks on Layer 3 records."""

    def __init__(self, config: Layer3Config):
        self.config = config

    # -- public API --------------------------------------------------------

    def validate(self, record: Layer3Record) -> ValidationResult:
        """Run all 12 deterministic checks. Returns a ValidationResult."""
        errors: List[str] = []
        warnings: List[str] = []
        for check in [
            self._check_schema_completeness, self._check_coordinate_range,
            self._check_land_validation, self._check_distance_bounds,
            self._check_material_coverage, self._check_step_coverage,
            self._check_leg_continuity, self._check_transport_modes,
            self._check_reasoning_quality, self._check_convergence,
            self._check_warehouse_terminus, self._check_leg_indexing,
        ]:
            errs, warns = check(record)
            errors.extend(errs)
            warnings.extend(warns)
        return ValidationResult(is_valid=len(errors) == 0,
                                errors=errors, warnings=warnings)

    def validate_and_correct(self, record: Layer3Record) -> ValidationResult:
        """Run validation, then apply corrective fixes if needed."""
        result = self.validate(record)
        if result.warnings or not result.is_valid:
            corrected, corrections = self._apply_corrections(record)
            result.corrections_applied = corrections
            result.corrected_record = corrected
        return result

    # -- 12 checks (each returns (errors, warnings)) ----------------------

    def _check_schema_completeness(self, record: Layer3Record) -> _Pair:
        """Check 1: every leg has all 13 fields with correct types."""
        errors: List[str] = []
        for leg in record.transport_legs:
            for name, etype in _LEG_FIELDS:
                if not hasattr(leg, name):
                    errors.append(f"Leg {leg.leg_index}: missing '{name}'")
                elif not isinstance(getattr(leg, name), etype):
                    errors.append(
                        f"Leg {leg.leg_index}: '{name}' expected "
                        f"{etype.__name__}, got {type(getattr(leg, name)).__name__}")
        return errors, []

    def _check_coordinate_range(self, record: Layer3Record) -> _Pair:
        """Check 2: lat in [-90,90], lon in [-180,180]."""
        errors: List[str] = []
        for leg in record.transport_legs:
            for lbl, lat, lon in [("from", leg.from_lat, leg.from_lon),
                                  ("to", leg.to_lat, leg.to_lon)]:
                if not (-90 <= lat <= 90):
                    errors.append(
                        f"Leg {leg.leg_index}: {lbl}_lat {lat} out of [-90,90]")
                if not (-180 <= lon <= 180):
                    errors.append(
                        f"Leg {leg.leg_index}: {lbl}_lon {lon} out of [-180,180]")
        return errors, []

    def _check_land_validation(self, record: Layer3Record) -> _Pair:
        """Check 3: stub -- always passes (land/sea mask not yet loaded)."""
        return [], []

    def _check_distance_bounds(self, record: Layer3Record) -> _Pair:
        """Check 4: each leg distance within configured bounds."""
        errors: List[str] = []
        lo, hi = self.config.min_leg_distance_km, self.config.max_leg_distance_km
        for leg in record.transport_legs:
            if leg.distance_km < lo or leg.distance_km > hi:
                errors.append(
                    f"Leg {leg.leg_index}: distance_km {leg.distance_km} "
                    f"outside [{lo}, {hi}]")
        return errors, []

    def _check_material_coverage(self, record: Layer3Record) -> _Pair:
        """Check 5: every material in record.materials appears in legs."""
        errors: List[str] = []
        leg_mats = {leg.material for leg in record.transport_legs}
        for mat in record.materials:
            if mat not in leg_mats:
                errors.append(f"Material '{mat}' has no transport legs")
        return errors, []

    def _check_step_coverage(self, record: Layer3Record) -> _Pair:
        """Check 6: every step in step_material_mapping appears in legs."""
        warnings: List[str] = []
        for mat, steps in record.step_material_mapping.items():
            leg_steps: Set[str] = set()
            for leg in record.transport_legs:
                if leg.material == mat:
                    leg_steps.update([leg.from_step, leg.to_step])
            for step in steps:
                if step not in leg_steps:
                    warnings.append(
                        f"Step '{step}' for material '{mat}' not in legs")
        return [], warnings

    def _check_leg_continuity(self, record: Layer3Record) -> _Pair:
        """Check 7: to_location of leg N == from_location of leg N+1."""
        errors: List[str] = []
        for mat, legs in self._chains(record).items():
            for i in range(len(legs) - 1):
                if legs[i].to_location != legs[i + 1].from_location:
                    errors.append(
                        f"Material '{mat}': discontinuity at legs "
                        f"{legs[i].leg_index}->{legs[i+1].leg_index} "
                        f"('{legs[i].to_location}' != "
                        f"'{legs[i+1].from_location}')")
        return errors, []

    def _check_transport_modes(self, record: Layer3Record) -> _Pair:
        """Check 8: modes in allowed set, non-empty per leg."""
        errors: List[str] = []
        for leg in record.transport_legs:
            if not leg.transport_modes:
                errors.append(f"Leg {leg.leg_index}: transport_modes is empty")
                continue
            for mode in leg.transport_modes:
                if mode not in ALLOWED_TRANSPORT_MODES:
                    errors.append(f"Leg {leg.leg_index}: invalid mode '{mode}'")
        return errors, []

    def _check_reasoning_quality(self, record: Layer3Record) -> _Pair:
        """Check 9: reasoning non-empty, >= min_reasoning_length."""
        warnings: List[str] = []
        min_len = self.config.min_reasoning_length
        for leg in record.transport_legs:
            text = (leg.reasoning or "").strip()
            if not text:
                warnings.append(f"Leg {leg.leg_index}: reasoning is empty")
            elif len(text) < min_len:
                warnings.append(
                    f"Leg {leg.leg_index}: reasoning too short "
                    f"({len(text)} < {min_len})")
        return [], warnings

    def _check_convergence(self, record: Layer3Record) -> _Pair:
        """Check 10: all materials converge at the assembly step."""
        warnings: List[str] = []
        if len(record.materials) <= 1:
            return [], []
        chains = self._chains(record)
        # Collect to_steps per material chain
        per_mat: List[List[str]] = []
        for mat in record.materials:
            per_mat.append([l.to_step for l in chains.get(mat, [])])
        if any(not s for s in per_mat):
            return [], []
        # Find shared to_steps across all material chains
        shared: Set[str] = set(per_mat[0])
        for steps in per_mat[1:]:
            shared &= set(steps)
        if not shared:
            warnings.append("No shared assembly step across materials")
            return [], warnings
        # Pick the assembly step (latest shared step by max position)
        assembly = max(shared, key=lambda s: max(
            (i for sl in per_mat for i, st in enumerate(sl) if st == s),
            default=0))
        # Verify all materials route to the same location at that step
        locs: Set[str] = set()
        for mat in record.materials:
            for leg in chains.get(mat, []):
                if leg.to_step == assembly:
                    locs.add(leg.to_location)
                    break
        if len(locs) > 1:
            warnings.append(
                f"Materials converge at '{assembly}' but at "
                f"different locations: {sorted(locs)}")
        return [], warnings

    def _check_warehouse_terminus(self, record: Layer3Record) -> _Pair:
        """Check 11: all material chains end at the same destination."""
        warnings: List[str] = []
        endpoints: Set[str] = set()
        for legs in self._chains(record).values():
            if legs:
                endpoints.add(legs[-1].to_location)
        if len(endpoints) > 1:
            warnings.append(
                f"Material chains end at different destinations: "
                f"{sorted(endpoints)}")
        return [], warnings

    def _check_leg_indexing(self, record: Layer3Record) -> _Pair:
        """Check 12: sequential indices from 0, no gaps per material."""
        warnings: List[str] = []
        for mat, legs in self._chains(record).items():
            indices = [l.leg_index for l in legs]
            if indices != list(range(len(indices))):
                warnings.append(
                    f"Material '{mat}': leg indices {indices} "
                    f"should be {list(range(len(indices)))}")
        return [], warnings

    # -- corrective fixes --------------------------------------------------

    def _apply_corrections(
        self, record: Layer3Record,
    ) -> Tuple[Layer3Record, List[str]]:
        """Apply safe auto-fixes. Returns (corrected_record, corrections)."""
        rec = copy.deepcopy(record)
        fixes: List[str] = []
        # 1. Recompute total_distance_km
        computed = rec.compute_total_distance()
        if abs(computed - rec.total_distance_km) > 0.01:
            fixes.append(
                f"Recomputed total_distance_km: {rec.total_distance_km} -> {computed}")
            rec.total_distance_km = computed
        # 2. Round coordinates
        dp = self.config.coordinate_decimal_places
        coord_fixed = False
        for leg in rec.transport_legs:
            for attr in ("from_lat", "from_lon", "to_lat", "to_lon"):
                old = getattr(leg, attr)
                new = round(old, dp)
                if old != new:
                    setattr(leg, attr, new)
                    coord_fixed = True
        if coord_fixed:
            fixes.append(f"Rounded coordinates to {dp} decimal places")
        # 3. Strip/collapse whitespace in reasoning
        ws_fixed = False
        for leg in rec.transport_legs:
            cleaned = re.sub(r"\s+", " ", leg.reasoning).strip()
            if cleaned != leg.reasoning:
                leg.reasoning = cleaned
                ws_fixed = True
        if ws_fixed:
            fixes.append("Cleaned whitespace in reasoning fields")
        # 4. Re-index legs if gaps
        reindexed = False
        for legs in self._chains(rec).values():
            for new_idx, leg in enumerate(legs):
                if leg.leg_index != new_idx:
                    leg.leg_index = new_idx
                    reindexed = True
        if reindexed:
            fixes.append("Re-indexed legs sequentially from 0")
        return rec, fixes

    # -- helpers -----------------------------------------------------------

    def _chains(self, record: Layer3Record) -> Dict[str, List[TransportLeg]]:
        """Group legs by material, sorted by leg_index."""
        out: Dict[str, List[TransportLeg]] = {}
        for leg in record.transport_legs:
            out.setdefault(leg.material, []).append(leg)
        for legs in out.values():
            legs.sort(key=lambda l: l.leg_index)
        return out
