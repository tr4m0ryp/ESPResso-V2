"""
Passport Verifier for Layer 5: Upstream Hash Verification

Each upstream layer (1-4) stamps a SHA-256 validation hash on its output
records.  Layer 5 verifies these hashes instead of re-running deterministic
checks, making validation O(1) per layer per record.
"""

import hashlib
import json
import logging
from typing import Dict, List

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord,
    PassportVerificationResult,
)

logger = logging.getLogger(__name__)

# Fields whose JSON serialization forms each layer's passport hash.
LAYER_FIELDS: Dict[int, List[str]] = {
    1: [
        "materials",
        "material_weights_kg",
        "material_percentages",
        "total_weight_kg",
    ],
    2: [
        "preprocessing_path_id",
        "preprocessing_steps",
    ],
    3: [
        "transport_scenario_id",
        "total_transport_distance_km",
        "supply_chain_type",
    ],
    4: [
        "packaging_config_id",
        "packaging_categories",
        "packaging_masses_kg",
        "total_packaging_mass_kg",
    ],
}

_LAYER_NAMES = {
    1: "layer1",
    2: "layer2",
    3: "layer3",
    4: "layer4",
}

_PASSPORT_ATTR = {
    1: "layer1_passport_hash",
    2: "layer2_passport_hash",
    3: "layer3_passport_hash",
    4: "layer4_passport_hash",
}

_VALID_ATTR = {
    1: "layer1_hash_valid",
    2: "layer2_hash_valid",
    3: "layer3_hash_valid",
    4: "layer4_hash_valid",
}


class PassportVerifier:
    """Verifies upstream layer passport hashes on product records."""

    def __init__(self, config: Layer5Config):
        self.config = config

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def verify(
        self, record: CompleteProductRecord
    ) -> PassportVerificationResult:
        """Verify all upstream layer passports on a record.

        If ``passport_enabled`` is False in config, returns an all-True
        result immediately (bypass mode).
        """
        if not self.config.passport_enabled:
            return PassportVerificationResult(is_valid=True)

        missing: List[str] = []
        errors: List[str] = []
        layer_valid: Dict[str, bool] = {}

        for layer_num in (1, 2, 3, 4):
            stored_hash = getattr(record, _PASSPORT_ATTR[layer_num])

            if stored_hash is None:
                layer_name = _LAYER_NAMES[layer_num]
                missing.append(layer_name)
                layer_valid[_VALID_ATTR[layer_num]] = False
                logger.warning(
                    "Record %s is missing %s passport hash",
                    record.subcategory_id,
                    layer_name,
                )
                continue

            expected = self.compute_passport_hash(record, layer_num)
            if stored_hash != expected:
                layer_valid[_VALID_ATTR[layer_num]] = False
                errors.append(
                    "Layer %d hash mismatch for %s: "
                    "stored=%s, expected=%s"
                    % (layer_num, record.subcategory_id,
                       stored_hash, expected)
                )
            else:
                layer_valid[_VALID_ATTR[layer_num]] = True

        is_valid = (
            not missing
            and not errors
            and all(layer_valid.values())
        )

        return PassportVerificationResult(
            is_valid=is_valid,
            layer1_hash_valid=layer_valid.get(
                "layer1_hash_valid", True
            ),
            layer2_hash_valid=layer_valid.get(
                "layer2_hash_valid", True
            ),
            layer3_hash_valid=layer_valid.get(
                "layer3_hash_valid", True
            ),
            layer4_hash_valid=layer_valid.get(
                "layer4_hash_valid", True
            ),
            missing_passports=missing,
            errors=errors,
        )

    @staticmethod
    def compute_passport_hash(
        record: CompleteProductRecord, layer: int
    ) -> str:
        """Compute the expected passport hash for a given layer.

        JSON-serializes the layer's key fields with ``sort_keys=True``,
        then returns the hex-digest of the SHA-256 hash.
        """
        if layer not in LAYER_FIELDS:
            raise ValueError(
                "Invalid layer number: %d (expected 1-4)" % layer
            )

        field_names = LAYER_FIELDS[layer]
        payload = {
            name: getattr(record, name) for name in field_names
        }
        serialized = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def verify_batch(
        self, records: List[CompleteProductRecord]
    ) -> Dict[str, PassportVerificationResult]:
        """Verify passports for a batch of records.

        Returns a dict mapping ``subcategory_id`` to the verification
        result for that record.
        """
        results: Dict[str, PassportVerificationResult] = {}
        for record in records:
            results[record.subcategory_id] = self.verify(record)
        return results
