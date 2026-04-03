"""
Transport Generator for Layer 3 (V2).

Single-record transport leg generation with support for two-pass
correction feedback. Replaces the V1 TransportScenarioGenerator
and its 5-scenario variant logic.
"""

import logging
import time
from typing import Dict, List, Optional, Any

from data.data_generation.layer_3.config.config import Layer3Config
from data.data_generation.layer_3.io.layer2_reader import Layer2Record
from data.data_generation.layer_3.clients.api_client import Layer3Client
from data.data_generation.layer_3.prompts.builder import PromptBuilder
from data.data_generation.layer_3.models.models import TransportLeg, Layer3Record

logger = logging.getLogger(__name__)


class TransportGenerator:
    """Generates transport legs for Layer 2 records via LLM.

    Uses PromptBuilder for prompt assembly and Layer3Client for API
    calls. Each Layer 2 record produces exactly one Layer3Record
    containing a list of TransportLeg objects.
    """

    def __init__(
        self,
        config: Layer3Config,
        api_client: Layer3Client,
        prompt_builder: PromptBuilder,
    ):
        self.config = config
        self.api_client = api_client
        self.prompt_builder = prompt_builder
        self._system_prompt = prompt_builder.get_system_prompt()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def generate_for_record(
        self,
        record: Layer2Record,
        seed: int = 0,
        warehouse: str = "EU",
    ) -> Layer3Record:
        """Generate transport legs for a single Layer 2 record.

        Returns a Layer3Record with transport_legs and total_distance_km.
        Retries indefinitely on API failure (no rule-based fallback).
        """
        user_prompt = self.prompt_builder.build_user_prompt(
            record, seed=seed, warehouse=warehouse
        )

        max_gen_attempts = 5
        attempt = 0
        while attempt < max_gen_attempts:
            attempt += 1
            try:
                raw_legs = self.api_client.generate_transport_legs(
                    self._system_prompt, user_prompt
                )

                legs = self._parse_legs(raw_legs)
                if not legs:
                    logger.warning(
                        "Attempt %d for %s: no valid legs parsed, retrying",
                        attempt,
                        record.preprocessing_path_id,
                    )
                    time.sleep(min(2 ** attempt, 60))
                    continue

                # Reject truncated responses: need at least 1 leg per
                # material to have any chance of passing validation
                min_expected = len(record.materials)
                if len(legs) < min_expected:
                    logger.warning(
                        "Attempt %d for %s: truncated response "
                        "(%d legs < %d materials), retrying",
                        attempt,
                        record.preprocessing_path_id,
                        len(legs),
                        min_expected,
                    )
                    time.sleep(min(2 ** attempt, 60))
                    continue

                total_distance_km = sum(
                    leg.distance_km for leg in legs
                )

                layer3_record = self._assemble_record(
                    record, legs, total_distance_km
                )

                logger.info(
                    "Generated %d legs (%.1f km total) for %s in %d attempt(s)",
                    len(legs),
                    total_distance_km,
                    record.preprocessing_path_id,
                    attempt,
                )
                return layer3_record

            except Exception as exc:
                logger.warning(
                    "Attempt %d for %s failed: %s. Retrying ...",
                    attempt,
                    record.preprocessing_path_id,
                    exc,
                )
                time.sleep(min(2 ** attempt, 60))

        raise RuntimeError(
            f"Failed to generate legs for {record.preprocessing_path_id} "
            f"after {max_gen_attempts} attempts"
        )

    def regenerate_with_feedback(
        self,
        record: Layer2Record,
        failures: List[str],
        seed: int = 0,
        warehouse: str = "EU",
    ) -> Optional[Layer3Record]:
        """Regenerate transport legs with correction feedback.

        Used in the two-pass validation flow. Returns None if
        regeneration fails after max_retries attempts.
        """
        if not failures:
            return None

        correction_prompt = self.prompt_builder.build_correction_prompt(
            record, failures, seed=seed, warehouse=warehouse
        )

        max_retries: int = getattr(self.config, "max_retries", 3)

        for attempt in range(1, max_retries + 1):
            try:
                raw_legs = self.api_client.generate_transport_legs(
                    self._system_prompt, correction_prompt
                )

                legs = self._parse_legs(raw_legs)
                if not legs:
                    logger.warning(
                        "Correction attempt %d/%d for %s: "
                        "no valid legs parsed",
                        attempt,
                        max_retries,
                        record.preprocessing_path_id,
                    )
                    if attempt < max_retries:
                        time.sleep(min(2 ** attempt, 60))
                    continue

                total_distance_km = sum(
                    leg.distance_km for leg in legs
                )

                layer3_record = self._assemble_record(
                    record, legs, total_distance_km
                )

                logger.info(
                    "Regenerated %d legs (%.1f km total) for %s "
                    "on correction attempt %d/%d",
                    len(legs),
                    total_distance_km,
                    record.preprocessing_path_id,
                    attempt,
                    max_retries,
                )
                return layer3_record

            except Exception as exc:
                logger.warning(
                    "Correction attempt %d/%d for %s failed: %s",
                    attempt,
                    max_retries,
                    record.preprocessing_path_id,
                    exc,
                )
                if attempt < max_retries:
                    time.sleep(min(2 ** attempt, 60))

        logger.error(
            "Regeneration failed for %s after %d attempts",
            record.preprocessing_path_id,
            max_retries,
        )
        return None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_legs(
        self, raw_legs: List[Dict[str, Any]]
    ) -> List[TransportLeg]:
        """Parse raw API response dicts into TransportLeg objects."""
        legs: List[TransportLeg] = []
        for raw in raw_legs:
            try:
                leg = TransportLeg.from_dict(raw)
                legs.append(leg)
            except (KeyError, ValueError, TypeError) as exc:
                logger.warning("Failed to parse leg: %s", exc)
        return legs

    @staticmethod
    def _assemble_record(
        record: Layer2Record,
        legs: List[TransportLeg],
        total_distance_km: float,
    ) -> Layer3Record:
        """Assemble a Layer3Record from Layer2Record fields and legs."""
        return Layer3Record(
            category_id=record.category_id,
            category_name=record.category_name,
            subcategory_id=record.subcategory_id,
            subcategory_name=record.subcategory_name,
            materials=record.materials,
            material_weights_kg=record.material_weights_kg,
            material_percentages=record.material_percentages,
            total_weight_kg=record.total_weight_kg,
            preprocessing_path_id=record.preprocessing_path_id,
            preprocessing_steps=record.preprocessing_steps,
            step_material_mapping=record.step_material_mapping,
            transport_legs=legs,
            total_distance_km=total_distance_km,
        )
