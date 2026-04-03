"""Cross-Layer Coherence Prompt Builder for Layer 5 Validation."""

import json
import logging
import re
from typing import Dict, List

from data.data_generation.layer_5.models.models import (
    CompleteProductRecord,
    CrossLayerCoherenceResult,
)

logger = logging.getLogger(__name__)

# Maximum records per batch to keep prompts within token limits
MAX_BATCH_SIZE = 50


class CoherencePromptBuilder:
    """Builds prompts for cross-layer coherence evaluation via LLM.

    The system prompt establishes the LLM as a cross-layer coherence
    evaluator that looks for contradictions between layers and assesses
    whether the full material->processing->transport->packaging lifecycle
    tells a plausible story.
    """

    def __init__(self):
        self._system_prompt = self._build_system_prompt()

    @property
    def system_prompt(self) -> str:
        """Return the system prompt for coherence evaluation."""
        return self._system_prompt

    def build_batch_prompt(
        self, records: List[CompleteProductRecord]
    ) -> str:
        """Build prompt for evaluating a batch of records.

        Args:
            records: Up to 50 CompleteProductRecord instances.

        Returns:
            Formatted prompt string with all records and expected
            JSON output format.
        """
        if len(records) > MAX_BATCH_SIZE:
            logger.warning(
                "Batch size %d exceeds maximum %d; truncating",
                len(records),
                MAX_BATCH_SIZE,
            )
            records = records[:MAX_BATCH_SIZE]

        record_blocks = []
        for i, record in enumerate(records):
            block = self._format_record(i + 1, record)
            record_blocks.append(block)

        records_text = "\n\n".join(record_blocks)

        return (
            f"Evaluate the following {len(records)} product records for "
            f"cross-layer coherence.\n\n"
            f"{records_text}\n\n"
            f"For EACH record, output a JSON object keyed by its "
            f"record_N identifier (record_1, record_2, etc.).\n\n"
            f"Expected output format:\n"
            f"{{\n"
            f'  "record_1": {{\n'
            f'    "lifecycle_coherence_score": 0.XX,\n'
            f'    "cross_layer_contradiction_score": 0.XX,\n'
            f'    "overall_coherence_score": 0.XX,\n'
            f'    "contradictions_found": ["specific contradiction 1"],\n'
            f'    "recommendation": "accept"\n'
            f"  }}\n"
            f"}}\n\n"
            f"Output ONLY the JSON object. No other text."
        )

    def parse_batch_response(
        self, response: str, record_ids: List[str]
    ) -> Dict[str, CrossLayerCoherenceResult]:
        """Parse LLM response into CrossLayerCoherenceResult objects.

        Args:
            response: Raw LLM response text (may contain markdown
                code fences).
            record_ids: Expected record IDs for the batch.

        Returns:
            Dictionary mapping record_id to CrossLayerCoherenceResult.
            Missing or unparseable records get safe defaults.
        """
        parsed_data = self._try_parse_json(response)

        if parsed_data is None:
            logger.error(
                "Failed to parse coherence response JSON; "
                "returning defaults for %d records",
                len(record_ids),
            )
            return {
                rid: self._default_result() for rid in record_ids
            }

        results: Dict[str, CrossLayerCoherenceResult] = {}

        for i, rid in enumerate(record_ids):
            # Look up by batch index key (record_1, record_2, ...)
            key = f"record_{i + 1}"
            entry = parsed_data.get(key)
            # Fallback: try the record_id directly (old format)
            if entry is None:
                entry = parsed_data.get(rid)
            if entry is None:
                logger.warning(
                    "Missing coherence result for %s (key %s); "
                    "using default",
                    rid,
                    key,
                )
                results[rid] = self._default_result()
                continue

            results[rid] = self._entry_to_result(entry)

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_system_prompt(self) -> str:
        """Build the system prompt for cross-layer coherence evaluation."""
        return (
            "You are a cross-layer coherence evaluator for product "
            "lifecycle data. Your sole task is to detect contradictions "
            "BETWEEN layers and assess whether the full lifecycle "
            "(materials -> processing -> transport -> packaging) tells "
            "a plausible, internally consistent story.\n\n"
            "IMPORTANT RULES:\n"
            "- DO NOT validate individual fields in isolation. "
            "Per-field validation is handled elsewhere.\n"
            "- DO NOT use thinking tags or chain-of-thought markup.\n"
            "- Output ONLY the JSON object described in the user "
            "prompt. No preamble, no explanation, no markdown.\n"
            "- Focus exclusively on contradictions between layers and "
            "lifecycle plausibility.\n\n"
            "SCORING GUIDELINES:\n\n"
            "lifecycle_coherence_score (0.0-1.0):\n"
            "  Does the material -> processing -> transport -> packaging "
            "chain tell a plausible story? Consider whether the "
            "processing steps make sense for the declared materials, "
            "whether the transport distance and mode are realistic for "
            "the supply chain type and product origin, and whether the "
            "packaging is proportionate to the product.\n"
            "  1.0 = fully coherent lifecycle narrative\n"
            "  0.5 = questionable coherence, notable gaps\n"
            "  0.0 = completely incoherent lifecycle\n\n"
            "cross_layer_contradiction_score (0.0-1.0):\n"
            "  Measures the ABSENCE of contradictions between layers. "
            "Look for conflicts such as: heavy materials with minimal "
            "packaging, short-haul classification with intercontinental "
            "distances, processing steps impossible for the declared "
            "materials, packaging mass wildly disproportionate to "
            "product weight.\n"
            "  1.0 = no contradictions found\n"
            "  0.5 = moderate contradictions\n"
            "  0.0 = severe contradictions across layers\n\n"
            "overall_coherence_score (0.0-1.0):\n"
            "  Combined assessment of lifecycle plausibility and "
            "cross-layer consistency. Typically close to the average "
            "of the other two scores but may be adjusted if one issue "
            "is particularly severe.\n\n"
            "contradictions_found:\n"
            "  List specific contradictions as short, concrete strings. "
            "Empty list if none found.\n\n"
            "recommendation:\n"
            '  "accept" if overall_coherence_score >= 0.85 and no '
            "major contradictions.\n"
            '  "review" if overall_coherence_score >= 0.60 or minor '
            "contradictions exist.\n"
            '  "reject" if overall_coherence_score < 0.60 or severe '
            "contradictions exist."
        )

    def _format_record(
        self, index: int, record: CompleteProductRecord
    ) -> str:
        """Format a single record for inclusion in the batch prompt."""
        # Materials with percentages
        material_parts = []
        for mat, pct in zip(
            record.materials, record.material_percentages
        ):
            material_parts.append(f"{mat} ({pct:.0f}%)")
        materials_str = ", ".join(material_parts)

        # Processing steps
        processing_str = ", ".join(record.preprocessing_steps)

        # Packaging categories
        categories_str = ", ".join(record.packaging_categories)

        return (
            f"record_{index} ({record.subcategory_name}, "
            f"{record.subcategory_id}):\n"
            f"  Product: {record.category_name}, "
            f"{record.total_weight_kg:.3f}kg\n"
            f"  Materials: {materials_str}\n"
            f"  Processing: {processing_str}\n"
            f"  Transport: {record.total_transport_distance_km}km "
            f"({record.supply_chain_type})\n"
            f"  Packaging: {record.total_packaging_mass_kg:.3f}kg "
            f"({categories_str})"
        )

    def _try_parse_json(self, response: str) -> dict | None:
        """Attempt to parse JSON from a raw LLM response.

        Strips markdown code fences if present before parsing.
        Returns None on failure.
        """
        cleaned = response.strip()

        # Strip markdown code fences (```json ... ``` or ``` ... ```)
        if cleaned.startswith("```"):
            # Remove opening fence (with optional language tag)
            cleaned = re.sub(r"^```(?:json)?\s*\n?", "", cleaned)
            # Remove closing fence
            cleaned = re.sub(r"\n?```\s*$", "", cleaned)

        try:
            data = json.loads(cleaned)
            if isinstance(data, dict):
                return data
            logger.error(
                "Coherence response JSON is not a dict: %s",
                type(data).__name__,
            )
            return None
        except json.JSONDecodeError as exc:
            logger.error("JSON decode error in coherence response: %s", exc)
            return None

    def _entry_to_result(
        self, entry: dict
    ) -> CrossLayerCoherenceResult:
        """Convert a parsed JSON entry to a CrossLayerCoherenceResult."""
        lifecycle = self._clamp_score(
            entry.get("lifecycle_coherence_score", 0.7)
        )
        contradiction = self._clamp_score(
            entry.get("cross_layer_contradiction_score", 0.7)
        )
        overall = self._clamp_score(
            entry.get("overall_coherence_score", 0.7)
        )

        contradictions = entry.get("contradictions_found", [])
        if not isinstance(contradictions, list):
            contradictions = []

        recommendation = entry.get("recommendation", "review")
        if recommendation not in ("accept", "review", "reject"):
            recommendation = "review"

        return CrossLayerCoherenceResult(
            lifecycle_coherence_score=lifecycle,
            cross_layer_contradiction_score=contradiction,
            overall_coherence_score=overall,
            contradictions_found=contradictions,
            recommendation=recommendation,
        )

    @staticmethod
    def _default_result() -> CrossLayerCoherenceResult:
        """Return a safe default result for missing or failed records."""
        return CrossLayerCoherenceResult(
            lifecycle_coherence_score=0.7,
            cross_layer_contradiction_score=0.7,
            overall_coherence_score=0.7,
            contradictions_found=[],
            recommendation="review",
        )

    @staticmethod
    def _clamp_score(value) -> float:
        """Clamp a score value to [0.0, 1.0]."""
        try:
            v = float(value)
        except (TypeError, ValueError):
            return 0.7
        return max(0.0, min(1.0, v))
