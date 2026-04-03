"""
Semantic Validator for Layer 3: Stage 3 Validation

Uses an LLM to evaluate whether generated transport legs are
geographically and logistically plausible. Catches errors that pass
structural checks but are geographically wrong.

See LAYER3_DESIGN.md section 9.4.
"""

import json
import logging
import re
from typing import Any, Dict, List, Optional

from data.data_generation.layer_3.clients.api_client import Layer3Client
from data.data_generation.layer_3.config.config import Layer3Config
from data.data_generation.layer_3.models.models import (
    Layer3Record,
    SemanticValidationResult,
)

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "You are a global textile supply chain logistics analyst. "
    "Your job is to evaluate whether transport leg sequences for "
    "textile products are geographically and logistically plausible. "
    "Respond ONLY with a JSON object -- no explanation outside the JSON."
)


class SemanticValidator:
    """Evaluates transport leg plausibility via LLM.

    Scores three aspects (0.0-1.0): location, route, and mode
    plausibility. Recommendation derived from average score vs
    config thresholds.
    """

    def __init__(self, config: Layer3Config, api_client: Layer3Client):
        self.config = config
        self.api_client = api_client

    # -- public API --------------------------------------------------------

    def validate(self, record: Layer3Record) -> SemanticValidationResult:
        """Evaluate plausibility of a record's transport legs using LLM.

        On API failure, returns recommendation="review".
        """
        if not record.transport_legs:
            return SemanticValidationResult(
                location_plausibility_score=0.0,
                route_plausibility_score=0.0,
                mode_plausibility_score=0.0,
                issues_found=["Record has no transport legs"],
                recommendation="reject",
            )

        user_prompt = self._build_validation_prompt(record)
        max_retries = getattr(self.config, "semantic_max_retries", 2)
        last_error: Optional[str] = None

        for attempt in range(1, max_retries + 1):
            try:
                response = self.api_client.generate_transport_legs(
                    system_prompt=_SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                )
                result = self._parse_response(response)
                if result is not None:
                    result.recommendation = self._compute_recommendation(result)
                    logger.info(
                        "Semantic validation for %s: loc=%.2f route=%.2f "
                        "mode=%.2f -> %s",
                        record.preprocessing_path_id,
                        result.location_plausibility_score,
                        result.route_plausibility_score,
                        result.mode_plausibility_score,
                        result.recommendation,
                    )
                    return result
                last_error = "Unparseable LLM response"
                logger.warning(
                    "Semantic parse failed (attempt %d/%d) for %s",
                    attempt, max_retries, record.preprocessing_path_id,
                )
            except Exception as exc:
                last_error = str(exc)
                logger.warning(
                    "Semantic LLM call failed (attempt %d/%d): %s",
                    attempt, max_retries, exc,
                )

        logger.error(
            "Semantic validation failed after %d attempts for %s: %s",
            max_retries, record.preprocessing_path_id, last_error,
        )
        return _create_review_fallback(last_error or "Unknown error")

    def validate_batch(
        self, records: List[Layer3Record]
    ) -> List[SemanticValidationResult]:
        """Validate a batch of records sequentially."""
        results: List[SemanticValidationResult] = []
        for i, record in enumerate(records):
            logger.debug(
                "Semantic validation: record %d/%d (%s)",
                i + 1, len(records), record.preprocessing_path_id,
            )
            results.append(self.validate(record))
        return results

    # -- prompt construction -----------------------------------------------

    def _build_validation_prompt(self, record: Layer3Record) -> str:
        """Build the user prompt presenting transport legs to the LLM."""
        materials_str = ", ".join(record.materials)
        steps_str = ", ".join(record.preprocessing_steps)

        lines = [
            "Evaluate the plausibility of the following transport legs "
            "for a textile product.",
            "",
            "PRODUCT CONTEXT:",
            f"  Product: {record.category_name} > {record.subcategory_name}",
            f"  Materials: {materials_str}",
            f"  Processing steps: {steps_str}",
            f"  Total distance: {record.total_distance_km:.1f} km",
            "",
            f"TRANSPORT LEGS ({len(record.transport_legs)}):",
        ]

        for leg in record.transport_legs:
            modes_str = ", ".join(leg.transport_modes)
            lines.extend([
                f"  Leg {leg.leg_index}:",
                f"    Material: {leg.material}",
                f"    From: {leg.from_location} ({leg.from_step}) "
                f"[{leg.from_lat:.2f}, {leg.from_lon:.2f}]",
                f"    To: {leg.to_location} ({leg.to_step}) "
                f"[{leg.to_lat:.2f}, {leg.to_lon:.2f}]",
                f"    Distance: {leg.distance_km:.1f} km | Modes: {modes_str}",
                "",
            ])

        lines.extend([
            "EVALUATE THREE ASPECTS (score each 0.0 to 1.0):",
            "",
            "1. location_score: Are the assigned locations realistic "
            "for the processing steps and materials?",
            "2. route_score: Do the transport routes make geographic sense?",
            "3. mode_score: Are the transport modes appropriate for "
            "the distances and geography?",
            "",
            "Respond with ONLY this JSON (no other text):",
            '{',
            '  "location_score": 0.XX,',
            '  "route_score": 0.XX,',
            '  "mode_score": 0.XX,',
            '  "issues": ["specific issue 1", "specific issue 2"]',
            '}',
        ])
        return "\n".join(lines)

    # -- response parsing --------------------------------------------------

    def _parse_response(
        self, response: Any
    ) -> Optional[SemanticValidationResult]:
        """Parse the LLM response into a SemanticValidationResult.

        Handles dict, list-of-dicts, or raw string responses.
        """
        data: Optional[Dict[str, Any]] = None
        if isinstance(response, dict):
            data = response
        elif isinstance(response, list) and len(response) > 0:
            data = response[0] if isinstance(response[0], dict) else None
        elif isinstance(response, str):
            data = _extract_json_object(response)

        if data is None:
            return None

        try:
            loc = _clamp(float(data.get("location_score", 0.0)))
            route = _clamp(float(data.get("route_score", 0.0)))
            mode = _clamp(float(data.get("mode_score", 0.0)))
            issues = data.get("issues", [])
            if not isinstance(issues, list):
                issues = [str(issues)] if issues else []
            return SemanticValidationResult(
                location_plausibility_score=loc,
                route_plausibility_score=route,
                mode_plausibility_score=mode,
                issues_found=[str(i) for i in issues],
            )
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning("Failed to parse semantic scores: %s", exc)
            return None

    # -- recommendation logic ----------------------------------------------

    def _compute_recommendation(
        self, result: SemanticValidationResult
    ) -> str:
        """Derive accept/review/reject from the average score."""
        avg = (
            result.location_plausibility_score
            + result.route_plausibility_score
            + result.mode_plausibility_score
        ) / 3.0
        accept = getattr(self.config, "semantic_accept_threshold", 0.80)
        review = getattr(self.config, "semantic_review_threshold", 0.60)
        if avg >= accept:
            return "accept"
        elif avg >= review:
            return "review"
        return "reject"


# -- module-level helpers --------------------------------------------------


def _clamp(value: float) -> float:
    """Clamp a score to [0.0, 1.0]."""
    return max(0.0, min(1.0, value))


def _create_review_fallback(error_msg: str) -> SemanticValidationResult:
    """Return a safe fallback when validation cannot complete."""
    return SemanticValidationResult(
        location_plausibility_score=0.0,
        route_plausibility_score=0.0,
        mode_plausibility_score=0.0,
        issues_found=[
            f"Semantic validation could not complete: {error_msg}"
        ],
        recommendation="review",
    )


def _extract_json_object(text: str) -> Optional[Dict[str, Any]]:
    """Try to extract a JSON object from raw text."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    text = re.sub(r"<thinking>.*?</thinking>", "", text, flags=re.DOTALL)
    text = text.strip()

    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            return obj
    except json.JSONDecodeError:
        pass

    # Bracket-match the first { ... }
    start = text.find("{")
    if start != -1:
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start : i + 1])
                    except json.JSONDecodeError:
                        break
    return None
