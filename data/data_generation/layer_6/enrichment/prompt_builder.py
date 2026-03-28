"""
Prompt builder for Layer 6 transport distance extraction.

Provides the system prompt and batch user prompt builder for LLM calls
that extract per-mode distance totals from transport leg reasoning text.
"""

import json
import logging
from typing import Any, Dict, List, Union

logger = logging.getLogger(__name__)

# Fields retained per leg when stripping for token efficiency
_KEEP_LEG_FIELDS = {"transport_modes", "distance_km", "reasoning"}

# System prompt stored as a module-level constant
SYSTEM_PROMPT = (
    "You are a transport logistics data extraction engine. Your task is to "
    "read textile supply chain transport leg data and extract the total "
    "distance traveled by each transport mode.\n\n"
    "TASK\n"
    "For each record, you receive a JSON array of transport legs. Each leg has:\n"
    "- transport_modes: ordered list of modes used "
    '(e.g., ["road", "sea", "road"])\n'
    "- distance_km: total distance for that leg\n"
    "- reasoning: narrative describing the journey with per-segment distances\n\n"
    "EXTRACTION RULES\n"
    "1. For SINGLE-MODE legs (transport_modes has one entry): assign the full "
    "distance_km to that mode.\n"
    "2. For MULTI-MODE legs (transport_modes has multiple entries): read the "
    "reasoning field and extract the distance for each segment. The reasoning "
    "always describes each segment with its distance (e.g., \"Trucked 430 km "
    'to port. Shipped 2180 km. Final 340 km by road.").\n'
    "3. Sum all distances per mode across ALL legs in the record.\n"
    "4. The five valid modes are: road, sea, rail, air, inland_waterway. "
    "Return 0.0 for any mode not used.\n"
    "5. Round all distances to 1 decimal place.\n\n"
    "OUTPUT FORMAT\n"
    "Return a JSON array with one object per record, in the order received. "
    "Each object:\n"
    "{\n"
    '  "id": "<the record id provided>",\n'
    '  "road_km": <float>,\n'
    '  "sea_km": <float>,\n'
    '  "rail_km": <float>,\n'
    '  "air_km": <float>,\n'
    '  "inland_waterway_km": <float>\n'
    "}\n\n"
    "CRITICAL RULES\n"
    "- Extract distances ONLY from the reasoning text. Do not estimate or infer.\n"
    "- If the reasoning does not specify per-segment distances for a multi-mode "
    "leg, divide the leg distance proportionally by the number of modes "
    "(fallback only).\n"
    "- Output ONLY the JSON array. No explanation, no markdown fences, "
    "no preamble."
)


def get_system_prompt() -> str:
    """Return the system prompt for transport distance extraction."""
    return SYSTEM_PROMPT


def strip_leg_fields(leg: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of leg containing only transport_modes, distance_km, reasoning.

    Drops coordinates, locations, from_step, to_step, leg_index and any other
    fields to reduce token usage in LLM calls.
    """
    return {k: v for k, v in leg.items() if k in _KEEP_LEG_FIELDS}


def build_batch_prompt(records: List[Dict[str, Any]]) -> str:
    """Build a multi-record user prompt for batch transport distance extraction.

    Args:
        records: List of record dicts, each containing at minimum:
            - record_id (str): unique identifier for the record
            - total_distance_km (float): total journey distance
            - transport_legs (str or list): JSON string or parsed list of legs

    Returns:
        Formatted prompt string ready to send to the LLM.
    """
    n = len(records)
    lines: List[str] = []
    lines.append(
        "Extract transport mode distances for the following %d records." % n
    )

    for i, record in enumerate(records, start=1):
        record_id = record.get("record_id", "unknown")
        total_km = record.get("total_distance_km", 0.0)
        legs_raw = record.get("transport_legs", [])
        legs = _parse_legs(legs_raw, record_id)
        stripped_legs = [strip_leg_fields(leg) for leg in legs]

        lines.append("")
        lines.append("--- Record %d (id: %s) ---" % (i, record_id))
        lines.append("total_distance_km: %s" % _format_km(total_km))
        lines.append("transport_legs:")
        lines.append(_format_legs_json(stripped_legs))

    return "\n".join(lines)


# -- Internal helpers ----------------------------------------------------------


def _parse_legs(
    legs_raw: Union[str, list, None], record_id: str
) -> List[Dict[str, Any]]:
    """Parse transport_legs field which may be a JSON string or a list.

    Returns an empty list if the value is absent, malformed, or not a list.
    """
    if legs_raw is None:
        logger.warning("record %s: transport_legs is None", record_id)
        return []

    if isinstance(legs_raw, list):
        return legs_raw

    if isinstance(legs_raw, str):
        try:
            parsed = json.loads(legs_raw)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "record %s: failed to parse transport_legs JSON: %s",
                record_id,
                exc,
            )
            return []
        if isinstance(parsed, list):
            return parsed
        logger.warning(
            "record %s: transport_legs JSON is not a list: %s",
            record_id,
            type(parsed).__name__,
        )
        return []

    logger.warning(
        "record %s: unexpected transport_legs type: %s",
        record_id,
        type(legs_raw).__name__,
    )
    return []


def _format_km(value: Any) -> str:
    """Format a distance value as a plain number string."""
    try:
        return "%.1f" % float(value)
    except (TypeError, ValueError):
        return "0.0"


def _format_legs_json(legs: List[Dict[str, Any]]) -> str:
    """Serialize a stripped list of legs to a compact JSON string.

    Uses 2-space indentation so reasoning text is readable in the prompt.
    """
    return json.dumps(legs, indent=2, ensure_ascii=False)
