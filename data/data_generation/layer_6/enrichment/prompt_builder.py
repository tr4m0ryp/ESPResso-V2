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
SYSTEM_PROMPT = """\
You are a transport logistics data extraction engine.

TASK
For each record you receive:
- total_distance_km: the authoritative total distance
- pre_computed_km: distances already assigned to single-mode legs (exact)
- multi_mode_legs: ONLY the legs with multiple transport modes

Your job: extract per-mode distances from the multi_mode_legs, then ADD \
them to the pre_computed values for the final output.

EXTRACTION RULES

Step 1: For each multi-mode leg, read the reasoning to find approximate \
per-segment distances.

Step 2: SCALE segments to match the leg's distance_km:
  - Sum raw segment distances from reasoning.
  - scale_factor = distance_km / sum_of_raw_segments.
  - Multiply each segment by scale_factor.

Step 3: Sum scaled distances per mode across all multi-mode legs.

Step 4: ADD multi-mode totals to pre_computed_km values.

Step 5: VERIFY your final totals sum to total_distance_km. If off by \
more than 0.5%, re-check and correct.

WORKED EXAMPLE
Input: total_distance_km: 5050.0
pre_computed_km: {"road_km": 100.0, "sea_km": 0.0, ...}
multi_mode_legs: [{"transport_modes": ["road","sea","road"], \
"distance_km": 4950.0, "reasoning": "Trucked 430 km. Shipped 4200 km. \
Final 340 km by road."}]

Raw: road=430, sea=4200, road=340. Sum=4970.
Scale: 4950/4970=0.99598. Scaled: road=766.9, sea=4183.1.
Add pre_computed: road=100+766.9=866.9, sea=0+4183.1=4183.1.
Verify: 866.9+4183.1=5050.0. OK.

OUTPUT FORMAT
JSON array, one object per record, in order:
{"id":"<id>","road_km":<float>,"sea_km":<float>,"rail_km":<float>,\
"air_km":<float>,"inland_waterway_km":<float>}

RULES
- distance_km is AUTHORITATIVE. Reasoning distances are approximate.
- Always scale so segments sum to the leg's distance_km.
- If reasoning lacks per-segment distances, split equally among modes.
- If no multi_mode_legs, output pre_computed_km values directly.
- Output ONLY the JSON array. No explanation, no fences, no preamble.\
"""


def get_system_prompt() -> str:
    """Return the system prompt for transport distance extraction."""
    return SYSTEM_PROMPT


def strip_leg_fields(leg: Dict[str, Any]) -> Dict[str, Any]:
    """Return a copy of leg containing only transport_modes, distance_km, reasoning.

    Drops coordinates, locations, from_step, to_step, leg_index and any other
    fields to reduce token usage in LLM calls.
    """
    return {k: v for k, v in leg.items() if k in _KEEP_LEG_FIELDS}


def _split_legs(legs: List[Dict[str, Any]]):
    """Split legs into single-mode (pre-computed) and multi-mode (needs LLM).

    Returns (pre_computed dict of mode->km, multi_mode_legs list,
             multi_mode_remaining_km).
    """
    pre = {"road_km": 0.0, "sea_km": 0.0, "rail_km": 0.0,
           "air_km": 0.0, "inland_waterway_km": 0.0}
    mode_map = {"road": "road_km", "sea": "sea_km", "rail": "rail_km",
                "air": "air_km", "inland_waterway": "inland_waterway_km"}
    multi = []
    pre_total = 0.0

    for leg in legs:
        modes = leg.get("transport_modes", [])
        km = float(leg.get("distance_km", 0.0))
        if len(modes) == 1 and modes[0] in mode_map:
            pre[mode_map[modes[0]]] += round(km, 1)
            pre_total += km
        else:
            multi.append(leg)

    return pre, multi, pre_total


def build_batch_prompt(records: List[Dict[str, Any]]) -> str:
    """Build a token-optimized batch prompt.

    Single-mode legs are pre-computed locally. Only multi-mode legs
    are sent to the LLM with their reasoning text.
    """
    n = len(records)
    lines: List[str] = []
    lines.append(
        "Extract transport mode distances for the following %d records." % n
    )
    lines.append(
        "NOTE: Single-mode legs have been pre-computed. "
        "pre_computed_km shows those totals. Only multi-mode legs "
        "are listed below. Add your multi-mode extraction to the "
        "pre-computed values for the final output."
    )

    for i, record in enumerate(records, start=1):
        record_id = record.get("record_id", "unknown")
        total_km = record.get("total_distance_km", 0.0)
        legs_raw = record.get("transport_legs", [])
        legs = _parse_legs(legs_raw, record_id)

        pre, multi, pre_total = _split_legs(legs)
        multi_stripped = [strip_leg_fields(l) for l in multi]
        remaining_km = float(total_km) - pre_total

        lines.append("")
        lines.append("--- Record %d (id: %s) ---" % (i, record_id))
        lines.append("total_distance_km: %s" % _format_km(total_km))
        lines.append("pre_computed_km: %s" % json.dumps(
            {k: round(v, 1) for k, v in pre.items()}, ensure_ascii=False
        ))
        if multi_stripped:
            lines.append("remaining_km_for_multi_mode: %s" % _format_km(
                remaining_km))
            lines.append("multi_mode_legs:")
            lines.append(_format_legs_json(multi_stripped))
        else:
            lines.append("(all legs are single-mode, no LLM extraction needed)")

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
