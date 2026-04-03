"""
Prompt builder for Layer 4 Packaging Configuration Generator.

Assembles the static system prompt from text files and builds per-record
user prompts from Layer 3 input data. Intended to replace the legacy
prompts/prompts.py in the V2 pipeline.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from data.data_generation.layer_4.config.config import Layer4Config

logger = logging.getLogger(__name__)


class PromptBuilder:
    """Builds system and user prompts for V2 packaging configuration generation.

    The system prompt is loaded once from text files in prompts/system/ and
    cached for reuse across all records. The user prompt is built per-record
    using product and transport journey data from the Layer 3 Parquet.
    """

    def __init__(self, config: Layer4Config):
        self.config = config
        self._system_prompt: Optional[str] = None
        self._system_prompts_dir = (
            Path(__file__).resolve().parent / "system"
        )

    # -- System prompt (static, cached) ------------------------------------

    def get_system_prompt(self) -> str:
        """Load and cache the concatenated system prompt from text files.

        Reads all .txt files in prompts/system/ sorted by filename,
        concatenates them with double newlines, and caches the result.
        Subsequent calls return the cached string.

        Raises FileNotFoundError if the directory is missing or empty.
        """
        if self._system_prompt is not None:
            return self._system_prompt

        prompts_dir = self._system_prompts_dir
        if not prompts_dir.is_dir():
            raise FileNotFoundError(
                "System prompts directory not found: %s" % prompts_dir
            )

        txt_files = sorted(prompts_dir.glob("*.txt"))
        if not txt_files:
            raise FileNotFoundError(
                "No .txt files found in system prompts directory: %s"
                % prompts_dir
            )

        parts: List[str] = []
        for txt_file in txt_files:
            content = txt_file.read_text(encoding="utf-8").strip()
            if content:
                parts.append(content)
            logger.debug("Loaded system prompt file: %s", txt_file.name)

        if not parts:
            raise FileNotFoundError(
                "All .txt files in system prompts directory are empty: %s"
                % prompts_dir
            )

        self._system_prompt = "\n\n".join(parts)
        logger.info(
            "System prompt assembled from %d files (%d characters)",
            len(parts),
            len(self._system_prompt),
        )
        return self._system_prompt

    # -- User prompt (per-record) ------------------------------------------

    def build_user_prompt(self, record: Dict[str, Any]) -> str:
        """Build a per-record user prompt from a Layer 3 record dict.

        Includes product context (subcategory, category, weight, materials)
        and a transport journey summary derived from transport_legs.
        """
        lines: List[str] = []

        # Product context
        subcategory_name = record.get("subcategory_name")
        category_name = record.get("category_name", "Unknown")
        # Handle NaN/None subcategory (some categories have no subcategories)
        if subcategory_name is None or (
            isinstance(subcategory_name, float)
            and subcategory_name != subcategory_name
        ) or str(subcategory_name).lower() == "nan":
            product_label = category_name
        else:
            product_label = "%s (%s)" % (subcategory_name, category_name)
        total_weight_kg = record.get("total_weight_kg", 0.0)
        materials_raw = record.get("materials", [])
        materials = self._parse_json_field(materials_raw)
        if not isinstance(materials, list):
            materials = []
        materials_str = ", ".join(str(m) for m in materials) if materials else "Unknown"

        lines.append(
            "Predict the packaging for this textile product:"
        )
        lines.append("")
        lines.append("Product: %s" % product_label)
        lines.append("Product weight: %.4g kg" % total_weight_kg)
        lines.append("Materials: %s" % materials_str)

        # Transport journey summary
        summary = self._extract_transport_summary(record)

        lines.append("")
        lines.append(
            "Transport journey (%.4g km total, %d legs):"
            % (summary["total_distance_km"], summary["n_legs"])
        )
        lines.append("- Modes used: %s" % summary["unique_modes"])
        lines.append("- Origin: %s" % summary["origin"])
        lines.append("- Destination: %s" % summary["destination"])

        if summary["leg_lines"]:
            lines.append("- Legs:")
            for leg_line in summary["leg_lines"]:
                lines.append("  %s" % leg_line)

        return "\n".join(lines)

    # -- Batch user prompt -------------------------------------------------

    def build_batch_user_prompt(self, records: List[Dict[str, Any]]) -> str:
        """Build a multi-product user prompt for batch API calls.

        Numbers each product 1-N with delimiters and reuses
        build_user_prompt() for each product block.
        """
        n = len(records)
        parts: List[str] = []
        parts.append(
            "Estimate packaging for the following %d products. "
            "Return a JSON array with one result per product, in order." % n
        )

        for i, record in enumerate(records, start=1):
            parts.append("")
            parts.append("--- Product %d ---" % i)
            parts.append(self.build_user_prompt(record))

        return "\n".join(parts)

    # -- Correction prompt (two-pass regeneration) -------------------------

    def build_correction_prompt(
        self, record: Dict[str, Any], failures: List[str]
    ) -> str:
        """Build a user prompt with correction feedback for two-pass flow.

        Identical to build_user_prompt but appends a CORRECTIONS block
        listing validation failures so the model can address them.
        """
        base = self.build_user_prompt(record)

        correction_lines: List[str] = []
        correction_lines.append("")
        correction_lines.append("CORRECTIONS REQUIRED")
        correction_lines.append(
            "Your previous response failed validation. "
            "Fix the following issues and regenerate the full output:"
        )
        for i, failure in enumerate(failures, start=1):
            correction_lines.append("%d. %s" % (i, failure))
        correction_lines.append("")
        correction_lines.append(
            "Ensure the corrected output passes all validation checks. "
            "Respond with ONLY the corrected JSON object."
        )

        return base + "\n" + "\n".join(correction_lines)

    # -- Internal helpers --------------------------------------------------

    def _parse_json_field(self, value: Any) -> Any:
        """Parse a field that may be a JSON-encoded string or already parsed.

        Returns lists and dicts as-is. Attempts json.loads on strings.
        Returns an empty list on any parse failure.
        """
        if isinstance(value, (list, dict)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, ValueError):
                return []
        return []

    def _extract_transport_summary(
        self, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Extract transport journey details from a Layer 3 record.

        Parses the transport_legs JSON field and derives:
        - total_distance_km
        - unique_modes (sorted, deduped)
        - origin (first leg from_location)
        - destination (last leg to_location)
        - n_legs
        - leg_lines (list of formatted leg summary strings)

        Returns safe "Unknown" defaults when transport_legs is absent or
        malformed.
        """
        total_distance_km = float(record.get("total_distance_km", 0.0))
        legs_raw = record.get("transport_legs", [])
        legs = self._parse_json_field(legs_raw)

        if not isinstance(legs, list) or not legs:
            return {
                "total_distance_km": total_distance_km,
                "unique_modes": "Unknown",
                "origin": "Unknown",
                "destination": "Unknown",
                "n_legs": 0,
                "leg_lines": [],
            }

        # Collect modes across all legs
        all_modes: List[str] = []
        for leg in legs:
            modes = leg.get("transport_modes", [])
            if isinstance(modes, list):
                all_modes.extend(str(m) for m in modes)

        unique_modes_list = sorted(set(all_modes))
        unique_modes_str = (
            ", ".join(unique_modes_list) if unique_modes_list else "Unknown"
        )

        origin = legs[0].get("from_location", "Unknown")
        destination = legs[-1].get("to_location", "Unknown")
        n_legs = len(legs)

        # Build one summary line per leg
        leg_lines: List[str] = []
        for i, leg in enumerate(legs, start=1):
            from_loc = leg.get("from_location", "Unknown")
            to_loc = leg.get("to_location", "Unknown")
            dist = leg.get("distance_km", 0.0)
            modes = leg.get("transport_modes", [])
            modes_str = (
                ", ".join(str(m) for m in modes) if modes else "Unknown"
            )
            leg_lines.append(
                "%d. %s -> %s (%.4g km, %s)"
                % (i, from_loc, to_loc, dist, modes_str)
            )

        return {
            "total_distance_km": total_distance_km,
            "unique_modes": unique_modes_str,
            "origin": origin,
            "destination": destination,
            "n_legs": n_legs,
            "leg_lines": leg_lines,
        }
