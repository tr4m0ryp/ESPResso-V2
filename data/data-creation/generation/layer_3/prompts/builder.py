"""
Prompt builder for Layer 3 V2 Transport Scenario Generator.

Assembles the static system prompt from text files and builds per-record
user prompts from Layer 2 input data. Replaces the V1 prompts/prompts.py.
"""

import logging
from pathlib import Path
from typing import List, Optional

from data.data_generation.layer_3.config.config import Layer3Config
from data.data_generation.layer_3.io.layer2_reader import Layer2Record

logger = logging.getLogger(__name__)


class PromptBuilder:
    """Builds system and user prompts for V2 transport scenario generation.

    The system prompt is loaded once from text files and cached for reuse
    across all records. The user prompt is built per-record with product-
    specific data from Layer 2.
    """

    def __init__(self, config: Layer3Config):
        self.config = config
        self._system_prompt: Optional[str] = None

    # -- System prompt (static, cached) ------------------------------------

    def get_system_prompt(self) -> str:
        """Load and cache the concatenated system prompt from text files.

        Reads all .txt files in prompts/system/ sorted by filename,
        concatenates them with double newlines, and caches the result.
        Subsequent calls return the cached string.
        """
        if self._system_prompt is not None:
            return self._system_prompt

        prompts_dir: Path = self.config.system_prompts_dir
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

        self._system_prompt = "\n\n".join(parts)
        logger.info(
            "System prompt assembled from %d files (%d characters)",
            len(parts),
            len(self._system_prompt),
        )
        return self._system_prompt

    # -- User prompt (per-record) ------------------------------------------

    def build_user_prompt(
        self,
        record: Layer2Record,
        seed: int = 0,
        warehouse: str = "EU",
    ) -> str:
        """Build a per-record user prompt with product-specific data.

        Contains product details, material composition, the step-material
        mapping from Layer 2, the target warehouse, and a seed number for
        location variety.
        """
        sections: List[str] = []

        # Product details
        sections.append("PRODUCT")
        sections.append("Category: %s" % record.category_name)
        sections.append("Subcategory: %s" % record.subcategory_name)
        sections.append("Total weight: %.3f kg" % record.total_weight_kg)

        # Materials with weights and percentages
        sections.append("")
        sections.append("MATERIALS")
        for i, material in enumerate(record.materials):
            weight = record.material_weights_kg[i]
            percentage = record.material_percentages[i]
            sections.append(
                "- %s: %.4f kg (%d%%)" % (material, weight, percentage)
            )

        # Step-material mapping
        sections.append("")
        sections.append("PROCESSING STEPS PER MATERIAL")
        for material, steps in record.step_material_mapping.items():
            step_list = ", ".join(steps) if steps else "(none)"
            sections.append("- %s: [%s]" % (material, step_list))

        # Warehouse and seed
        sections.append("")
        sections.append("TARGET WAREHOUSE: %s" % warehouse)
        sections.append("SEED: %d" % seed)

        # Explicit material checklist
        sections.append("")
        sections.append(
            "REQUIRED MATERIALS (%d total -- you MUST generate legs "
            "for ALL of them):" % len(record.materials)
        )
        for i, material in enumerate(record.materials, 1):
            sections.append("  %d. \"%s\"" % (i, material))

        sections.append("")
        sections.append("CHECKLIST (verify before responding):")
        sections.append(
            "- ALL %d materials above appear in your output legs"
            % len(record.materials)
        )
        sections.append(
            "- Material names are EXACT copies (including commas and spaces)"
        )
        sections.append(
            "- All chains converge at the same assembly location"
        )
        sections.append(
            "- All chains end at the same warehouse destination"
        )
        sections.append("- All distance_km values are >= 1.0")

        return "\n".join(sections)

    # -- Correction prompt (two-pass regeneration) -------------------------

    def build_correction_prompt(
        self,
        record: Layer2Record,
        failures: List[str],
        seed: int = 0,
        warehouse: str = "EU",
    ) -> str:
        """Build a user prompt with correction feedback for two-pass flow.

        Identical to build_user_prompt but appends a CORRECTIONS block
        listing the specific validation failures and asking the LLM to
        fix them in the regenerated output.
        """
        base_prompt = self.build_user_prompt(
            record, seed=seed, warehouse=warehouse
        )

        correction_lines: List[str] = []
        correction_lines.append("")
        correction_lines.append("CORRECTIONS REQUIRED")
        correction_lines.append(
            "Your previous response failed validation. "
            "Fix the following issues and regenerate the full "
            "transport_legs JSON array:"
        )
        for i, failure in enumerate(failures, start=1):
            correction_lines.append("%d. %s" % (i, failure))
        correction_lines.append("")
        correction_lines.append(
            "Ensure the corrected output passes all validation checks. "
            "Respond with ONLY the corrected JSON array."
        )

        return base_prompt + "\n".join(correction_lines)
