"""
Reality check prompts for Layer 2: Processing Path Feasibility.

Validates that generated preprocessing step sequences are realistic
manufacturing workflows for the given materials.

Prompts enforce strict JSON-only output with no preamble, code fences,
thinking tags, or surrounding text.
"""

import json

SYSTEM_PROMPT = (
    "You are a textile manufacturing process engineer. Your SOLE job is "
    "to evaluate whether preprocessing step sequences are feasible "
    "manufacturing workflows.\n\n"
    "CRITICAL OUTPUT RULES:\n"
    "- You MUST respond with ONLY valid JSON.\n"
    "- Do NOT include any text, explanation, or commentary outside the JSON.\n"
    "- Do NOT wrap the JSON in markdown code fences (no ``` or ```json).\n"
    "- Do NOT use thinking tags (<think>, <thinking>, etc.).\n"
    "- Do NOT include any preamble such as 'Here is the result' or "
    "'Sure, here you go'.\n"
    "- Your entire response must be parseable by json.loads() with no "
    "preprocessing.\n"
    "- Start your response with { and end it with }."
)

VALIDATION_PROMPT_TEMPLATE = """Evaluate whether each preprocessing path below is a REALISTIC manufacturing workflow.

A path is realistic when:
- Processing steps are in a feasible manufacturing order
- Each step is applicable to the listed materials
- The sequence represents a real-world production pipeline
- No impossible step orderings (e.g., dyeing before spinning)

Records to evaluate:
{records_formatted}

You MUST respond with ONLY the following JSON structure and nothing else.
Do NOT include any text before or after the JSON.
Do NOT use markdown code fences.
Do NOT use thinking tags or any preamble.
Your response must start with {{ and end with }}.

Required JSON schema:
{{
  "results": [
    {{
      "index": <integer, the record number starting at 0>,
      "realistic": <boolean, true or false>,
      "reason": <string, one sentence explaining your judgment>,
      "improvement": <string, how to fix if false; empty string if true>
    }}
  ]
}}

You MUST include an entry for EVERY record listed above. Do not skip any.

Example of a valid response for 2 records:
{{"results": [{{"index": 0, "realistic": true, "reason": "Scouring then carding then spinning is the standard wool workflow.", "improvement": ""}}, {{"index": 1, "realistic": false, "reason": "Dyeing cannot precede spinning for staple fibers.", "improvement": "Move dyeing step to after spinning or use fiber-dye method."}}]}}"""


def format_record(record) -> str:
    """Serialize one Layer2Record to readable text."""
    materials_str = ", ".join(record.materials)
    steps_str = " -> ".join(record.preprocessing_steps)

    mapping_str = ""
    if record.step_material_mapping:
        mapping = record.step_material_mapping
        if isinstance(mapping, str):
            try:
                mapping = json.loads(mapping)
            except (json.JSONDecodeError, TypeError):
                mapping = {}
        parts = []
        for mat, steps in mapping.items():
            parts.append(f"  {mat}: {', '.join(steps)}")
        mapping_str = "\n".join(parts)

    text = (
        f"Product: {record.category_name} > {record.subcategory_name}\n"
        f"  Materials: {materials_str}\n"
        f"  Steps: {steps_str}"
    )
    if mapping_str:
        text += f"\n  Material-step mapping:\n{mapping_str}"
    return text


def format_batch(records) -> str:
    """Format N records for the validation prompt."""
    lines = []
    for i, record in enumerate(records):
        lines.append(f"[Record {i}]")
        lines.append(format_record(record))
        lines.append("")
    return "\n".join(lines)


def get_validation_prompt(records_text: str) -> str:
    """Build the full validation prompt with records embedded."""
    return VALIDATION_PROMPT_TEMPLATE.format(records_formatted=records_text)
