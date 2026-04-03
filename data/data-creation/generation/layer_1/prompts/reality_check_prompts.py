"""
Reality check prompts for Layer 1: Material-Product Coherence.

Validates that generated material compositions are realistic for the
given product category. Checks material appropriateness, blend ratios,
and weight plausibility.

Prompts enforce strict JSON-only output with no preamble, code fences,
thinking tags, or surrounding text.
"""

SYSTEM_PROMPT = (
    "You are a textile industry quality auditor. Your SOLE job is to "
    "evaluate whether material compositions for fashion products are "
    "realistic and commercially viable.\n\n"
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

VALIDATION_PROMPT_TEMPLATE = """Evaluate whether each product composition below is REALISTIC.

A composition is realistic when:
- Materials are appropriate for the product category
- Blend ratios follow industry norms (e.g., cashmere as minor component, not 80%+)
- Total weight is plausible for the product type
- The product could actually be manufactured and sold at retail

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
{{"results": [{{"index": 0, "realistic": true, "reason": "Cotton-polyester blend at 60/40 is standard for casual shirts.", "improvement": ""}}, {{"index": 1, "realistic": false, "reason": "100% cashmere at 2.5 kg is implausible for a t-shirt.", "improvement": "Reduce cashmere percentage to 5-15% and blend with wool or cotton."}}]}}"""


def format_record(record) -> str:
    """Serialize one ProductComposition to readable text."""
    materials_str = ", ".join(
        f"{m} ({p}%, {w:.3f} kg)"
        for m, p, w in zip(
            record.materials,
            record.material_percentages,
            record.material_weights_kg,
        )
    )
    return (
        f"Category: {record.category_name} > {record.subcategory_name}\n"
        f"  Materials: {materials_str}\n"
        f"  Total weight: {record.total_weight_kg:.3f} kg"
    )


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
