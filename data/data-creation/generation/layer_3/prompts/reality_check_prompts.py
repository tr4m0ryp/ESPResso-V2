"""
Reality check prompts for Layer 3: Supply Chain Plausibility (V2).

Validates that generated transport legs have plausible locations,
routes, distances, and transport mode selections.

NOTE: The SemanticValidator in core/semantic_validator.py handles its
own prompt construction for per-record validation. This module provides
the batch-oriented prompts used by the shared RealityChecker, updated
for the V2 per-leg transport format.

V1 format (deprecated): flat records with origin_region, supply_chain_type,
    total_transport_distance_km, and transport_modes fields.
V2 format (current): records with transport_legs containing per-leg
    from/to locations, coordinates, distances, and transport modes.
"""

SYSTEM_PROMPT = (
    "You are a global supply chain logistics analyst. Your job is to "
    "evaluate whether transport leg sequences for textile products are "
    "geographically and logistically plausible. Respond ONLY with JSON."
)

VALIDATION_PROMPT_TEMPLATE = """Evaluate whether each transport record below has REALISTIC transport legs.

A record's transport legs are realistic when:
- Processing step locations are plausible for the materials (e.g., silk spinning in a known silk region)
- Transport routes make geographic sense (no impossible paths)
- Transport modes match the distance and geography (road for short overland, sea for ocean crossings)
- Distances are consistent with the origin and destination coordinates
- The overall supply chain routing makes economic and logistic sense

For each record, return a JSON object with:
- "index": the record number (starting at 0)
- "realistic": true or false
- "reason": one sentence explaining your judgment
- "improvement": if false, how to fix it; if true, empty string

Records to evaluate:
{records_formatted}

Respond with ONLY this JSON (no explanation outside the JSON):
{{"results": [
  {{"index": 0, "realistic": true, "reason": "...", "improvement": ""}},
  ...
]}}"""


def format_record(record) -> str:
    """Serialize one V2 Layer3Record to readable text for validation.

    Presents the product context and each transport leg with its
    locations, coordinates, distance, and transport modes.
    """
    materials_str = ", ".join(record.materials)
    steps_str = ", ".join(record.preprocessing_steps)

    lines = [
        f"Product: {record.category_name} > {record.subcategory_name}",
        f"  Materials: {materials_str}",
        f"  Processing steps: {steps_str}",
        f"  Total distance: {record.total_distance_km:.1f} km",
        f"  Number of legs: {len(record.transport_legs)}",
    ]

    for leg in record.transport_legs:
        modes_str = ", ".join(leg.transport_modes)
        lines.append(
            f"  Leg {leg.leg_index}: {leg.material} | "
            f"{leg.from_location} ({leg.from_step}) "
            f"[{leg.from_lat:.2f}, {leg.from_lon:.2f}] -> "
            f"{leg.to_location} ({leg.to_step}) "
            f"[{leg.to_lat:.2f}, {leg.to_lon:.2f}] | "
            f"{leg.distance_km:.1f} km | {modes_str}"
        )

    return "\n".join(lines)


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
    return VALIDATION_PROMPT_TEMPLATE.format(
        records_formatted=records_text
    )
