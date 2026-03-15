Create the system prompt for the Layer 4 packaging generator.

Create the `prompts/system/` directory with a single text file containing the complete
system prompt. Unlike Layer 3 (which uses 8 numbered files), Layer 4's prompt is short
enough for a single file.

## File to create

`prompts/system/00_packaging_system.txt`

## System prompt structure

The prompt has 5 sections, separated by `===` headers. Total length: approximately
80-100 lines.

### Section 1: Role (~3 lines)

Set Claude Sonnet's persona as a packaging logistics expert for the textile and apparel
industry. State the task: predict realistic packaging mass per emission factor category
based on product characteristics and transport journey.

### Section 2: Packaging Categories (~20 lines)

Define exactly three categories with their emission factors and example materials:

1. **Paper/Cardboard** (EF: 1.3 kgCO2e/kg)
   Example materials: cardboard boxes, corrugated cardboard, tissue paper, kraft paper,
   paper wrapping, paper tags, labels, cardboard inserts, paper stuffing.

2. **Plastic** (EF: 3.5 kgCO2e/kg)
   Example materials: polybags (PE/PP), shrink wrap, garment covers, plastic hangers,
   plastic clips, bubble wrap, plastic tags, zip-lock bags.

3. **Other** (EF: 2.5 kgCO2e/kg)
   Example materials: composite materials, mixed-material packaging, silica gel packets,
   foam inserts, rubber bands, metal clips, glass-based materials, anything not clearly
   Paper/Cardboard or Plastic.

Note: glass is intentionally included under Other, not as a separate category.

### Section 3: Transport-driven reasoning (~30 lines)

This is the core of the prompt. Define how transport modes affect packaging needs:

**Per-mode implications:**
- Sea freight: humidity, salt air, weeks of exposure. Needs moisture barriers (plastic
  polybags) and sturdy outer protection (corrugated cardboard). Higher mass for both
  Paper/Cardboard and Plastic.
- Air freight: weight is expensive. Minimize total mass. Favor lightweight plastic
  (polybags) over heavy cardboard.
- Road transport: vibration and handling impacts. Standard cardboard protection. Balanced
  Paper/Cardboard and Plastic.
- Rail transport: similar to road, longer distances, less handling. Standard protection.
- Inland waterway: humidity like sea but shorter duration. Moderate moisture protection.

**Multi-modal combinations (the common case):**
The transport journey is provided as a list of legs, each with its own transport modes.
The packaging must protect the product across the ENTIRE journey. The most demanding leg
dictates the protection level. For example:
- Journey with a sea leg: treat as sea-level protection even if most legs are road.
- Journey with only road/rail legs: standard protection sufficient.
- Journey with an air leg: optimize for weight.

**Distance-based scaling:**
- Short journeys (<500 km total): minimal packaging. 1-3% of product weight.
- Medium journeys (500-2000 km): standard packaging. 3-6% of product weight.
- Long journeys (>2000 km): enhanced packaging. 5-10% of product weight.

### Section 4: Product-specific packaging norms (~25 lines)

Mass ranges per product type per category. These are reference ranges, not hard limits:

- Lightweight foldable items (t-shirts, shirts, underwear, 0.1-0.3 kg):
  Paper/Cardboard: 0.005-0.015 kg, Plastic: 0.003-0.010 kg, Other: 0.000-0.002 kg.

- Medium-weight items (jeans, trousers, dresses, 0.3-0.8 kg):
  Paper/Cardboard: 0.010-0.030 kg, Plastic: 0.005-0.015 kg, Other: 0.000-0.003 kg.

- Heavy/structured items (coats, jackets, suits, 0.8-2.0 kg):
  Paper/Cardboard: 0.020-0.060 kg, Plastic: 0.010-0.030 kg, Other: 0.000-0.005 kg.

- Footwear (shoes, boots, 0.5-2.0 kg):
  Paper/Cardboard: 0.050-0.120 kg, Plastic: 0.005-0.015 kg, Other: 0.002-0.008 kg.

- Accessories (scarves, belts, hats, 0.05-0.3 kg):
  Paper/Cardboard: 0.005-0.020 kg, Plastic: 0.002-0.008 kg, Other: 0.000-0.002 kg.

### Section 5: Output format (~15 lines)

Instruct the model to respond with ONLY a JSON object, no markdown fences, no text
outside the JSON:

```
{
  "paper_cardboard_kg": <float>,
  "plastic_kg": <float>,
  "other_kg": <float>,
  "reasoning": "<1-3 sentences>"
}
```

Rules:
- All masses >= 0.0
- At least one mass > 0.0
- Use at most 4 decimal places
- Reasoning must reference the transport journey and product type

## Design rules

- Plain text only. No Python code in the system prompt file.
- No emojis.
- Keep total length under 120 lines.
- Use `===` section separators (consistent with Layer 3 system prompts).
- Do NOT include emission factor values in the output format section. The EF values are
  shown in Section 2 for context but are not part of the model's output task.

## Files to create

- `prompts/system/00_packaging_system.txt`

## Files to remove

- `prompts/reality_check_prompts.py` -- not used in V2 (no LLM-based validation)

## Reference

- Layer 3 system prompts pattern: `layer_3/prompts/system/00_role.txt` through `07_output_format.txt`
- Design doc: `layer_4/DESIGN_V2.md` section 6.1 (System Prompt)
