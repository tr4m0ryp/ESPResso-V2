"""
Prompt templates for Layer 1 stratified batch generation.
"""

SYSTEM_PROMPT = """You are a fashion product composition expert with deep knowledge of textile materials, garment construction, and manufacturing processes. You generate realistic material compositions for fashion products based on industry standards and market practices.

=== DEFINITION OF REALISTIC ===
A REALISTIC material composition means:
1. COMMERCIALLY VIABLE: The product could actually be manufactured and sold in retail stores
2. MATERIALLY APPROPRIATE: Materials are suitable for the product's intended use, comfort, and durability
3. INDUSTRY-STANDARD BLENDS: Fiber ratios follow established textile industry practices
4. WEIGHT-ACCURATE: Total weight matches real-world products of the same category
5. FUNCTIONALLY LOGICAL: Material choices support the garment's purpose (stretch for activewear, warmth for coats, etc.)
6. COST-FEASIBLE: Blend ratios reflect realistic cost structures (e.g., cashmere as minor component, not primary)

Examples of REALISTIC compositions:
- T-shirt: 95% cotton, 5% elastane (0.18 kg) - common stretch tee
- Coat: 70% wool, 30% polyester (1.2 kg) - standard winter coat blend
- Jeans: 98% cotton, 2% elastane (0.55 kg) - typical stretch denim

Examples of UNREALISTIC compositions (AVOID):
- T-shirt: 100% cashmere (too expensive for basic tee)
- Jeans: 50% silk, 50% wool (wrong materials entirely)
- Coat: 0.05 kg total weight (impossibly light)"""


SINGLE_PRODUCT_PROMPT_TEMPLATE = """Generate a realistic material composition for this fashion product.

TARGET PRODUCT:
{product_context}

AVAILABLE MATERIALS (grouped by category -- select from ANY category):
{materials_grouped}

CONSTRAINTS:
- Select 2-5 materials from ANY category above
- Total weight must be realistic: {weight_min:.2f} - {weight_max:.2f} kg
- Percentages must sum to 100%
- Primary material should dominate (typically 50-80%)

STRICT NAME RULE:
You MUST copy-paste material names EXACTLY from the list above.
Do NOT paraphrase, abbreviate, or invent material names.
Do NOT use generic names like "cotton" or "polyester" -- use the FULL name
(e.g. "fibre, cotton" not "cotton", "fibre, polyester" not "polyester").
Any composition with a material name not found in the list above will be REJECTED.

OUTPUT FORMAT (JSON only):
{{
  "category_id": "{category_id}",
  "category_name": "{category_name}",
  "subcategory_id": "{subcategory_id}",
  "subcategory_name": "{subcategory_name}",
  "materials": ["material1", "material2"],
  "material_weights_kg": [0.xx, 0.xx],
  "material_percentages": [xx, xx],
  "total_weight_kg": x.xx
}}"""


STRATIFIED_BATCH_PROMPT_TEMPLATE = """Generate {num_products} material compositions for {product_context}.
Organize your output into EXACTLY the 5 sections below.
Generate EXACTLY {per_section} products per section ({num_products} total).

=== SECTION 1: CONVENTIONAL ({per_section} products) ===
Primary materials: fibre, cotton / fibre, polyester / textile, woven cotton / textile, knit cotton.
Generate compositions using mainstream, widely available material combinations.

=== SECTION 2: NATURAL/SUSTAINABLE ({per_section} products) ===
Primary materials: fibre, flax / cottonized fibre, hemp / fibre, cotton, organic / cellulose fibre / fibre, viscose.
Generate compositions prioritizing natural or sustainably sourced materials.

=== SECTION 3: PREMIUM ({per_section} products) ===
Primary materials: textile, silk / fibre, silk, short / sheep fleece in the grease / wool, organic, at farm gate.
Generate compositions using high-end materials found in luxury retail.

=== SECTION 4: PERFORMANCE ({per_section} products) ===
Primary materials: nylon 6 / nylon 6-6 / fibre, polyester / ethylene vinyl acetate copolymer.
Generate compositions optimized for function (stretch, moisture-wicking, durability).

=== SECTION 5: BLENDED/INNOVATIVE ({per_section} products) ===
Generate compositions using unusual but commercially real material combinations
that do not fit neatly into the above categories. Use ONLY materials from the list below.

AVAILABLE MATERIALS (select from ANY category -- grouped for reference):
{materials_grouped}

CONSTRAINTS:
- Select 2-5 materials from ANY category above for each product
- Weight range: {weight_min:.2f} - {weight_max:.2f} kg
- Percentages must sum to 100%
- Every composition must represent a product that could be sold in retail

STRICT NAME RULE:
You MUST copy-paste material names EXACTLY from the list above.
Do NOT paraphrase, abbreviate, or invent material names.
Do NOT add prefixes like "leather," before hide names.
Do NOT use generic names like "cotton" or "polyester" -- use the FULL name
(e.g. "fibre, cotton" not "cotton", "fibre, polyester" not "polyester").
Any composition with a material name not found in the list above will be REJECTED.

ANTI-DUPLICATION:
Before returning your response, review ALL {num_products} products across all sections.
No two products may share the same set of materials (even with different percentages).
If you find duplicates, replace one with a different combination.

OUTPUT FORMAT (JSON):
{{
  "products": [
    {{
      "section": 1,
      "category_id": "{category_id}",
      "category_name": "{category_name}",
      "subcategory_id": "{subcategory_id}",
      "subcategory_name": "{subcategory_name}",
      "materials": ["material1", "material2"],
      "material_weights_kg": [0.xx, 0.xx],
      "material_percentages": [xx, xx],
      "total_weight_kg": x.xx
    }},
    ...
  ]
}}

Generate exactly {num_products} REALISTIC compositions now:"""


FILL_BATCH_PROMPT_TEMPLATE = """Generate {num_products} additional material compositions for {product_context}.

These are FILL records to complete a batch. Generate compositions that are DIFFERENT
from the ones already generated.

ALREADY GENERATED (do NOT repeat these material combinations):
{existing_fingerprints}

{section_instructions}

AVAILABLE MATERIALS (select from ANY category):
{materials_grouped}

CONSTRAINTS:
- Select 2-5 materials from ANY category above
- Weight range: {weight_min:.2f} - {weight_max:.2f} kg
- Percentages must sum to 100%

STRICT NAME RULE:
You MUST copy-paste material names EXACTLY from the list above.
Do NOT paraphrase, abbreviate, or invent material names.
Do NOT use generic names like "cotton" or "polyester" -- use the FULL name
(e.g. "fibre, cotton" not "cotton", "fibre, polyester" not "polyester").
Any composition with a material name not found in the list above will be REJECTED.

OUTPUT FORMAT (JSON):
{{
  "products": [
    {{
      "category_id": "{category_id}",
      "category_name": "{category_name}",
      "subcategory_id": "{subcategory_id}",
      "subcategory_name": "{subcategory_name}",
      "materials": ["material1", "material2"],
      "material_weights_kg": [0.xx, 0.xx],
      "material_percentages": [xx, xx],
      "total_weight_kg": x.xx
    }},
    ...
  ]
}}

Generate exactly {num_products} compositions now:"""


class PromptBuilder:
    """Builds prompts for stratified batch generation."""

    def __init__(self):
        self.system_prompt = SYSTEM_PROMPT

    def build_single_product_prompt(
        self,
        product_context: str,
        materials_grouped: str,
        category_id: str,
        category_name: str,
        subcategory_id: str,
        subcategory_name: str,
        weight_min: float,
        weight_max: float
    ) -> str:
        """Build prompt for generating a single product composition."""
        return SINGLE_PRODUCT_PROMPT_TEMPLATE.format(
            product_context=product_context,
            materials_grouped=materials_grouped,
            category_id=category_id,
            category_name=category_name,
            subcategory_id=subcategory_id,
            subcategory_name=subcategory_name,
            weight_min=weight_min,
            weight_max=weight_max
        )

    def build_batch_prompt(
        self,
        product_context: str,
        materials_grouped: str,
        category_id: str,
        category_name: str,
        subcategory_id: str,
        subcategory_name: str,
        weight_min: float,
        weight_max: float,
        num_products: int
    ) -> str:
        """Build stratified batch prompt for generating multiple products."""
        per_section = num_products // 5
        return STRATIFIED_BATCH_PROMPT_TEMPLATE.format(
            product_context=product_context,
            materials_grouped=materials_grouped,
            category_id=category_id,
            category_name=category_name,
            subcategory_id=subcategory_id,
            subcategory_name=subcategory_name,
            weight_min=weight_min,
            weight_max=weight_max,
            num_products=num_products,
            per_section=per_section
        )

    def build_fill_prompt(
        self,
        product_context: str,
        materials_grouped: str,
        category_id: str,
        category_name: str,
        subcategory_id: str,
        subcategory_name: str,
        weight_min: float,
        weight_max: float,
        num_products: int,
        existing_fingerprints: str,
        section_instructions: str = "Generate diverse compositions across different material strategies."
    ) -> str:
        """Build fill prompt for targeted follow-up when records are missing."""
        return FILL_BATCH_PROMPT_TEMPLATE.format(
            product_context=product_context,
            materials_grouped=materials_grouped,
            category_id=category_id,
            category_name=category_name,
            subcategory_id=subcategory_id,
            subcategory_name=subcategory_name,
            weight_min=weight_min,
            weight_max=weight_max,
            num_products=num_products,
            existing_fingerprints=existing_fingerprints,
            section_instructions=section_instructions
        )

    def estimate_tokens(self, text: str) -> int:
        """Rough estimate of token count (4 chars per token average)."""
        return len(text) // 4
