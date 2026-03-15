"""
Prompt templates for Layer 2 preprocessing path generation.
"""

SYSTEM_PROMPT = """You are a textile manufacturing expert with deep knowledge of material processing,
manufacturing workflows, and industrial processes. You generate realistic preprocessing pathways
for fashion products based on their material composition and industry standards.

CRITICAL: Each pathway you generate must be UNIQUE and DISTINCT. Avoid duplicating step sequences.

=== DEFINITION OF REALISTIC PREPROCESSING PATHS ===
A REALISTIC preprocessing pathway means:
1. TECHNICALLY FEASIBLE: Each step can physically be performed on the given material
2. LOGICALLY SEQUENCED: Steps follow the actual order used in textile manufacturing
   (e.g., spinning before weaving, dyeing before finishing)
3. MATERIAL-APPROPRIATE: Only steps applicable to the specific fiber type are included
   (e.g., ginning for cotton, not for polyester; extrusion for synthetics, not for wool)
4. INDUSTRY-STANDARD: Reflects actual factory workflows used by textile manufacturers
5. COMPLETE BUT NOT REDUNDANT: Includes necessary steps without duplicating processes
6. COMPATIBLE PROCESSES: No conflicting treatments (e.g., not both waterproofing AND high-absorbency finishing)

Examples of REALISTIC pathways:
- Cotton T-shirt: ginning → carding → spinning → knitting → scouring → batch dyeing → softening
- Polyester Jacket: extrusion → drawing → texturing → weaving → continuous dyeing → waterproofing
- Wool Sweater: scouring → carding → combing → spinning → knitting → finishing → softening

Examples of UNREALISTIC pathways (AVOID):
- Cotton: extrusion → drawing (these are for synthetics only)
- Polyester: ginning → retting (these are for natural fibers only)
- Any: weaving → spinning (wrong order - spinning must come first)"""


GENERATION_PROMPT_TEMPLATE = """Generate {num_paths} TRULY DIFFERENT preprocessing pathways for this fashion product.

PRODUCT:
- Category: {category_name} ({category_id})
- Subcategory: {subcategory_name} ({subcategory_id})
- Weight: {total_weight_kg} kg
- Materials: {materials_with_weights}

AVAILABLE STEPS:
{processing_steps}

VALID COMBINATIONS:
{material_process_combinations}

MANDATORY DIVERSITY RULES - EACH PATH MUST BE STRUCTURALLY DIFFERENT:

Path 1 (MINIMAL - 3-4 steps): Basic processing only
  Example: carding -> spinning -> weaving -> finishing

Path 2 (STANDARD WOVEN - 5-6 steps): Include weaving + batch dyeing
  Example: ginning -> carding -> spinning -> weaving -> batch dyeing -> finishing

Path 3 (STANDARD KNIT - 5-6 steps): Include knitting + batch dyeing
  Example: scouring -> carding -> spinning -> knitting -> batch dyeing -> softening

Path 4 (PREMIUM - 6-7 steps): Include mercerizing or sanforizing
  Example: ginning -> carding -> spinning -> weaving -> mercerizing -> continuous dyeing -> calendering

Path 5 (PERFORMANCE - 6-7 steps): Include waterproofing or flame retardant
  Example: carding -> spinning -> weaving -> batch dyeing -> finishing -> waterproofing

Path 6 (ANTIMICROBIAL - 5-6 steps): Include antimicrobial treatment
  Example: spinning -> knitting -> bleaching -> batch dyeing -> antimicrobial treatment -> finishing

Path 7 (NONWOVEN - 4-5 steps): Use nonwoven production instead of weaving/knitting
  Example: carding -> nonwoven production -> batch dyeing -> coating -> finishing

Path 8 (PRINTED - 6-7 steps): Include printing step
  Example: carding -> spinning -> weaving -> bleaching -> printing -> finishing -> softening

Path 9 (TEXTURED SYNTHETIC - 5-6 steps): Use texturing for synthetic fibers
  Example: extrusion -> drawing -> texturing -> weaving -> continuous dyeing -> heat setting

Path 10 (LUXURY FINISH - 7-8 steps): Multiple finishing steps
  Example: scouring -> combing -> spinning -> weaving -> batch dyeing -> raising -> softening -> calendering

BAD OUTPUT (DO NOT GENERATE THIS):
[
  {{"steps": ["spinning", "finishing", "mercerizing", "nonwoven production"]}},
  {{"steps": ["spinning", "nonwoven production", "finishing", "mercerizing"]}},
  {{"steps": ["nonwoven production", "spinning", "finishing", "mercerizing"]}},
  ... (same 4 steps shuffled 10 times - THIS IS WRONG)
]

GOOD OUTPUT (GENERATE LIKE THIS):
[
  {{"steps": ["carding", "spinning", "weaving", "finishing"]}},
  {{"steps": ["ginning", "carding", "spinning", "weaving", "batch dyeing", "softening"]}},
  {{"steps": ["scouring", "spinning", "knitting", "continuous dyeing", "finishing"]}},
  ... (different steps, different lengths, different processes)
]

OUTPUT FORMAT (JSON array):
[
  {{
    "preprocessing_path_id": "pp-001",
    "preprocessing_steps": ["step1", "step2", ...],
    "step_material_mapping": {{"material_name": ["step1", "step2"]}},
    "reasoning": "Minimal processing path"
  }},
  ...exactly {num_paths} items...
]

RESPOND WITH ONLY THE JSON ARRAY. NO OTHER TEXT."""


BATCH_GENERATION_PROMPT_TEMPLATE = """Generate preprocessing pathways for a BATCH of {batch_size} fashion products.

BATCH OF PRODUCTS:
{products_batch}

AVAILABLE PROCESSING STEPS (32 steps):
{processing_steps}

MATERIAL-PROCESS COMBINATIONS LOOKUP:
{material_process_combinations}

TASK:
For EACH product in the batch, generate {paths_per_product} UNIQUE preprocessing pathways.

CRITICAL DEDUPLICATION RULES:
1. Within each product: All {paths_per_product} pathways must be DISTINCT
2. Across products with same materials: Vary the pathways to create diversity
3. Use the full range of available processing steps

OUTPUT FORMAT (JSON object with product IDs as keys):
{{
  "product_1": [
    {{"preprocessing_path_id": "pp-001", "preprocessing_steps": [...], "step_material_mapping": {{...}}, "reasoning": "..."}},
    ...
  ],
  "product_2": [
    ...
  ]
}}"""


class PromptBuilder:
    """Builds prompts for preprocessing path generation."""

    def __init__(self):
        self.system_prompt = SYSTEM_PROMPT

    def build_generation_prompt(
        self,
        category_id: str,
        category_name: str,
        subcategory_id: str,
        subcategory_name: str,
        total_weight_kg: float,
        materials_with_weights: str,
        processing_steps: str,
        material_process_combinations: str,
        num_paths: int = 10
    ) -> str:
        """Build the main generation prompt for a single product."""
        return GENERATION_PROMPT_TEMPLATE.format(
            category_id=category_id,
            category_name=category_name,
            subcategory_id=subcategory_id,
            subcategory_name=subcategory_name,
            total_weight_kg=total_weight_kg,
            materials_with_weights=materials_with_weights,
            processing_steps=processing_steps,
            material_process_combinations=material_process_combinations,
            num_paths=num_paths
        )

    def build_batch_prompt(
        self,
        products_batch: str,
        processing_steps: str,
        material_process_combinations: str,
        batch_size: int,
        paths_per_product: int = 10
    ) -> str:
        """Build prompt for batch processing multiple products."""
        return BATCH_GENERATION_PROMPT_TEMPLATE.format(
            batch_size=batch_size,
            products_batch=products_batch,
            processing_steps=processing_steps,
            material_process_combinations=material_process_combinations,
            paths_per_product=paths_per_product
        )

    def estimate_tokens(self, text: str) -> int:
        """Rough estimate of token count (4 chars per token average)."""
        return len(text) // 4
