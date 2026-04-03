"""
Prompt templates for Layer 4 Packaging Configuration Generator

Builds context-aware prompts for generating realistic packaging configurations
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


# Definition of realistic packaging configurations for consistent generation
REALISM_DEFINITION = """
=== DEFINITION OF REALISTIC PACKAGING CONFIGURATIONS ===
A REALISTIC packaging configuration means:
1. WEIGHT-PROPORTIONAL: Packaging weight is 2-10% of product weight
   - Lightweight items (t-shirts): 5-15g packaging
   - Medium items (jeans, dresses): 20-50g packaging
   - Heavy items (coats, boots): 50-150g packaging
2. CATEGORY-APPROPRIATE: Packaging materials match product requirements
   - Delicate items: Tissue paper + polybag
   - Structured items: Cardboard box + paper stuffing
   - Hanging items: Garment cover + hanger
3. TRANSPORT-SUITABLE: Protection level matches transport distance
   - Short-haul: Minimal packaging (polybag only)
   - Long-haul: Robust packaging (box + protective materials)
4. INDUSTRY-STANDARD: Reflects actual retail packaging practices
   - Primary: Direct contact with product (polybag, tissue)
   - Secondary: Outer protection (box, master carton)
5. ENVIRONMENTALLY CONSCIOUS: Modern practices favor recyclable materials
   - Paper/cardboard preferred over plastic where feasible
   - Minimal but adequate protection

Typical packaging combinations by product type:
- T-shirts/Shirts: Polybag + tissue paper + cardboard insert (10-20g)
- Jeans/Trousers: Polybag + paper label + cardboard hanger (25-40g)
- Coats/Jackets: Garment cover + tissue + cardboard box (80-150g)
- Footwear: Shoebox + tissue paper + silica gel (100-200g)
- Accessories: Small box or pouch + tissue (5-30g)

Examples of UNREALISTIC packaging (AVOID):
- 500g packaging for a 200g t-shirt (excessive)
- No packaging at all for international shipment (inadequate)
- Glass containers for soft goods (inappropriate material)
"""

class PromptBuilder:
    """Builds prompts for packaging configuration generation."""

    def __init__(self, config):
        self.config = config
        self.realism_definition = REALISM_DEFINITION

    def build_product_context(self, 
                            category_id: str,
                            category_name: str,
                            subcategory_id: str,
                            subcategory_name: str,
                            total_weight_kg: float,
                            transport_distance_km: float,
                            supply_chain_type: str) -> str:
        """Build product context for packaging generation."""
        
        context_parts = []
        
        # Basic product info
        context_parts.append(f"Product: {subcategory_name} ({subcategory_id})")
        context_parts.append(f"Category: {category_name} ({category_id})")
        context_parts.append(f"Product Weight: {total_weight_kg} kg")
        
        # Transport context
        context_parts.append(f"Transport Distance: {transport_distance_km} km")
        context_parts.append(f"Supply Chain Type: {supply_chain_type}")
        
        # Product-specific context
        if "t-shirt" in subcategory_name.lower() or "shirt" in subcategory_name.lower():
            context_parts.append("Product Type: Basic apparel (lightweight, foldable)")
        elif "coat" in subcategory_name.lower() or "jacket" in subcategory_name.lower():
            context_parts.append("Product Type: Outerwear (heavier, may need hanging)")
        elif "shoes" in subcategory_name.lower() or "footwear" in subcategory_name.lower():
            context_parts.append("Product Type: Footwear (structured, needs protection)")
        elif "dress" in subcategory_name.lower():
            context_parts.append("Product Type: Fashion apparel (may be delicate)")
        else:
            context_parts.append("Product Type: General apparel")
        
        # Packaging intensity determination
        intensity = self.config.get_packaging_intensity(total_weight_kg, transport_distance_km, supply_chain_type)
        context_parts.append(f"Packaging Intensity: {intensity}")
        
        return "\n".join(context_parts)

    def build_transport_context(self, 
                              transport_distance_km: float,
                              supply_chain_type: str,
                              origin_region: str) -> str:
        """Build transport context for packaging requirements."""
        
        context_parts = []
        
        # Transport distance context
        if transport_distance_km < 500:
            context_parts.append("Transport: Short-haul (<500km), minimal protection needed")
        elif transport_distance_km < 2000:
            context_parts.append("Transport: Medium-haul (500-2000km), standard protection")
        else:
            context_parts.append("Transport: Long-haul (>2000km), enhanced protection needed")
        
        # Supply chain type context
        if supply_chain_type == "long_haul":
            context_parts.append("Supply Chain: Long-haul international, robust packaging required")
        elif supply_chain_type == "medium_haul":
            context_parts.append("Supply Chain: Regional transport, standard packaging")
        else:
            context_parts.append("Supply Chain: Local/domestic, minimal packaging")
        
        # Origin region context
        if origin_region:
            context_parts.append(f"Origin Region: {origin_region}")
            
            # Regional-specific considerations
            if "Bangladesh" in origin_region:
                context_parts.append("Regional Context: Major textile manufacturing hub")
            elif "Vietnam" in origin_region:
                context_parts.append("Regional Context: Key apparel manufacturing center")
            elif "China" in origin_region:
                context_parts.append("Regional Context: Major manufacturing base")
        
        return "\n".join(context_parts)

    def build_packaging_prompt(self, 
                             product_context: str,
                             transport_context: str,
                             num_configs: int) -> str:
        """Build comprehensive prompt for packaging configuration generation."""
        
        prompt = f"""You are a packaging logistics expert specializing in textile and apparel supply chains.

{self.realism_definition}

PRODUCT CONTEXT:
{product_context}

TRANSPORT CONTEXT:
{transport_context}

TASK:
Generate exactly {num_configs} realistic packaging configurations for this product.

For each configuration, provide:
1. packaging_config_id: pc-001, pc-002, etc.
2. packaging_items: list of packaging materials used
3. packaging_categories: categories of materials (Paper/Cardboard/Plastic/Glass/Other)
4. packaging_masses_kg: weight of each packaging component
5. total_packaging_mass_kg: total packaging weight
6. reasoning: explanation of packaging choices

PACKAGING CONSIDERATIONS:
- Packaging typically 2-10% of product weight
- Consider protection needs: moisture, impact, presentation
- Balance protection vs. environmental impact
- Use realistic material combinations
- Include both primary and secondary packaging

PACKAGING MATERIALS AVAILABLE:
- Paper/Cardboard: Cardboard box, Paper wrap, Tissue paper, Kraft paper, Corrugated cardboard, Paper tags/labels
- Plastic: Polybag (PE), Plastic hanger, Plastic clips, Shrink wrap, Bubble wrap, Plastic tags, Garment cover
- Glass: Glass fibre reinforcement, Decorative glass elements
- Other: Composite materials, Mixed material packaging

Generate exactly {num_configs} varied but realistic packaging configurations."""

        return prompt

    def build_validation_prompt(self, config_data: Dict[str, Any]) -> str:
        """Build prompt for validating generated packaging configurations."""
        
        return f"""Validate this packaging configuration for plausibility:

Configuration: {json.dumps(config_data, indent=2)}

Check for:
1. Realistic material combinations
2. Appropriate packaging weights (2-10% of product weight)
3. Logical material choices for product type
4. Complete field coverage
5. Valid JSON structure

Provide brief validation feedback."""

    def get_packaging_categories(self) -> List[str]:
        """Get available packaging categories."""
        return [
            "Paper/Cardboard",
            "Plastic", 
            "Glass",
            "Other/Unspecified"
        ]