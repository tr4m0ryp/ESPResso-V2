"""
Prompt templates for Layer 3 Transport Scenario Generator.

Builds context-aware prompts for generating realistic transport scenarios.
"""

import logging
from typing import Dict, List, Any, Optional
from data.data_generation.layer_3.io.layer2_reader import Layer2Record

logger = logging.getLogger(__name__)


# Definition of realistic transport scenarios for consistent generation
REALISM_DEFINITION = """
=== DEFINITION OF REALISTIC TRANSPORT SCENARIOS ===
A REALISTIC transport scenario means:
1. GEOGRAPHICALLY ACCURATE: Origin regions match actual manufacturing locations for the materials
   (e.g., cotton from India/USA/Uzbekistan, synthetic fibers from China/Taiwan, leather from Italy/India)
2. DISTANCE-APPROPRIATE: Total transport distances are plausible for the supply chain type
   - Short-haul: 50-500 km (regional/domestic)
   - Medium-haul: 500-2000 km (continental)
   - Long-haul: 2000-15000 km (intercontinental)
3. MODE-LOGICAL: Transport modes match distance and product characteristics
   - <500 km: Road (truck) dominant
   - 500-2000 km: Road/Rail combination
   - >2000 km: Sea freight for bulk, air for high-value/urgent
4. SUPPLY-CHAIN COHERENT: Multi-leg journeys reflect actual manufacturing flows
   (raw material → processing → manufacturing → distribution center → retail)
5. ECONOMICALLY SENSIBLE: Transport costs are proportional to product value
   (luxury items may use air freight; basic items use sea freight)

Examples of REALISTIC scenarios:
- Cotton T-shirt: India (cotton) → Bangladesh (manufacturing) → EU (retail) = ~8000 km, sea freight
- Wool Coat: Australia (wool) → China (manufacturing) → USA (retail) = ~15000 km, sea freight
- Local Basics: Turkey (manufacturing) → Germany (retail) = ~2500 km, road/sea

Examples of UNREALISTIC scenarios (AVOID):
- 50,000 km total distance (exceeds circumference of Earth)
- Air freight for bulk basic apparel (economically infeasible)
- Raw materials sourced from regions that don't produce them
"""

class PromptBuilder:
    """Builds prompts for transport scenario generation."""

    def __init__(self, config):
        self.config = config
        self.realism_definition = REALISM_DEFINITION

    def build_product_context(self, record: Layer2Record) -> str:
        """Build product context from Layer 2 record."""
        context_parts = []
        
        # Basic product info
        context_parts.append(f"- Category: {record.category_name} ({record.category_id})")
        context_parts.append(f"- Subcategory: {record.subcategory_name} ({record.subcategory_id})")
        context_parts.append(f"- Total Product Weight: {record.total_weight_kg} kg")
        
        # Product type implications for transport
        if "footwear" in record.category_name.lower():
            context_parts.append("- Product Type: Footwear (typically has complex multi-material construction)")
        elif "coat" in record.subcategory_name.lower() or "jacket" in record.subcategory_name.lower():
            context_parts.append("- Product Type: Outerwear (heavier items, may require protective packaging)")
        elif "t-shirt" in record.subcategory_name.lower() or "polo" in record.subcategory_name.lower():
            context_parts.append("- Product Type: Basic apparel (lighter items, high volume)")
        elif "dress" in record.subcategory_name.lower():
            context_parts.append("- Product Type: Fashion apparel (may include delicate fabrics)")
        elif "jeans" in record.subcategory_name.lower() or "trousers" in record.subcategory_name.lower():
            context_parts.append("- Product Type: Bottom wear (durable construction, heavier fabrics)")
        elif "accessories" in record.category_name.lower():
            context_parts.append("- Product Type: Accessories (small, may include metal components)")
        
        return "\n".join(context_parts)

    def build_materials_info(self, record: Layer2Record) -> str:
        """Build materials information for transport context."""
        material_parts = []
        
        material_parts.append("Material Composition:")
        for i, material in enumerate(record.materials):
            weight = record.material_weights_kg[i]
            percentage = record.material_percentages[i]
            material_parts.append(f"  - {material}: {weight} kg ({percentage}%)")
        
        # Add transport-relevant material characteristics
        material_parts.append("")
        material_parts.append("Transport-Relevant Material Characteristics:")
        
        materials_text = " ".join(record.materials).lower()
        
        if any(fiber in materials_text for fiber in ["cotton", "wool", "silk", "linen", "hemp"]):
            material_parts.append("  - Contains natural fibers (typically travel from agricultural regions)")
        
        if any(fiber in materials_text for fiber in ["polyester", "nylon", "acrylic", "viscose"]):
            material_parts.append("  - Contains synthetic fibers (often produced near manufacturing hubs)")
        
        if "leather" in materials_text or "hide" in materials_text or "suede" in materials_text:
            material_parts.append("  - Contains leather (requires specialized processing regions)")
        
        if "down" in materials_text or "feather" in materials_text:
            material_parts.append("  - Contains down/feathers (typically from specific geographic regions)")
        
        if any(metal in materials_text for metal in ["steel", "aluminium", "brass", "zinc"]):
            material_parts.append("  - Contains metal components (hardware, may have different supply chain)")
        
        if "rubber" in materials_text or "latex" in materials_text or "eva" in materials_text:
            material_parts.append("  - Contains rubber/foam materials (typically from tropical regions)")
        
        return "\n".join(material_parts)

    def build_preprocessing_context(self, record: Layer2Record) -> str:
        """Build preprocessing context for manufacturing location inference."""
        process_parts = []
        
        process_parts.append("Manufacturing Process Steps:")
        for i, step in enumerate(record.preprocessing_steps):
            process_parts.append(f"  {i+1}. {step}")
        
        # Add manufacturing implications
        process_parts.append("")
        process_parts.append("Manufacturing Process Implications:")
        
        steps_text = " ".join(record.preprocessing_steps).lower()
        
        if any(step in steps_text for step in ["ginning", "scutching", "retting", "decortication"]):
            process_parts.append("  - Includes fiber preparation (typically near raw material sources)")
        
        if any(step in steps_text for step in ["spinning", "weaving", "knitting"]):
            process_parts.append("  - Includes textile manufacturing (major hubs: China, India, Bangladesh, Vietnam)")
        
        if any(step in steps_text for step in ["tanning", "dyeing", "finishing"]):
            process_parts.append("  - Includes chemical processing (specialized facilities, environmental regulations)")
        
        if "extrusion" in steps_text:
            process_parts.append("  - Includes synthetic fiber production (chemical industry, energy-intensive)")
        
        if any(step in steps_text for step in ["batch dyeing", "continuous dyeing", "printing"]):
            process_parts.append("  - Includes coloration processes (water/energy intensive, regulatory considerations)")
        
        if any(step in steps_text for step in ["waterproofing", "flame retardant", "antimicrobial"]):
            process_parts.append("  - Includes special treatments (specialized chemicals, performance requirements)")
        
        # Material-specific processing insights
        material_step_mapping = record.step_material_mapping
        if material_step_mapping:
            process_parts.append("")
            process_parts.append("Material-Specific Processing:")
            for material, steps in material_step_mapping.items():
                if steps:
                    process_parts.append(f"  - {material}: {', '.join(steps[:3])}{'...' if len(steps) > 3 else ''}")
        
        return "\n".join(process_parts)

    def build_geographic_context(self, record: Layer2Record) -> str:
        """Build geographic context for transport scenario generation."""
        geo_parts = []
        
        # Infer material origins
        geo_parts.append("Material Geographic Origins:")
        for material in record.materials:
            origins = self.config.get_material_origins(material)
            geo_parts.append(f"  - {material}: {', '.join(origins[:3])}{'...' if len(origins) > 3 else ''}")
        
        # Manufacturing hub inference
        geo_parts.append("")
        geo_parts.append("Likely Manufacturing Regions:")
        
        steps_text = " ".join(record.preprocessing_steps).lower()
        materials_text = " ".join(record.materials).lower()
        
        if "leather" in materials_text or "tanning" in steps_text:
            geo_parts.append("  - Leather processing: Italy, India, Brazil, China (specialized facilities)")
        
        if "extrusion" in steps_text or any(syn in materials_text for syn in ["polyester", "nylon", "acrylic"]):
            geo_parts.append("  - Synthetic production: China, Taiwan, South Korea, Japan, USA")
        
        if "footwear" in record.category_name.lower() or "sole" in steps_text:
            geo_parts.append("  - Footwear assembly: Vietnam, China, Indonesia, India")
        
        # Default textile manufacturing
        geo_parts.append("  - Textile manufacturing: China, India, Bangladesh, Vietnam, Pakistan, Turkey")
        
        # Transport route considerations
        geo_parts.append("")
        geo_parts.append("Transport Route Considerations:")
        geo_parts.append("  - Intra-Asian supply chains: Shorter distances, road/rail viable")
        geo_parts.append("  - Asia to Europe/North America: Long-haul, sea freight dominant")
        geo_parts.append("  - Raw materials to manufacturing: Often long-distance (cotton, wool, leather)")
        geo_parts.append("  - Manufacturing to markets: Depends on final destination")
        
        return "\n".join(geo_parts)

    def build_full_context(self, record: Layer2Record) -> Dict[str, str]:
        """Build complete context dictionary for prompt generation."""
        return {
            "product_context": self.build_product_context(record),
            "materials_info": self.build_materials_info(record),
            "preprocessing_context": self.build_preprocessing_context(record),
            "geographic_context": self.build_geographic_context(record),
            "realism_definition": self.realism_definition,
            "output_format": self._get_output_format()
        }
    
    def _get_output_format(self) -> str:
        """Define the exact output format for consistent generation."""
        return """=== REQUIRED OUTPUT FORMAT ===
Generate EXACTLY 5 scenarios per product. Each scenario MUST include:

1. transport_scenario_id: Unique identifier (use format: ts-{suffix})
   - Suffix should be: cost, speed, eco, risk, regional (one per strategy)
2. total_transport_distance_km: Numeric value (float, rounded to 1 decimal)
3. supply_chain_type: One of: short_haul, medium_haul, long_haul
4. origin_region: Text string (country or region name)
5. transport_modes: JSON array of strings (2-3 modes maximum)
   Example: ["sea", "rail"] or ["air", "road"]
6. reasoning: Text string explaining strategic logic (minimum 100 characters)

CRITICAL REQUIREMENTS:
- All 5 scenarios must be STRATEGICALLY DIFFERENT
- Cost-optimized: use sea freight, longer routes, low-cost origins
- Speed-optimized: use air freight, direct routes, efficient hubs
- Eco-optimized: use rail, sustainable routing, green regions
- Risk-diversified: multiple origins, flexible routing
- Regional-proximity: short distances (<4000km), nearby hubs
- Each scenario should reflect a REAL business strategy
- Distances must be realistic for the strategy
- Exactly 2-3 transport modes per scenario (not all modes)

RESPOND WITH VALID JSON ONLY.
"""

    def build_validation_prompt(self, scenario: Dict[str, Any], context: Dict[str, str]) -> str:
        """Build prompt for validating a generated transport scenario."""
        
        prompt = f"""You are a supply chain logistics expert. Validate this transport scenario for plausibility.

PRODUCT CONTEXT:
{context['product_context']}

MATERIALS:
{context['materials_info']}

PROPOSED TRANSPORT SCENARIO:
- Distance: {scenario.get('total_transport_distance_km', 'unknown')} km
- Supply Chain Type: {scenario.get('supply_chain_type', 'unknown')}
- Origin Region: {scenario.get('origin_region', 'unknown')}
- Transport Modes: {', '.join(scenario.get('transport_modes', []))}
- Reasoning: {scenario.get('reasoning', 'none provided')}

VALIDATION CRITERIA:
1. Is the distance realistic for this product type and material origins?
2. Is the origin region logical given the materials and manufacturing processes?
3. Are the transport modes appropriate for the distance and product characteristics?
4. Is the overall supply chain logic sound?

RESPOND WITH:
- "VALID" if the scenario is plausible
- "INVALID: [reason]" if there are issues
- "MODIFY: [suggestions]" if minor adjustments needed

Validation result:"""

        return prompt