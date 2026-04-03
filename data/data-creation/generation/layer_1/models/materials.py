"""
Material database and category mapping for Layer 1.

Handles loading materials from base_materials.parquet and grouping them
by category for organized prompt presentation.
"""

import re

import pandas as pd
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set


@dataclass
class Material:
    """Represents a single material from the EcoInvent database."""
    name: str
    material_type: str
    reference_id: str
    description: str
    carbon_footprint: float
    notes: str

    @property
    def display_name(self) -> str:
        """Get a clean display name for the material."""
        return self.name.replace('"', '').strip()


class MaterialDatabase:
    """Database of materials loaded from Product_materials.csv."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.materials: Dict[str, Material] = {}
        self._load_materials()

    def _load_materials(self) -> None:
        """Load materials from Parquet file."""
        df = pd.read_parquet(self.csv_path)
        for _, row in df.iterrows():
            name = str(row.get('material_name', '')).strip()
            if not name:
                continue

            try:
                cf = float(row.get('carbon_footprint_kgCO2e_per_kg', 0))
            except (ValueError, TypeError):
                cf = 0.0

            material = Material(
                name=name,
                material_type=str(row.get('category', '')).strip(),
                reference_id=str(row.get('ref_id', '')).strip(),
                description=str(row.get('material_class', '')).strip(),
                carbon_footprint=cf,
                notes=str(row.get('notes', '')).strip()
            )
            self.materials[name.lower()] = material

    def get_material(self, name: str) -> Optional[Material]:
        """Get a material by name (case-insensitive)."""
        return self.materials.get(name.lower())

    def find_materials(self, pattern: str) -> List[Material]:
        """Find materials matching a pattern."""
        pattern_lower = pattern.lower()
        return [m for name, m in self.materials.items() if pattern_lower in name]

    def get_all_materials(self) -> List[Material]:
        """Get all materials."""
        return list(self.materials.values())

    def get_material_names(self) -> List[str]:
        """Get all material names."""
        return [m.name for m in self.materials.values()]

    def validate_material(self, name: str) -> bool:
        """Check if a material name exists in the database."""
        return name.lower() in self.materials

    def __len__(self) -> int:
        return len(self.materials)


class MaterialCategoryMapper:
    """
    Maps materials to categories for prompt readability.

    Groups all non-excluded materials by category so the full material pool
    can be presented in an organized format to the LLM.
    """

    # Patterns to EXCLUDE from textile materials (non-textile, waste, processes, etc.)
    EXCLUSION_PATTERNS: List[str] = [
        # Raw agricultural materials (not processed fibres)
        "seed cotton", "decorticated",
        # Waste and by-products
        "bottom ash", "residues", "waste", "sludge", "sewage",
        # Processing steps (not materials)
        "bleaching", "mercerizing", "sanforizing", "dyeing", "finishing",
        # Construction materials
        "fibreboard", "fiber board",
        "fibre cement", "gypsum", "insulation",
        "corrugated", "facing tile", "roof slate", "duct",
        "cement bonded", "wood wool",
        # Engineering plastics (not textile fibres)
        "glass-filled", "injection moulded", "reinforced plastic",
        "carbon fibre reinforced", "glass fibre",
        # Packaging
        "packing",
        # Non-textile polymers
        "biopolymer", "bio-polymer", "starch biopolymer",
        # Miscellaneous non-textiles
        "medium density", "mswi", "ww from",
        # Market processes (not actual materials)
        "market for",
    ]

    # Material category definitions based on material types
    CATEGORY_PATTERNS: Dict[str, List[str]] = {
        "natural_fibers": [
            "cotton", "wool", "silk", "flax", "hemp", "jute", "linen",
            "ramie", "sisal", "coir", "kapok", "bamboo"
        ],
        "synthetic_fibers": [
            "polyester", "nylon", "viscose", "acrylic", "polypropylene",
            "elastane", "spandex", "lycra", "polyamide", "polyurethane",
            "modacrylic", "olefin", "pla", "polylactic"
        ],
        "down_feathers": [
            "down", "feather", "duck", "goose"
        ],
        "leather_hides": [
            "leather", "hide", "suede", "nubuck", "calfskin", "lambskin",
            "pigskin", "cowhide", "goatskin"
        ],
        "technical_materials": [
            "carbon fibre", "glass fibre", "aramid", "kevlar",
            "graphene", "ceramic", "metal fibre"
        ],
        "rubber_foam": [
            "rubber", "latex", "eva", "foam", "neoprene",
            "polyurethane foam", "memory foam"
        ],
        "metals": [
            "steel", "aluminium", "aluminum", "brass", "zinc",
            "copper", "iron", "nickel", "chrome", "titanium"
        ],
        "wood_cellulose": [
            "wood", "cellulose", "lyocell", "tencel", "modal",
            "cupro", "paper", "cardboard"
        ],
        "recycled": [
            "recycled", "reclaimed", "upcycled", "regenerated"
        ]
    }

    def __init__(self, material_db: MaterialDatabase):
        self.material_db = material_db
        self.category_materials: Dict[str, List[Material]] = {}
        self._categorize_materials()

    def _is_excluded_material(self, material_name: str) -> bool:
        """Check if a material should be excluded from textile generation."""
        name_lower = material_name.lower()
        for exclusion in self.EXCLUSION_PATTERNS:
            if exclusion in name_lower:
                return True
        return False

    def _categorize_materials(self) -> None:
        """Categorize all materials based on pattern matching and CSV category, excluding non-textiles."""
        # Map CSV categories to our internal categories
        csv_category_mapping = {
            "fibre": ["natural_fibers", "synthetic_fibers", "wood_cellulose"],
            "polymer": ["synthetic_fibers"],
            "textile": ["natural_fibers", "synthetic_fibers"],
            "yarn": ["natural_fibers", "synthetic_fibers"],
            "metal": ["metals"],
            "rubber": ["rubber_foam"],
            "foam": ["rubber_foam"],
            "hide": ["leather_hides"],
            "feathers": ["down_feathers"],
            "natural": ["natural_fibers"],
            "finished_product": ["synthetic_fibers", "natural_fibers"],
        }
        
        for category, patterns in self.CATEGORY_PATTERNS.items():
            self.category_materials[category] = []

            for material in self.material_db.get_all_materials():
                material_name_lower = material.name.lower()
                
                # Skip excluded materials (waste, processes, non-textiles)
                if self._is_excluded_material(material.name):
                    continue
                
                # First check if the CSV category maps to this internal category
                csv_cat = material.material_type.lower()  # material_type holds CSV 'category'
                if csv_cat in csv_category_mapping:
                    if category in csv_category_mapping[csv_cat]:
                        self.category_materials[category].append(material)
                        continue
                
                # Fallback to pattern matching on material name
                for pattern in patterns:
                    if pattern in material_name_lower:
                        self.category_materials[category].append(material)
                        break

    # Mapping from human-readable category names to internal keys
    CATEGORY_NAME_ALIASES: Dict[str, str] = {
        # Natural fibers
        "natural fibers": "natural_fibers",
        "natural_fibers": "natural_fibers",
        "natural fibres": "natural_fibers",
        "1. natural fibers": "natural_fibers",
        # Synthetic fibers
        "synthetic fibers": "synthetic_fibers",
        "synthetic_fibers": "synthetic_fibers",
        "synthetic fibres": "synthetic_fibers",
        "2. synthetic fibers": "synthetic_fibers",
        # Down & feathers
        "down & feathers": "down_feathers",
        "down_feathers": "down_feathers",
        "down and feathers": "down_feathers",
        "3. down & feathers": "down_feathers",
        # Leather & hides
        "leather & hides": "leather_hides",
        "leather_hides": "leather_hides",
        "leather and hides": "leather_hides",
        "leather": "leather_hides",
        "4. leather & hides": "leather_hides",
        # Technical materials
        "technical materials": "technical_materials",
        "technical_materials": "technical_materials",
        "5. technical materials": "technical_materials",
        # Rubber & foam
        "rubber & foam": "rubber_foam",
        "rubber_foam": "rubber_foam",
        "rubber and foam": "rubber_foam",
        "6. rubber & foam": "rubber_foam",
        # Metals
        "metals": "metals",
        "7. metals": "metals",
        # Wood & cellulose
        "wood & cellulose": "wood_cellulose",
        "wood_cellulose": "wood_cellulose",
        "wood and cellulose": "wood_cellulose",
        "8. wood & cellulose": "wood_cellulose",
        # Recycled
        "recycled materials": "recycled",
        "recycled": "recycled",
        "9. recycled materials": "recycled",
    }

    def _normalize_category_name(self, category: str) -> str:
        """Normalize a category name to internal key format."""
        # Clean up the category name
        category_clean = category.strip().lower()

        # Remove numbering prefix like "1. " or "2. "
        import re
        category_clean = re.sub(r'^\d+\.\s*', '', category_clean)

        # Try alias lookup
        if category_clean in self.CATEGORY_NAME_ALIASES:
            return self.CATEGORY_NAME_ALIASES[category_clean]

        # Try converting to snake_case
        snake_case = category_clean.replace(' ', '_').replace('&', 'and').replace('-', '_')
        if snake_case in self.category_materials:
            return snake_case

        # Try simple underscore replacement
        simple = category_clean.replace(' ', '_')
        if simple in self.category_materials:
            return simple

        # Return original if no match (will be filtered out)
        return category_clean

    def get_materials_for_categories(self, categories: List[str]) -> List[Material]:
        """Get all materials belonging to specified categories."""
        materials: Set[str] = set()
        result: List[Material] = []

        for category in categories:
            # Normalize category name to internal key
            normalized_category = self._normalize_category_name(category)

            if normalized_category in self.category_materials:
                for material in self.category_materials[normalized_category]:
                    if material.name not in materials:
                        materials.add(material.name)
                        result.append(material)

        return result

    def get_all_textile_materials(self) -> List[Material]:
        """Get all non-excluded materials from every category."""
        seen = set()
        result = []
        for materials in self.category_materials.values():
            for m in materials:
                if m.name not in seen:
                    seen.add(m.name)
                    result.append(m)
        return result

    def format_all_materials_grouped(self) -> str:
        """Format ALL materials grouped by category for the prompt."""
        category_display = {
            "natural_fibers": "Natural Fibers",
            "synthetic_fibers": "Synthetic Fibers",
            "down_feathers": "Down & Feathers",
            "leather_hides": "Leather & Hides",
            "technical_materials": "Technical Materials",
            "rubber_foam": "Rubber & Foam",
            "metals": "Metals",
            "wood_cellulose": "Wood & Cellulose",
            "recycled": "Recycled Materials",
        }
        sections = []
        for cat_key, display_name in category_display.items():
            materials = self.category_materials.get(cat_key, [])
            if not materials:
                continue
            lines = [f"{display_name}:"]
            for m in materials:
                lines.append(f"- {m.display_name} (EF: {m.carbon_footprint:.2f} kg CO2eq/kg)")
            sections.append("\n".join(lines))
        return "\n\n".join(sections)

    def format_materials_for_prompt(self, materials: List[Material], include_ef: bool = True) -> str:
        """Format materials list for inclusion in prompt."""
        lines = []
        for m in materials:
            if include_ef:
                lines.append(f"- {m.display_name} (EF: {m.carbon_footprint:.2f} kg CO2eq/kg)")
            else:
                lines.append(f"- {m.display_name}")
        return "\n".join(lines)

    def format_categories_for_prompt(self) -> str:
        """Format category descriptions for Stage A prompt."""
        category_descriptions = {
            "natural_fibers": "Natural fibers (cotton, wool, silk, flax, hemp, jute, linen, bamboo)",
            "synthetic_fibers": "Synthetic fibers (polyester, nylon, viscose, acrylic, elastane, polyamide)",
            "down_feathers": "Down & feathers (duck down, goose down, feathers)",
            "leather_hides": "Leather & hides (leather, suede, nubuck, calfskin, lambskin)",
            "technical_materials": "Technical materials (carbon fibre, glass fibre, aramid, kevlar)",
            "rubber_foam": "Rubber & foam (rubber, latex, EVA, neoprene, foam)",
            "metals": "Metals (steel, aluminium, brass, zinc, copper - for hardware)",
            "wood_cellulose": "Wood & cellulose (lyocell, tencel, modal, cupro)",
            "recycled": "Recycled materials (recycled polyester, recycled cotton, etc.)"
        }

        lines = []
        for i, (cat, desc) in enumerate(category_descriptions.items(), 1):
            count = len(self.category_materials.get(cat, []))
            lines.append(f"{i}. {desc} [{count} materials]")

        return "\n".join(lines)

    def get_category_stats(self) -> Dict[str, int]:
        """Get count of materials per category."""
        return {cat: len(materials) for cat, materials in self.category_materials.items()}
