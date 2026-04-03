"""
Taxonomy loader for Layer 1.

Loads and manages the clothing taxonomy from taxonomy_category.parquet.
"""

import pandas as pd
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Iterator


@dataclass
class TaxonomyItem:
    """Represents a single item in the taxonomy hierarchy."""
    main_category: str
    main_category_id: str
    subcategory: str
    subcategory_id: str
    sub_subcategory: Optional[str]
    sub_subcategory_id: Optional[str]

    @property
    def full_id(self) -> str:
        """Get the most specific ID available."""
        if self.sub_subcategory_id:
            return self.sub_subcategory_id
        return self.subcategory_id

    @property
    def full_name(self) -> str:
        """Get the most specific name available."""
        if self.sub_subcategory:
            return self.sub_subcategory
        return self.subcategory

    @property
    def category_path(self) -> str:
        """Get the full category path."""
        if self.sub_subcategory:
            return f"{self.main_category} > {self.subcategory} > {self.sub_subcategory}"
        return f"{self.main_category} > {self.subcategory}"

    @property
    def is_clothing(self) -> bool:
        return self.main_category_id == "cl"

    @property
    def is_footwear(self) -> bool:
        return self.main_category_id == "fw"

    @property
    def is_accessory(self) -> bool:
        return self.main_category_id == "ac"


class TaxonomyLoader:
    """Loads and manages the product taxonomy."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.items: List[TaxonomyItem] = []
        self._by_id: Dict[str, TaxonomyItem] = {}
        self._load_taxonomy()

    def _load_taxonomy(self) -> None:
        """Load taxonomy from Parquet file."""
        df = pd.read_parquet(self.csv_path)
        for _, row in df.iterrows():
            item = TaxonomyItem(
                main_category=str(row.get('Main_Category', '')).strip(),
                main_category_id=str(row.get('Main_Category_ID', '')).strip(),
                subcategory=str(row.get('Subcategory', '')).strip(),
                subcategory_id=str(row.get('Subcategory_ID', '')).strip(),
                sub_subcategory=str(row.get('Sub_Subcategory', '')).strip() or None,
                sub_subcategory_id=str(row.get('Sub_Subcategory_ID', '')).strip() or None,
            )
            self.items.append(item)
            self._by_id[item.full_id] = item

    def get_by_id(self, item_id: str) -> Optional[TaxonomyItem]:
        """Get a taxonomy item by its ID."""
        return self._by_id.get(item_id)

    def get_clothing_items(self) -> List[TaxonomyItem]:
        """Get all clothing items (sub-subcategory level)."""
        return [item for item in self.items if item.is_clothing]

    def get_footwear_items(self) -> List[TaxonomyItem]:
        """Get all footwear items (subcategory level)."""
        return [item for item in self.items if item.is_footwear]

    def get_accessory_items(self) -> List[TaxonomyItem]:
        """Get all accessory items (subcategory level)."""
        return [item for item in self.items if item.is_accessory]

    def get_all_generation_targets(self) -> List[TaxonomyItem]:
        """
        Get all items that should be used for generation.

        For clothing: returns sub-subcategory items
        For footwear/accessories: returns subcategory items
        """
        return self.items

    def get_unique_subcategories(self) -> List[str]:
        """Get unique subcategory IDs."""
        seen = set()
        result = []
        for item in self.items:
            if item.subcategory_id not in seen:
                seen.add(item.subcategory_id)
                result.append(item.subcategory_id)
        return result

    def get_items_by_main_category(self, main_category_id: str) -> List[TaxonomyItem]:
        """Get all items for a main category."""
        return [item for item in self.items if item.main_category_id == main_category_id]

    def get_items_by_subcategory(self, subcategory_id: str) -> List[TaxonomyItem]:
        """Get all items for a subcategory."""
        return [item for item in self.items if item.subcategory_id == subcategory_id]

    def __iter__(self) -> Iterator[TaxonomyItem]:
        return iter(self.items)

    def __len__(self) -> int:
        return len(self.items)

    def get_stats(self) -> Dict[str, int]:
        """Get taxonomy statistics."""
        return {
            "total_items": len(self.items),
            "clothing_items": len(self.get_clothing_items()),
            "footwear_items": len(self.get_footwear_items()),
            "accessory_items": len(self.get_accessory_items()),
            "unique_subcategories": len(self.get_unique_subcategories()),
        }

    def format_item_for_prompt(self, item: TaxonomyItem) -> str:
        """Format a taxonomy item for inclusion in a prompt."""
        if item.sub_subcategory:
            return (
                f"Category: {item.subcategory} ({item.subcategory_id})\n"
                f"Subcategory: {item.sub_subcategory} ({item.sub_subcategory_id})\n"
                f"Type: {item.main_category}"
            )
        else:
            return (
                f"Category: {item.subcategory} ({item.subcategory_id})\n"
                f"Type: {item.main_category}"
            )
