"""
Product Composition Generator for Layer 1.

Uses stratified batch prompting with the full material database
and fingerprint-based deduplication.
"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

from ..config.config import Layer1Config
from ..models.materials import MaterialDatabase, MaterialCategoryMapper, Material
from ..models.material_corrector import correct_material_list
from ..models.taxonomy import TaxonomyLoader, TaxonomyItem
from ..clients.api_client import Layer1Client
from ..prompts.prompts import PromptBuilder
from ...shared.api_client import APIError
from ...shared.reality_check_models import RecordCheckResult

logger = logging.getLogger(__name__)


@dataclass
class ProductComposition:
    """Represents a generated product composition."""
    category_id: str
    category_name: str
    subcategory_id: str
    subcategory_name: str
    materials: List[str]
    material_weights_kg: List[float]
    material_percentages: List[int]
    total_weight_kg: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for CSV output."""
        return {
            "category_id": self.category_id,
            "category_name": self.category_name,
            "subcategory_id": self.subcategory_id,
            "subcategory_name": self.subcategory_name,
            "materials": json.dumps(self.materials),
            "material_weights_kg": json.dumps(self.material_weights_kg),
            "material_percentages": json.dumps(self.material_percentages),
            "total_weight_kg": self.total_weight_kg
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ProductComposition":
        """Create from dictionary."""
        return cls(
            category_id=data["category_id"],
            category_name=data["category_name"],
            subcategory_id=data["subcategory_id"],
            subcategory_name=data["subcategory_name"],
            materials=data["materials"] if isinstance(data["materials"], list) else json.loads(data["materials"]),
            material_weights_kg=data["material_weights_kg"] if isinstance(data["material_weights_kg"], list) else json.loads(data["material_weights_kg"]),
            material_percentages=data["material_percentages"] if isinstance(data["material_percentages"], list) else json.loads(data["material_percentages"]),
            total_weight_kg=float(data["total_weight_kg"])
        )


def composition_fingerprint(comp: ProductComposition) -> tuple:
    """Normalized fingerprint for deduplication. Uses exact percentages."""
    sorted_materials = sorted(zip(comp.materials, comp.material_percentages))
    return tuple((m, p) for m, p in sorted_materials)


def deduplicate_batch(
    compositions: List[ProductComposition],
    seen_fingerprints: set
) -> Tuple[List[ProductComposition], int]:
    """Remove intra-batch and cross-batch duplicates.

    Returns (unique compositions, count of duplicates removed).
    """
    unique = []
    duplicates_removed = 0
    for comp in compositions:
        fp = composition_fingerprint(comp)
        if fp not in seen_fingerprints:
            seen_fingerprints.add(fp)
            unique.append(comp)
        else:
            duplicates_removed += 1
    return unique, duplicates_removed


def verify_batch_count(
    parsed_records: List[ProductComposition],
    expected_count: int,
    subcategory_id: str
) -> Tuple[List[ProductComposition], int]:
    """Verify and adjust batch count. Returns (records, shortfall)."""
    actual = len(parsed_records)
    if actual == expected_count:
        return parsed_records, 0
    if actual > expected_count:
        logger.warning(
            "%s: got %d, expected %d. Trimming.",
            subcategory_id, actual, expected_count,
        )
        return parsed_records[:expected_count], 0
    shortfall = expected_count - actual
    logger.warning(
        "%s: got %d, expected %d. Shortfall: %d",
        subcategory_id, actual, expected_count, shortfall,
    )
    return parsed_records, shortfall


class ProductCompositionGenerator:
    """
    Generates product compositions using stratified batch prompting
    with the full material database.
    """

    def __init__(
        self,
        config: Layer1Config,
        material_db: MaterialDatabase,
        category_mapper: MaterialCategoryMapper,
        taxonomy: TaxonomyLoader,
        api_client: Layer1Client
    ):
        self.config = config
        self.material_db = material_db
        self.category_mapper = category_mapper
        self.taxonomy = taxonomy
        self.api_client = api_client
        self.prompt_builder = PromptBuilder()

    def _get_item_context(self, item: TaxonomyItem) -> dict:
        """Extract common context needed for prompt building."""
        return {
            "product_context": self.taxonomy.format_item_for_prompt(item),
            "materials_grouped": self.category_mapper.format_all_materials_grouped(),
            "category_id": item.subcategory_id,
            "category_name": item.subcategory,
            "subcategory_id": item.sub_subcategory_id or item.subcategory_id,
            "subcategory_name": item.sub_subcategory or item.subcategory,
            "weight_min": self.config.get_weight_range(item.full_id)[0],
            "weight_max": self.config.get_weight_range(item.full_id)[1],
        }

    def generate_for_item(self, item: TaxonomyItem) -> Optional[ProductComposition]:
        """Generate a single composition using the full material database."""
        try:
            ctx = self._get_item_context(item)

            prompt = self.prompt_builder.build_single_product_prompt(
                product_context=ctx["product_context"],
                materials_grouped=ctx["materials_grouped"],
                category_id=ctx["category_id"],
                category_name=ctx["category_name"],
                subcategory_id=ctx["subcategory_id"],
                subcategory_name=ctx["subcategory_name"],
                weight_min=ctx["weight_min"],
                weight_max=ctx["weight_max"],
            )

            response_data = self.api_client.generate_json(
                prompt,
                system_prompt=self.prompt_builder.system_prompt
            )

            return self._parse_composition_response(response_data, item)

        except APIError as e:
            logger.error("API error generating for %s: %s", item.full_id, e)
            return None
        except Exception as e:
            logger.error("Error generating for %s: %s", item.full_id, e)
            return None

    def generate_batch_for_item(
        self,
        item: TaxonomyItem,
        num_products: int = 100
    ) -> List[ProductComposition]:
        """Generate multiple product compositions in a single stratified API call."""
        try:
            ctx = self._get_item_context(item)

            prompt = self.prompt_builder.build_batch_prompt(
                product_context=ctx["product_context"],
                materials_grouped=ctx["materials_grouped"],
                category_id=ctx["category_id"],
                category_name=ctx["category_name"],
                subcategory_id=ctx["subcategory_id"],
                subcategory_name=ctx["subcategory_name"],
                weight_min=ctx["weight_min"],
                weight_max=ctx["weight_max"],
                num_products=num_products,
            )

            max_tokens = num_products * 250 + 500

            response_data = self.api_client.generate_batch_json(
                prompt,
                system_prompt=self.prompt_builder.system_prompt,
                max_tokens=max_tokens
            )

            compositions = []
            for product_data in response_data:
                composition = self._parse_composition_response(product_data, item)
                if composition:
                    compositions.append(composition)

            logger.info(
                "Batch generated %d/%d products for %s",
                len(compositions), num_products, item.full_id,
            )
            return compositions

        except APIError as e:
            logger.error("API error in batch generation for %s: %s", item.full_id, e)
            return []
        except Exception as e:
            logger.error("Error in batch generation for %s: %s", item.full_id, e)
            return []

    def generate_fill_batch(
        self,
        item: TaxonomyItem,
        num_needed: int,
        existing_fingerprints: set,
    ) -> List[ProductComposition]:
        """Generate fill records with anti-duplication awareness."""
        try:
            ctx = self._get_item_context(item)

            # Format existing fingerprints for the prompt
            fp_lines = []
            for fp in existing_fingerprints:
                parts = [f"{m} ~{p}%" for m, p in fp]
                fp_lines.append("- " + ", ".join(parts))
            fp_text = "\n".join(fp_lines[-50:])  # Limit to last 50 to stay in token budget

            prompt = self.prompt_builder.build_fill_prompt(
                product_context=ctx["product_context"],
                materials_grouped=ctx["materials_grouped"],
                category_id=ctx["category_id"],
                category_name=ctx["category_name"],
                subcategory_id=ctx["subcategory_id"],
                subcategory_name=ctx["subcategory_name"],
                weight_min=ctx["weight_min"],
                weight_max=ctx["weight_max"],
                num_products=num_needed,
                existing_fingerprints=fp_text,
            )

            max_tokens = num_needed * 250 + 500

            response_data = self.api_client.generate_batch_json(
                prompt,
                system_prompt=self.prompt_builder.system_prompt,
                max_tokens=max_tokens,
            )

            compositions = []
            for product_data in response_data:
                composition = self._parse_composition_response(product_data, item)
                if composition:
                    compositions.append(composition)

            logger.info(
                "Fill generated %d/%d products for %s",
                len(compositions), num_needed, item.full_id,
            )
            return compositions

        except Exception as e:
            logger.error("Fill generation failed for %s: %s", item.full_id, e)
            return []

    def regenerate_with_feedback(
        self,
        item: TaxonomyItem,
        failures: List[RecordCheckResult],
    ) -> List[ProductComposition]:
        """Regenerate compositions for records that failed reality check.

        Uses the batch generation path but appends correction feedback
        from the reality checker so the LLM avoids the same mistakes.
        """
        if not failures:
            return []

        corrections = []
        for f in failures:
            corrections.append(
                f"- Record {f.record_index}: {f.justification}. "
                f"Fix: {f.improvement_hint}"
            )
        correction_block = (
            "\n\nCORRECTIONS REQUIRED -- the following compositions were "
            "rejected for being unrealistic. Generate replacements that "
            "avoid these issues:\n" + "\n".join(corrections)
        )

        try:
            ctx = self._get_item_context(item)

            prompt = self.prompt_builder.build_batch_prompt(
                product_context=ctx["product_context"],
                materials_grouped=ctx["materials_grouped"],
                category_id=ctx["category_id"],
                category_name=ctx["category_name"],
                subcategory_id=ctx["subcategory_id"],
                subcategory_name=ctx["subcategory_name"],
                weight_min=ctx["weight_min"],
                weight_max=ctx["weight_max"],
                num_products=len(failures),
            )
            prompt += correction_block

            max_tokens = len(failures) * 250 + 500

            response_data = self.api_client.generate_batch_json(
                prompt,
                system_prompt=self.prompt_builder.system_prompt,
                max_tokens=max_tokens,
            )

            compositions = []
            for product_data in response_data:
                composition = self._parse_composition_response(
                    product_data, item
                )
                if composition:
                    compositions.append(composition)

            logger.info(
                "Regenerated %d/%d compositions for %s",
                len(compositions), len(failures), item.full_id,
            )
            return compositions

        except Exception as e:
            logger.error(
                "Regeneration failed for %s: %s", item.full_id, e
            )
            return []

    def generate_batch(
        self,
        items: List[TaxonomyItem],
        products_per_item: int = 1
    ) -> List[ProductComposition]:
        """Generate compositions for a batch of taxonomy items."""
        compositions = []
        for item in items:
            for _ in range(products_per_item):
                composition = self.generate_for_item(item)
                if composition:
                    compositions.append(composition)
        return compositions

    def _parse_composition_response(
        self,
        data: Dict[str, Any],
        item: TaxonomyItem
    ) -> Optional[ProductComposition]:
        """Parse API response into ProductComposition."""
        try:
            materials = data.get("materials", [])
            if not materials:
                logger.warning("No materials in response for %s", item.full_id)
                return None

            weights = data.get("material_weights_kg", [])
            if not weights:
                logger.warning("No weights in response for %s", item.full_id)
                return None

            percentages = data.get("material_percentages", [])
            if not percentages:
                total = sum(weights)
                percentages = [int(w / total * 100) for w in weights]

            total_weight = data.get("total_weight_kg")
            if total_weight is None:
                total_weight = sum(weights)

            # Ensure consistent lengths
            min_len = min(len(materials), len(weights), len(percentages))
            materials = materials[:min_len]
            weights = weights[:min_len]
            percentages = percentages[:min_len]

            # Correct material names to match the reference database
            valid_names = set(self.material_db.get_material_names())
            materials, corrections, uncorrectable = correct_material_list(
                materials, valid_names
            )
            if corrections:
                logger.info(
                    "Corrected materials for %s: %s",
                    item.full_id, "; ".join(corrections),
                )
            if uncorrectable:
                logger.warning(
                    "Uncorrectable materials for %s (dropping): %s",
                    item.full_id, uncorrectable,
                )
                # Remove entries with uncorrectable materials
                uncorrectable_set = set(uncorrectable)
                filtered = [
                    (m, w, p) for m, w, p in zip(materials, weights, percentages)
                    if m not in uncorrectable_set
                ]
                if not filtered:
                    logger.warning(
                        "All materials uncorrectable for %s, skipping",
                        item.full_id,
                    )
                    return None
                materials, weights, percentages = zip(*filtered)
                materials = list(materials)
                weights = list(weights)
                percentages = list(percentages)
                # Rescale percentages to sum to 100
                pct_sum = sum(percentages)
                if pct_sum > 0 and pct_sum != 100:
                    factor = 100.0 / pct_sum
                    percentages = [int(round(p * factor)) for p in percentages]
                    diff = 100 - sum(percentages)
                    if diff != 0:
                        max_idx = percentages.index(max(percentages))
                        percentages[max_idx] += diff
                # Recalculate total weight
                total_weight = sum(weights)

            return ProductComposition(
                category_id=data.get("category_id", item.subcategory_id),
                category_name=data.get("category_name", item.subcategory),
                subcategory_id=data.get("subcategory_id", item.sub_subcategory_id or item.subcategory_id),
                subcategory_name=data.get("subcategory_name", item.sub_subcategory or item.subcategory),
                materials=materials,
                material_weights_kg=[float(w) for w in weights],
                material_percentages=[int(p) for p in percentages],
                total_weight_kg=float(total_weight)
            )

        except Exception as e:
            logger.error("Error parsing composition response: %s", e)
            return None
