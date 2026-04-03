"""
Validator for Layer 1 product compositions.

Validates generated compositions before output.
"""

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from ..config.config import Layer1Config
from ..models.materials import MaterialDatabase
from .generator import ProductComposition

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of validating a composition."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    normalized_composition: Optional[ProductComposition] = None


class CompositionValidator:
    """Validates and normalizes product compositions."""

    def __init__(self, config: Layer1Config, material_db: MaterialDatabase):
        self.config = config
        self.material_db = material_db

    def validate(self, composition: ProductComposition) -> ValidationResult:
        """
        Validate a product composition.

        Checks:
        1. Material existence in database
        2. Weight sum consistency
        3. Percentage sum (should equal 100)
        4. Array length consistency
        5. Positive values
        6. Realistic weight range
        """
        errors = []
        warnings = []

        # Check array lengths match
        n_materials = len(composition.materials)
        n_weights = len(composition.material_weights_kg)
        n_percentages = len(composition.material_percentages)

        if not (n_materials == n_weights == n_percentages):
            errors.append(
                f"Array length mismatch: materials={n_materials}, "
                f"weights={n_weights}, percentages={n_percentages}"
            )
            return ValidationResult(is_valid=False, errors=errors)

        if n_materials == 0:
            errors.append("No materials in composition")
            return ValidationResult(is_valid=False, errors=errors)

        # Check material existence (strict: reject invalid materials)
        invalid_materials = []
        for material in composition.materials:
            if not self.material_db.validate_material(material):
                invalid_materials.append(material)

        if invalid_materials:
            errors.append(f"Materials not in database: {invalid_materials}")

        # Check positive values
        for i, weight in enumerate(composition.material_weights_kg):
            if weight <= 0:
                errors.append(f"Non-positive weight for material {i}: {weight}")

        for i, pct in enumerate(composition.material_percentages):
            if pct <= 0:
                errors.append(f"Non-positive percentage for material {i}: {pct}")

        if composition.total_weight_kg <= 0:
            errors.append(f"Non-positive total weight: {composition.total_weight_kg}")

        # Check weight sum
        weight_sum = sum(composition.material_weights_kg)
        weight_diff = abs(weight_sum - composition.total_weight_kg)
        if weight_diff > 0.01:
            warnings.append(
                f"Weight sum mismatch: sum={weight_sum:.3f}, "
                f"total={composition.total_weight_kg:.3f}"
            )

        # Check percentage sum
        pct_sum = sum(composition.material_percentages)
        if abs(pct_sum - 100) > 1:
            warnings.append(f"Percentage sum is {pct_sum}, expected 100")

        # Check realistic weight range
        weight_min, weight_max = self.config.get_weight_range(composition.category_id)
        if composition.total_weight_kg < weight_min * 0.5:
            warnings.append(
                f"Weight {composition.total_weight_kg:.2f} kg is below expected "
                f"minimum {weight_min:.2f} kg for category {composition.category_id}"
            )
        elif composition.total_weight_kg > weight_max * 1.5:
            warnings.append(
                f"Weight {composition.total_weight_kg:.2f} kg is above expected "
                f"maximum {weight_max:.2f} kg for category {composition.category_id}"
            )

        if errors:
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        return ValidationResult(is_valid=True, errors=[], warnings=warnings)

    def validate_and_normalize(
        self,
        composition: ProductComposition
    ) -> ValidationResult:
        """
        Validate and normalize a composition.

        Attempts to fix minor issues like:
        - Weight sum mismatches
        - Percentage sum != 100
        """
        # First validate
        result = self.validate(composition)

        if not result.is_valid:
            return result

        # Attempt normalization
        normalized = self._normalize(composition)

        # Re-validate
        revalidation = self.validate(normalized)
        revalidation.normalized_composition = normalized

        return revalidation

    def _normalize(self, composition: ProductComposition) -> ProductComposition:
        """Normalize a composition to fix minor issues."""
        # Normalize percentages to sum to 100
        pct_sum = sum(composition.material_percentages)
        if pct_sum != 100 and pct_sum > 0:
            factor = 100 / pct_sum
            normalized_pcts = [int(round(p * factor)) for p in composition.material_percentages]

            # Adjust for rounding errors
            diff = 100 - sum(normalized_pcts)
            if diff != 0:
                # Add/subtract from largest percentage
                max_idx = normalized_pcts.index(max(normalized_pcts))
                normalized_pcts[max_idx] += diff
        else:
            normalized_pcts = composition.material_percentages

        # Normalize weights to match total
        weight_sum = sum(composition.material_weights_kg)
        if abs(weight_sum - composition.total_weight_kg) > 0.001 and weight_sum > 0:
            factor = composition.total_weight_kg / weight_sum
            normalized_weights = [round(w * factor, 4) for w in composition.material_weights_kg]
        else:
            normalized_weights = composition.material_weights_kg

        return ProductComposition(
            category_id=composition.category_id,
            category_name=composition.category_name,
            subcategory_id=composition.subcategory_id,
            subcategory_name=composition.subcategory_name,
            materials=composition.materials,
            material_weights_kg=normalized_weights,
            material_percentages=normalized_pcts,
            total_weight_kg=composition.total_weight_kg
        )

    def batch_validate(
        self,
        compositions: List[ProductComposition],
        normalize: bool = True
    ) -> Tuple[List[ProductComposition], List[Tuple[ProductComposition, ValidationResult]]]:
        """
        Validate a batch of compositions.

        Returns:
            Tuple of (valid_compositions, invalid_compositions_with_results)
        """
        valid = []
        invalid = []

        for comp in compositions:
            if normalize:
                result = self.validate_and_normalize(comp)
                if result.is_valid and result.normalized_composition:
                    valid.append(result.normalized_composition)
                else:
                    invalid.append((comp, result))
            else:
                result = self.validate(comp)
                if result.is_valid:
                    valid.append(comp)
                else:
                    invalid.append((comp, result))

        return valid, invalid
