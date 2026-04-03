"""
Validator for Layer 2 preprocessing paths.

Validates generated paths before output.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Set

from ..config.config import Layer2Config
from ..models.processing_data import ProcessingStepsDatabase, MaterialProcessCombinations
from ..core.generator import Layer2Record, PreprocessingPath

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Result of validating a preprocessing path."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    corrected_record: Optional[Layer2Record] = None


class PathValidator:
    """Validates and corrects preprocessing paths."""

    def __init__(
        self,
        config: Layer2Config,
        processing_steps_db: ProcessingStepsDatabase,
        material_process_combos: MaterialProcessCombinations
    ):
        self.config = config
        self.processing_steps_db = processing_steps_db
        self.material_process_combos = material_process_combos

        # Cache valid step names (lowercase)
        self._valid_steps: Set[str] = set(
            step.name.lower() for step in processing_steps_db.get_all_steps()
        )

        # Cache category ordering for sequence validation
        self._category_order: Dict[str, int] = {
            cat: idx for idx, cat in enumerate(config.processing_order)
        }

    def _get_category_index(self, step_name: str) -> Optional[int]:
        """Look up the category index for a step name."""
        step = self.processing_steps_db.get_step(step_name)
        if step is None:
            return None
        return self._category_order.get(step.category)

    def _check_sequence_order(self, steps: List[str]) -> List[str]:
        """Check non-decreasing category order. Returns warnings list."""
        warnings = []
        prev_idx, prev_step = -1, None
        for step in steps:
            idx = self._get_category_index(step)
            if idx is None:
                warnings.append(
                    f"Step '{step}' has unknown category, cannot verify order"
                )
                continue
            if idx < prev_idx:
                warnings.append(
                    f"Step '{step}' (order {idx}) appears after "
                    f"'{prev_step}' (order {prev_idx}) -- wrong sequence"
                )
                break
            prev_idx, prev_step = idx, step
        return warnings

    def _sort_steps_by_category(self, steps: List[str]) -> List[str]:
        """Stable-sort steps by category index. Unknown categories go to end."""
        max_order = len(self._category_order)

        def sort_key(step_name: str) -> int:
            idx = self._get_category_index(step_name)
            return idx if idx is not None else max_order

        return sorted(steps, key=sort_key)

    def validate(self, record: Layer2Record) -> ValidationResult:
        """
        Validate a Layer 2 record.

        Checks:
        1. All steps exist in processing_steps_overview.csv
        2. Material-step combinations are valid
        3. Every material has at least one step
        4. No duplicate steps for the same material
        5. Path ID is not empty
        6. Steps follow correct manufacturing sequence order
        """
        errors = []
        warnings = []

        # Check path ID
        if not record.preprocessing_path_id:
            errors.append("Empty preprocessing_path_id")

        # Check steps exist
        invalid_steps = []
        for step in record.preprocessing_steps:
            if step.lower() not in self._valid_steps:
                invalid_steps.append(step)

        if invalid_steps:
            warnings.append(f"Steps not in database: {invalid_steps}")

        # Check step-material mapping
        if not record.step_material_mapping:
            errors.append("Empty step_material_mapping")
        else:
            # Check every material has steps
            for material in record.materials:
                if material not in record.step_material_mapping:
                    # Try case-insensitive match
                    found = False
                    for mapped_mat in record.step_material_mapping.keys():
                        if mapped_mat.lower() == material.lower():
                            found = True
                            break
                    if not found:
                        warnings.append(f"Material '{material}' has no mapped steps")

            # Check for invalid material-step combinations
            invalid_combos = []
            for material, steps in record.step_material_mapping.items():
                for step in steps:
                    if not self.material_process_combos.is_valid_combination(material, step):
                        invalid_combos.append((material, step))

            if invalid_combos:
                warnings.append(f"Invalid material-step combinations: {invalid_combos[:5]}")

            # Check for duplicate steps per material
            for material, steps in record.step_material_mapping.items():
                if len(steps) != len(set(s.lower() for s in steps)):
                    warnings.append(f"Duplicate steps for material '{material}'")

        # Check preprocessing_steps matches mapping
        all_mapped_steps = set()
        for steps in record.step_material_mapping.values():
            all_mapped_steps.update(s.lower() for s in steps)

        unmapped_steps = [s for s in record.preprocessing_steps if s.lower() not in all_mapped_steps]
        if unmapped_steps:
            warnings.append(f"Steps in list but not in mapping: {unmapped_steps}")

        # Check manufacturing sequence order
        warnings.extend(self._check_sequence_order(record.preprocessing_steps))
        for material, steps in record.step_material_mapping.items():
            for w in self._check_sequence_order(steps):
                warnings.append(f"Material '{material}': {w}")

        if errors:
            return ValidationResult(is_valid=False, errors=errors, warnings=warnings)

        return ValidationResult(is_valid=True, errors=[], warnings=warnings)

    def validate_and_correct(self, record: Layer2Record) -> ValidationResult:
        """
        Validate and attempt to correct a record.

        Corrections:
        - Remove invalid steps
        - Remove duplicate steps
        - Ensure all materials have at least one step
        """
        result = self.validate(record)

        if result.is_valid and not result.warnings:
            result.corrected_record = record
            return result

        # Attempt corrections
        corrected = self._correct_record(record)

        # Re-validate
        revalidation = self.validate(corrected)
        revalidation.corrected_record = corrected

        return revalidation

    def _find_category_replacement(
        self, step_name: str, material: str, seen: Set[str]
    ) -> Optional[str]:
        """Find a same-category replacement step valid for the material."""
        original = self.processing_steps_db.get_step(step_name)
        if not original:
            return None
        for c in self.processing_steps_db.get_steps_by_category(original.category):
            if c.name.lower() not in seen and \
               self.material_process_combos.is_valid_combination(material, c.name):
                return c.name
        return None

    def _correct_record(self, record: Layer2Record) -> Layer2Record:
        """Apply corrections to a record."""
        # Filter to valid steps only
        valid_steps = []
        for step in record.preprocessing_steps:
            if step.lower() in self._valid_steps:
                valid_steps.append(step)

        # Correct step-material mapping
        corrected_mapping = {}
        for material in record.materials:
            # Find the mapping key (case-insensitive)
            mapped_steps = []
            for mapped_mat, steps in record.step_material_mapping.items():
                if mapped_mat.lower() == material.lower():
                    seen = set()
                    for step in steps:
                        step_lower = step.lower()
                        if step_lower in seen:
                            continue
                        if step_lower in self._valid_steps and \
                           self.material_process_combos.is_valid_combination(material, step):
                            mapped_steps.append(step)
                            seen.add(step_lower)
                        else:
                            # Replace with same-category step to preserve path shape
                            alt = self._find_category_replacement(step, material, seen)
                            if alt:
                                mapped_steps.append(alt)
                                seen.add(alt.lower())
                    break

            # If no valid steps found, pick from steps known to be valid for this material
            if not mapped_steps:
                valid_for_material = self.material_process_combos.get_valid_steps_for_material(material)
                if valid_for_material:
                    mapped_steps = valid_for_material[:3]
                else:
                    # Last resort: pick the first valid step from the original record
                    fallback = [s for s in record.preprocessing_steps if s.lower() in self._valid_steps]
                    mapped_steps = fallback[:3] if fallback else ["Finishing"]

            corrected_mapping[material] = self._sort_steps_by_category(mapped_steps)

        # Update preprocessing_steps to match mapping
        all_steps = set()
        for steps in corrected_mapping.values():
            all_steps.update(steps)
        corrected_steps = [s for s in valid_steps if s in all_steps]
        # Add any mapped steps not in the list
        for step in all_steps:
            if step not in corrected_steps:
                corrected_steps.append(step)
        corrected_steps = self._sort_steps_by_category(corrected_steps)

        return Layer2Record(
            category_id=record.category_id,
            category_name=record.category_name,
            subcategory_id=record.subcategory_id,
            subcategory_name=record.subcategory_name,
            materials=record.materials,
            material_weights_kg=record.material_weights_kg,
            material_percentages=record.material_percentages,
            total_weight_kg=record.total_weight_kg,
            preprocessing_path_id=record.preprocessing_path_id,
            preprocessing_steps=corrected_steps,
            step_material_mapping=corrected_mapping
        )

    def batch_validate(
        self,
        records: List[Layer2Record],
        correct: bool = True
    ) -> Tuple[List[Layer2Record], List[Tuple[Layer2Record, ValidationResult]]]:
        """
        Validate a batch of records.

        Returns:
            Tuple of (valid_records, invalid_records_with_results)
        """
        valid = []
        invalid = []

        for record in records:
            if correct:
                result = self.validate_and_correct(record)
                if result.is_valid and result.corrected_record:
                    valid.append(result.corrected_record)
                else:
                    invalid.append((record, result))
            else:
                result = self.validate(record)
                if result.is_valid:
                    valid.append(record)
                else:
                    invalid.append((record, result))

        return valid, invalid
