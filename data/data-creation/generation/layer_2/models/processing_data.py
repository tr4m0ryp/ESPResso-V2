"""
Processing steps and material-process combinations data loader for Layer 2.

Loads and manages:
- processing_steps.parquet (41 processing steps)
- material_processing_combinations.parquet (3,084 valid combinations)
"""

import pandas as pd
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class ProcessingStep:
    """Represents a single processing step."""
    name: str
    process_id: str
    category: str
    emission_factor: float
    applicable_materials: List[str]
    description: str
    data_quality: str
    reference_unit: str
    data_source: str

    @property
    def display_name(self) -> str:
        return self.name.strip()


@dataclass
class MaterialProcessCombination:
    """Represents a valid material-process combination."""
    material_name: str
    material_id: str
    material_type: str
    material_category: str
    processing_step: str
    process_id: str
    process_description: str
    reference_mass_kg: float
    emission_factor: float
    calculated_cf: float
    data_quality: str
    base_material_cf: float
    notes: str


class ProcessingStepsDatabase:
    """Database of processing steps loaded from processing_steps_overview.csv."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.steps: Dict[str, ProcessingStep] = {}
        self.steps_by_category: Dict[str, List[ProcessingStep]] = {}
        self._load_steps()

    def _load_steps(self) -> None:
        """Load processing steps from Parquet file."""
        df = pd.read_parquet(self.csv_path)
        for _, row in df.iterrows():
            name = str(row.get('process_name', '')).strip()
            if not name:
                continue

            applicable_str = str(row.get('applies_to', ''))
            applicable_materials = [
                m.strip() for m in applicable_str.split(',')
                if m.strip()
            ]

            try:
                ef = float(row.get('carbon_footprint_kgCO2e_per_kg', 0))
            except (ValueError, TypeError):
                ef = 0.0

            step = ProcessingStep(
                name=name,
                process_id=str(row.get('ref_id', '')).strip(),
                category=str(row.get('category', '')).strip(),
                emission_factor=ef,
                applicable_materials=applicable_materials,
                description=str(row.get('description', '')).strip(),
                data_quality=str(row.get('step_type', '')).strip(),
                reference_unit='kg',
                data_source='ecoinvent'
            )

            self.steps[name.lower()] = step

            category = step.category
            if category not in self.steps_by_category:
                self.steps_by_category[category] = []
            self.steps_by_category[category].append(step)

    def get_step(self, name: str) -> Optional[ProcessingStep]:
        """Get a processing step by name (case-insensitive)."""
        return self.steps.get(name.lower())

    def get_all_steps(self) -> List[ProcessingStep]:
        """Get all processing steps."""
        return list(self.steps.values())

    def get_steps_by_category(self, category: str) -> List[ProcessingStep]:
        """Get all steps in a category."""
        return self.steps_by_category.get(category, [])

    def get_categories(self) -> List[str]:
        """Get all category names."""
        return list(self.steps_by_category.keys())

    def validate_step(self, name: str) -> bool:
        """Check if a step name exists."""
        return name.lower() in self.steps

    def get_steps_for_material(self, material_name: str) -> List[ProcessingStep]:
        """Get all steps applicable to a material."""
        material_lower = material_name.lower()
        result = []
        for step in self.steps.values():
            for applicable in step.applicable_materials:
                if applicable.lower() in material_lower or material_lower in applicable.lower():
                    result.append(step)
                    break
        return result

    def format_for_prompt(self) -> str:
        """Format all steps for inclusion in prompt."""
        lines = []
        for category, steps in self.steps_by_category.items():
            lines.append(f"\n{category}:")
            for step in steps:
                lines.append(f"  - {step.name} (EF: {step.emission_factor:.2f} kg CO2e/kg)")
        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self.steps)


class MaterialProcessCombinations:
    """Database of valid material-process combinations from material_processing_emissions.csv."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self.combinations: List[MaterialProcessCombination] = []
        self._by_material: Dict[str, List[MaterialProcessCombination]] = {}
        self._by_step: Dict[str, List[MaterialProcessCombination]] = {}
        self._valid_pairs: Set[Tuple[str, str]] = set()
        self._load_combinations()

    def _load_combinations(self) -> None:
        """Load combinations from Parquet file."""
        df = pd.read_parquet(self.csv_path)
        for _, row in df.iterrows():
            material_name = str(row.get('material_name', '')).strip()
            processing_step = str(row.get('process_name', '')).strip()

            if not material_name or not processing_step:
                continue

            try:
                calc_cf = float(row.get('combined_cf_kgCO2e_per_kg', 0))
            except (ValueError, TypeError):
                calc_cf = 0.0

            combo = MaterialProcessCombination(
                material_name=material_name,
                material_id=str(row.get('material_ref_id', '')).strip(),
                material_type=str(row.get('material_category', '')).strip(),
                material_category=str(row.get('material_category', '')).strip(),
                processing_step=processing_step,
                process_id=str(row.get('process_ref_id', '')).strip(),
                process_description=str(row.get('process_category', '')).strip(),
                reference_mass_kg=1.0,
                emission_factor=calc_cf,
                calculated_cf=calc_cf,
                data_quality=str(row.get('material_source', '')).strip(),
                base_material_cf=0.0,
                notes=''
            )

            self.combinations.append(combo)

            # Index by material
            mat_key = material_name.lower()
            if mat_key not in self._by_material:
                self._by_material[mat_key] = []
            self._by_material[mat_key].append(combo)

            # Index by step
            step_key = processing_step.lower()
            if step_key not in self._by_step:
                self._by_step[step_key] = []
            self._by_step[step_key].append(combo)

            # Track valid pairs
            self._valid_pairs.add((mat_key, step_key))

        # Build prefix index for fast partial-match lookups in is_valid_combination.
        # Maps each step to the set of material keys it pairs with.
        self._step_to_materials: Dict[str, Set[str]] = {}
        for mat_key, step_key in self._valid_pairs:
            if step_key not in self._step_to_materials:
                self._step_to_materials[step_key] = set()
            self._step_to_materials[step_key].add(mat_key)

    def get_combinations_for_material(self, material_name: str) -> List[MaterialProcessCombination]:
        """Get all valid combinations for a material."""
        # Try exact match first
        mat_key = material_name.lower()
        if mat_key in self._by_material:
            return self._by_material[mat_key]

        # Stop words for token matching
        STOP_WORDS = {
            "fibre", "fiber", "textile", "yarn", "conventional", 
            "organic", "at farm gate", "at storehouse", "finished product",
            "part", "material", "raw", "recyclable", "generic"
        }

        # Try partial and token match
        result = []
        input_tokens = set(t.strip() for t in mat_key.replace(',', ' ').split())
        input_keys = {t for t in input_tokens if t not in STOP_WORDS}

        for key, combos in self._by_material.items():
            # 1. Substring match
            if mat_key in key or key in mat_key:
                result.extend(combos)
                continue

            # 2. Token match
            key_tokens = set(t.strip() for t in key.replace(',', ' ').split())
            key_keys = {t for t in key_tokens if t not in STOP_WORDS}

            if input_keys and key_keys and not input_keys.isdisjoint(key_keys):
                result.extend(combos)
                
        return result

    def get_valid_steps_for_material(self, material_name: str) -> List[str]:
        """Get list of valid processing step names for a material."""
        combos = self.get_combinations_for_material(material_name)
        return list(set(c.processing_step for c in combos))

    def is_valid_combination(self, material_name: str, step_name: str) -> bool:
        """Check if a material-step combination is valid."""
        mat_key = material_name.lower()
        step_key = step_name.lower()

        # Direct match (O(1))
        if (mat_key, step_key) in self._valid_pairs:
            return True

        # Partial match using prefix index: only scan materials paired with this step
        candidate_mats = self._step_to_materials.get(step_key)
        if candidate_mats:
            for m in candidate_mats:
                if mat_key in m or m in mat_key:
                    return True

        return False

    def get_emission_factor(self, material_name: str, step_name: str) -> Optional[float]:
        """Get emission factor for a material-step combination."""
        combos = self.get_combinations_for_material(material_name)
        step_lower = step_name.lower()
        for combo in combos:
            if combo.processing_step.lower() == step_lower:
                return combo.emission_factor
        return None

    def get_unique_materials(self) -> List[str]:
        """Get list of unique material names."""
        return list(self._by_material.keys())

    def get_unique_steps(self) -> List[str]:
        """Get list of unique step names."""
        return list(self._by_step.keys())

    def format_for_prompt(self, materials: Optional[List[str]] = None) -> str:
        """
        Format combinations for inclusion in prompt.

        If materials specified, only include combinations for those materials.
        """
        if materials:
            # Filter to specific materials
            relevant_combos = []
            for mat in materials:
                relevant_combos.extend(self.get_combinations_for_material(mat))
        else:
            relevant_combos = self.combinations

        # Group by material for readability
        by_material: Dict[str, List[str]] = {}
        for combo in relevant_combos:
            mat = combo.material_name
            if mat not in by_material:
                by_material[mat] = []
            step_info = f"{combo.processing_step} (EF: {combo.emission_factor:.2f})"
            if step_info not in by_material[mat]:
                by_material[mat].append(step_info)

        lines = []
        for mat, steps in by_material.items():
            lines.append(f"\n{mat}:")
            for step in steps:
                lines.append(f"  - {step}")

        return "\n".join(lines)

    def format_compact_for_prompt(self) -> str:
        """Format as compact CSV-like structure for large context."""
        lines = ["material_name,processing_step,emission_factor_kgCO2e_per_kg"]
        for combo in self.combinations:
            lines.append(f"{combo.material_name},{combo.processing_step},{combo.emission_factor:.3f}")
        return "\n".join(lines)

    def __len__(self) -> int:
        return len(self.combinations)
