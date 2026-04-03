"""
Preprocessing Path Generator for Layer 2.

Generates realistic preprocessing pathways for product compositions.
Optimized for batch processing with deduplication.
"""

import json
import logging
import random
import hashlib
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

from ..config.config import Layer2Config
from ..models.processing_data import ProcessingStepsDatabase, MaterialProcessCombinations
from ..io.layer1_reader import Layer1Record
from ..clients.api_client import Layer2Client
from ..prompts.prompts import PromptBuilder
from data.data_generation.shared.api_client import APIError
from data.data_generation.shared.reality_check_models import RecordCheckResult

logger = logging.getLogger(__name__)


@dataclass
class PreprocessingPath:
    """Represents a single preprocessing pathway."""
    preprocessing_path_id: str
    preprocessing_steps: List[str]
    step_material_mapping: Dict[str, List[str]]
    reasoning: str = ""
    path_hash: str = ""  # Hash for deduplication

    def __post_init__(self):
        """Generate hash for deduplication if not provided."""
        if not self.path_hash:
            # Hash steps AND material mapping so paths with identical steps
            # but different material assignments are kept as distinct variants.
            steps_str = "|".join(self.preprocessing_steps)
            mapping_str = json.dumps(self.step_material_mapping, sort_keys=True)
            combined = f"{steps_str}||{mapping_str}"
            self.path_hash = hashlib.md5(combined.encode()).hexdigest()[:12]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "preprocessing_path_id": self.preprocessing_path_id,
            "preprocessing_steps": json.dumps(self.preprocessing_steps),
            "step_material_mapping": json.dumps(self.step_material_mapping),
        }


@dataclass
class Layer2Record:
    """Represents a complete Layer 2 output record."""
    # From Layer 1
    category_id: str
    category_name: str
    subcategory_id: str
    subcategory_name: str
    materials: List[str]
    material_weights_kg: List[float]
    material_percentages: List[int]
    total_weight_kg: float

    # Layer 2 additions
    preprocessing_path_id: str
    preprocessing_steps: List[str]
    step_material_mapping: Dict[str, List[str]]

    @classmethod
    def from_layer1_and_path(
        cls,
        layer1_record: Layer1Record,
        path: PreprocessingPath
    ) -> "Layer2Record":
        """Create from Layer 1 record and preprocessing path."""
        return cls(
            category_id=layer1_record.category_id,
            category_name=layer1_record.category_name,
            subcategory_id=layer1_record.subcategory_id,
            subcategory_name=layer1_record.subcategory_name,
            materials=layer1_record.materials,
            material_weights_kg=layer1_record.material_weights_kg,
            material_percentages=layer1_record.material_percentages,
            total_weight_kg=layer1_record.total_weight_kg,
            preprocessing_path_id=path.preprocessing_path_id,
            preprocessing_steps=path.preprocessing_steps,
            step_material_mapping=path.step_material_mapping
        )

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
            "total_weight_kg": self.total_weight_kg,
            "preprocessing_path_id": self.preprocessing_path_id,
            "preprocessing_steps": json.dumps(self.preprocessing_steps),
            "step_material_mapping": json.dumps(self.step_material_mapping)
        }


class PreprocessingPathGenerator:
    """
    Generates preprocessing pathways.

    Uses the full material-process combinations dataset in context
    to generate realistic pathways with deduplication.
    """

    def __init__(
        self,
        config: Layer2Config,
        processing_steps_db: ProcessingStepsDatabase,
        material_process_combos: MaterialProcessCombinations,
        api_client: Layer2Client
    ):
        self.config = config
        self.processing_steps_db = processing_steps_db
        self.material_process_combos = material_process_combos
        self.api_client = api_client
        self.prompt_builder = PromptBuilder()

        # Pre-format data for prompts (done once)
        self._processing_steps_text = self.processing_steps_db.format_for_prompt()
        self._combinations_text = self.material_process_combos.format_compact_for_prompt()

        # Counter-based path ID generation (O(1) memory, thread-safe).
        # Sequential IDs (pp-000001, pp-000002, ...) are guaranteed unique
        # by the monotonically increasing counter -- no set needed.
        self._path_counter = 0
        self._id_lock = threading.Lock()

        # Statistics
        self._total_generated = 0

    def generate_paths_for_record(
        self,
        record: Layer1Record,
        num_paths: Optional[int] = None
    ) -> List[PreprocessingPath]:
        """
        Generate preprocessing paths for a Layer 1 record.

        Args:
            record: Layer 1 product composition record
            num_paths: Number of paths to generate (default: from config)

        Returns:
            List of unique preprocessing paths
        """
        if num_paths is None:
            num_paths = self.config.paths_per_product

        try:
            # Build prompt with full context
            prompt = self.prompt_builder.build_generation_prompt(
                category_id=record.category_id,
                category_name=record.category_name,
                subcategory_id=record.subcategory_id,
                subcategory_name=record.subcategory_name,
                total_weight_kg=record.total_weight_kg,
                materials_with_weights=record.format_materials_with_weights(),
                processing_steps=self._processing_steps_text,
                material_process_combinations=self._get_relevant_combinations(record.materials),
                num_paths=num_paths
            )

            # Generate via API using enhanced method for Qwen3 thinking models
            response_data = self.api_client.generate_preprocessing_paths(prompt, num_paths)

            # Parse response
            paths = self._parse_paths_response(response_data, record)

            return paths

        except APIError as e:
            logger.error(f"API error generating paths for record {record._row_index}: {e}")
            return []
        except Exception as e:
            logger.error(f"Error generating paths for record {record._row_index}: {e}")
            return []

    def _get_relevant_combinations(self, materials: List[str]) -> str:
        """Get combinations text filtered to relevant materials."""
        # For efficiency, filter to only materials in this product
        relevant_lines = ["material_name,processing_step,emission_factor_kgCO2e_per_kg"]
        
        # Stop words that are too generic to trigger a match on their own
        # This prevents "fibre" matching everything labeled as "fibre"
        STOP_WORDS = {
            "fibre", "fiber", "textile", "yarn", "conventional", 
            "organic", "at farm gate", "at storehouse", "finished product",
            "part", "material", "raw", "recyclable", "generic"
        }

        for combo in self.material_process_combos.combinations:
            mat_lower = combo.material_name.lower()
            
            # Prepare tokens for the combination material in DB
            # Replace commas with spaces to handle "wool, conventional"
            combo_tokens = set(t.strip() for t in mat_lower.replace(',', ' ').split())
            combo_keys = {t for t in combo_tokens if t not in STOP_WORDS}
            
            is_match = False
            for material in materials:
                mat_input_lower = material.lower()
                
                # 1. Direct substring match (existing logic - good for exact phrases)
                if mat_input_lower in mat_lower or mat_lower in mat_input_lower:
                    is_match = True
                    break
                    
                # 2. Key token match (new logic - handles naming variations)
                # Split input into tokens
                input_tokens = set(t.strip() for t in mat_input_lower.replace(',', ' ').split())
                input_keys = {t for t in input_tokens if t not in STOP_WORDS}
                
                # If we have shared key tokens (e.g. "wool", "silk", "polyurethane"), it's a match
                # Ensure we have at least one meaningful key
                if input_keys and combo_keys and not input_keys.isdisjoint(combo_keys):
                    is_match = True
                    break
            
            if is_match:
                relevant_lines.append(
                    f"{combo.material_name},{combo.processing_step},{combo.emission_factor:.3f}"
                )

        # If too few matches, include full dataset
        if len(relevant_lines) < 50:
            return self._combinations_text

        return "\n".join(relevant_lines)

    def _parse_paths_response(
        self,
        data: Any,
        record: Layer1Record
    ) -> List[PreprocessingPath]:
        """Parse API response into PreprocessingPath objects with deduplication."""
        paths = []
        local_hashes = set()  # Track hashes within this record for local dedup

        # Handle both list and single dict responses
        if isinstance(data, dict):
            data = [data]

        if not isinstance(data, list):
            logger.warning(f"Unexpected response type: {type(data)}")
            return []

        for item in data:
            try:
                self._total_generated += 1

                # Skip non-dict items (API sometimes returns strings instead of objects)
                if not isinstance(item, dict):
                    logger.debug(f"Skipping non-dict item of type {type(item).__name__}: {str(item)[:100]}")
                    continue

                # Always assign a sequential ID. LLM-suggested IDs are
                # ignored because validating them would require storing
                # all past IDs (O(n) memory). Counter-based IDs are
                # guaranteed unique in O(1) space.
                with self._id_lock:
                    path_id = self._generate_unique_path_id()

                steps = item.get("preprocessing_steps", [])
                if not steps:
                    continue

                mapping = item.get("step_material_mapping", {})
                if not mapping:
                    # Generate default mapping if not provided
                    mapping = self._generate_default_mapping(record.materials, steps)

                reasoning = item.get("reasoning", "")

                path = PreprocessingPath(
                    preprocessing_path_id=path_id,
                    preprocessing_steps=steps,
                    step_material_mapping=mapping,
                    reasoning=reasoning
                )

                # Local deduplication only (within same product)
                if path.path_hash in local_hashes:
                    logger.debug(f"Rejected duplicate path (local): {path.path_hash}")
                    continue

                local_hashes.add(path.path_hash)
                paths.append(path)

            except Exception as e:
                logger.warning(f"Failed to parse path item: {e}")
                continue

        return paths

    def _generate_unique_path_id(self) -> str:
        """Generate a unique path ID. Caller must hold self._id_lock.

        Uses a monotonically increasing counter, so every ID is unique
        without needing to store previously issued IDs (O(1) memory).
        """
        self._path_counter += 1
        return f"pp-{self._path_counter:06d}"

    def _generate_default_mapping(
        self,
        materials: List[str],
        steps: List[str]
    ) -> Dict[str, List[str]]:
        """Generate a default step-material mapping."""
        mapping = {}

        for material in materials:
            # Find valid steps for this material
            valid_steps = self.material_process_combos.get_valid_steps_for_material(material)
            valid_steps_lower = [s.lower() for s in valid_steps]

            # Map steps that are valid for this material
            material_steps = []
            for step in steps:
                if step.lower() in valid_steps_lower:
                    material_steps.append(step)

            if material_steps:
                mapping[material] = material_steps
            else:
                # Fallback: find valid steps for this material from the DB
                db_valid = self.material_process_combos.get_valid_steps_for_material(material)
                if db_valid:
                    mapping[material] = db_valid[:3]
                else:
                    mapping[material] = steps[:3] if len(steps) > 3 else steps

        return mapping

    def regenerate_with_feedback(
        self,
        l1_record: Layer1Record,
        failures: List[RecordCheckResult],
    ) -> List[Layer2Record]:
        """Regenerate Layer 2 records that failed reality check.

        Appends correction feedback to the generation prompt so the LLM
        avoids the same mistakes.
        """
        if not failures:
            return []

        corrections = []
        for f in failures:
            corrections.append(
                f"- {f.justification}. Fix: {f.improvement_hint}"
            )
        correction_block = (
            "\n\nCORRECTIONS REQUIRED -- the following preprocessing paths "
            "were rejected for being unrealistic. Generate replacements that "
            "avoid these issues:\n" + "\n".join(corrections)
        )

        try:
            prompt = self.prompt_builder.build_generation_prompt(
                category_id=l1_record.category_id,
                category_name=l1_record.category_name,
                subcategory_id=l1_record.subcategory_id,
                subcategory_name=l1_record.subcategory_name,
                total_weight_kg=l1_record.total_weight_kg,
                materials_with_weights=l1_record.format_materials_with_weights(),
                processing_steps=self._processing_steps_text,
                material_process_combinations=self._get_relevant_combinations(
                    l1_record.materials
                ),
                num_paths=len(failures),
            )
            prompt += correction_block

            response_data = self.api_client.generate_preprocessing_paths(
                prompt, len(failures)
            )

            paths = self._parse_paths_response(response_data, l1_record)
            layer2_records = [
                Layer2Record.from_layer1_and_path(l1_record, path)
                for path in paths
            ]

            logger.info(
                "Regenerated %d/%d paths for record %s",
                len(layer2_records), len(failures),
                l1_record.subcategory_id,
            )
            return layer2_records

        except Exception as e:
            logger.error(
                "Regeneration failed for %s: %s",
                l1_record.subcategory_id, e,
            )
            return []

    def generate_layer2_records(
        self,
        record: Layer1Record,
        num_paths: Optional[int] = None
    ) -> List[Layer2Record]:
        """
        Generate complete Layer 2 records for a Layer 1 record.

        Each Layer 1 record expands into multiple Layer 2 records.
        """
        paths = self.generate_paths_for_record(record, num_paths)

        layer2_records = []
        for path in paths:
            layer2_record = Layer2Record.from_layer1_and_path(record, path)
            layer2_records.append(layer2_record)

        return layer2_records

    def get_deduplication_stats(self) -> Dict[str, Any]:
        """Get statistics about generation."""
        return {
            "total_generated": self._total_generated,
            "unique_path_ids": self._path_counter,
        }
