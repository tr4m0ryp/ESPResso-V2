"""Carbon Footprint Calculator for Layer 6.

Loads reference databases from Parquet and computes per-record
carbon footprints by delegating to the components module.

Primary classes:
    CarbonFootprintCalculator -- Main calculator.

Dependencies:
    pandas, databases, components, material_aliases, transport_model.
"""

import json
import logging
from typing import Dict, List, Optional, Any

import pandas as pd

from data.data_generation.layer_6.config.config import Layer6Config
from data.data_generation.layer_6.core.databases import (
    MaterialDatabase, ProcessingDatabase, CalculationResult
)
from data.data_generation.layer_6.core.material_aliases import (
    validate_aliases
)
from data.data_generation.layer_6.core.transport_model import (
    TransportModeModel
)
from data.data_generation.layer_6.core import components

logger = logging.getLogger(__name__)


class CarbonFootprintCalculator:
    """Main calculator: loads databases, delegates to components."""

    def __init__(self, config: Layer6Config):
        """Initialize calculator with configuration."""
        self.config = config
        self.transport_model = TransportModeModel()
        self.material_db: Optional[MaterialDatabase] = None
        self.processing_db: Optional[ProcessingDatabase] = None
        self.step_ef_lookup: Dict[str, float] = {}
        self.packaging_ef = config.packaging_ef

        self.records_processed = 0
        self.records_with_warnings = 0
        self._match_stats = {'matches': 0, 'misses': 0}

    def load_databases(self) -> bool:
        """Load reference databases from Parquet files."""
        try:
            self.material_db = MaterialDatabase()
            df = pd.read_parquet(self.config.materials_db_path)

            for _, row in df.iterrows():
                name = str(row.get('material_name', ''))
                try:
                    ef = float(row.get(
                        'carbon_footprint_kgCO2e_per_kg', 0
                    ))
                    self.material_db.materials[name] = ef
                    ref_id = str(row.get('ref_id', ''))
                    if ref_id:
                        self.material_db.material_refs[name] = ref_id
                except (ValueError, TypeError):
                    continue

            logger.info(
                "Loaded %d materials from %s",
                len(self.material_db.materials),
                self.config.materials_db_path
            )

            ref_names = set(self.material_db.materials.keys())
            errors = validate_aliases(ref_names)
            if errors:
                logger.warning(
                    "%d alias targets missing from reference DB",
                    len(errors)
                )
        except Exception as e:
            logger.error("Failed to load materials database: %s", e)
            return False

        try:
            self.processing_db = ProcessingDatabase()
            df = pd.read_parquet(self.config.processing_db_path)

            for _, row in df.iterrows():
                material = str(row.get('material_name', ''))
                process = str(row.get('process_name', ''))
                try:
                    ef = float(row.get(
                        'combined_cf_kgCO2e_per_kg', 0
                    ))
                    self.processing_db.combinations[
                        (material, process)
                    ] = ef
                except (ValueError, TypeError):
                    continue

            logger.info(
                "Loaded %d material-process combinations from %s",
                len(self.processing_db.combinations),
                self.config.processing_db_path
            )
        except Exception as e:
            logger.error(
                "Failed to load processing database: %s", e
            )
            return False

        try:
            steps_df = pd.read_parquet(
                self.config.processing_steps_path
            )
            for _, row in steps_df.iterrows():
                name = str(row.get('process_name', ''))
                try:
                    ef = float(row.get(
                        'carbon_footprint_kgCO2e_per_kg', 0
                    ))
                    self.step_ef_lookup[name] = ef
                except (ValueError, TypeError):
                    continue
            logger.info(
                "Loaded %d step EFs from %s",
                len(self.step_ef_lookup),
                self.config.processing_steps_path
            )
        except FileNotFoundError:
            logger.warning(
                "Processing steps file not found: %s, "
                "step-level fallback disabled",
                self.config.processing_steps_path
            )
        except Exception as e:
            logger.warning(
                "Failed to load processing steps: %s", e
            )

        return True

    def _parse_json_array(self, value: Any) -> List[Any]:
        """Parse JSON array from string or pass through list."""
        if isinstance(value, list):
            return value
        if not value or value == '':
            return []
        try:
            return json.loads(str(value))
        except json.JSONDecodeError:
            try:
                import ast
                return ast.literal_eval(str(value))
            except (ValueError, SyntaxError):
                return []

    def calculate_record(
        self,
        record: Dict[str, Any]
    ) -> CalculationResult:
        """Calculate complete carbon footprint for a single record."""
        result = CalculationResult()

        materials = self._parse_json_array(
            record.get('materials', '[]')
        )
        material_weights = self._parse_json_array(
            record.get('material_weights_kg', '[]')
        )
        preprocessing_steps = self._parse_json_array(
            record.get('preprocessing_steps', '[]')
        )
        packaging_categories = self._parse_json_array(
            record.get('packaging_categories', '[]')
        )
        packaging_masses = self._parse_json_array(
            record.get('packaging_masses_kg', '[]')
        )

        total_weight = float(record.get('total_weight_kg', 0.0))
        transport_distance = float(
            record.get('total_transport_distance_km', 0.0)
        )

        try:
            material_weights = [float(w) for w in material_weights]
            packaging_masses = [float(m) for m in packaging_masses]
        except (TypeError, ValueError) as e:
            result.calculation_notes.append(
                f"Weight conversion error: {e}"
            )
            result.is_valid = False
            return result

        # 1. Raw materials
        cf_raw, raw_notes = components.calculate_raw_materials(
            materials, material_weights,
            self.material_db, self._match_stats
        )
        result.cf_raw_materials_kg_co2e = cf_raw
        result.calculation_notes.extend(raw_notes)

        # 2. Transport
        cf_transport, mode_probs, weighted_ef = (
            components.calculate_transport(
                total_weight, transport_distance,
                self.transport_model
            )
        )
        result.cf_transport_kg_co2e = cf_transport
        result.transport_mode_probabilities = mode_probs
        result.weighted_ef_g_co2e_tkm = weighted_ef

        # 3. Processing
        cf_processing, proc_notes = (
            components.calculate_processing(
                materials, material_weights, preprocessing_steps,
                self.material_db, self.processing_db,
                self.step_ef_lookup
            )
        )
        result.cf_processing_kg_co2e = cf_processing
        result.calculation_notes.extend(proc_notes)

        # 4. Packaging
        cf_packaging, pack_notes = components.calculate_packaging(
            packaging_categories, packaging_masses,
            self.packaging_ef
        )
        result.cf_packaging_kg_co2e = cf_packaging
        result.calculation_notes.extend(pack_notes)

        # 5. Totals with adjustment
        cf_modelled = (
            cf_raw + cf_transport + cf_processing + cf_packaging
        )
        cf_adjustment = cf_modelled * (
            self.config.adjustment_factor - 1.0
        )
        result.cf_modelled_kg_co2e = cf_modelled
        result.cf_adjustment_kg_co2e = cf_adjustment
        result.cf_total_kg_co2e = (
            cf_modelled * self.config.adjustment_factor
        )

        self.records_processed += 1
        if result.calculation_notes:
            self.records_with_warnings += 1

        return result

    def validate_result(
        self,
        result: CalculationResult
    ) -> List[str]:
        """Validate calculation result for sanity."""
        issues = []
        if result.cf_raw_materials_kg_co2e < 0:
            issues.append("Negative raw materials footprint")
        if result.cf_transport_kg_co2e < 0:
            issues.append("Negative transport footprint")
        if result.cf_processing_kg_co2e < 0:
            issues.append("Negative processing footprint")
        if result.cf_packaging_kg_co2e < 0:
            issues.append("Negative packaging footprint")
        if result.cf_raw_materials_kg_co2e > 50.0:
            issues.append("Raw materials footprint > 50 kgCO2e")
        if result.cf_transport_kg_co2e > 10.0:
            issues.append("Transport footprint > 10 kgCO2e")
        if result.cf_processing_kg_co2e > 30.0:
            issues.append("Processing footprint > 30 kgCO2e")
        if result.cf_packaging_kg_co2e > 2.0:
            issues.append("Packaging footprint > 2 kgCO2e")
        if result.cf_total_kg_co2e > 80.0:
            issues.append("Total footprint > 80 kgCO2e")
        return issues

    def get_statistics(self) -> Dict[str, Any]:
        """Get calculation statistics."""
        total = (
            self._match_stats['matches']
            + self._match_stats['misses']
        )
        return {
            'records_processed': self.records_processed,
            'records_with_warnings': self.records_with_warnings,
            'material_matches': self._match_stats['matches'],
            'material_misses': self._match_stats['misses'],
            'material_match_rate': (
                self._match_stats['matches'] / total
                if total > 0 else 0.0
            )
        }
