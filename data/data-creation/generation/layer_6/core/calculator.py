"""Carbon Footprint Calculator for Layer 6.

Loads reference databases and computes per-record carbon footprints.
Supports both enriched (per-mode distance) and logit transport paths.
"""

import ast
import json
import logging
from typing import Dict, List, Optional, Any

import pandas as pd

from data.data_generation.layer_6.config.config import (
    Layer6Config,
    TRANSPORT_EMISSION_FACTORS,
)
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

_MODE_DISTANCE_COLUMNS = {
    'road': 'road_km',
    'sea': 'sea_km',
    'rail': 'rail_km',
    'air': 'air_km',
    'inland_waterway': 'inland_waterway_km',
}


def _extract_mode_distances(record: Dict[str, Any]) -> Dict[str, float]:
    """Extract per-mode distances from enriched record columns.

    Args:
        record: Input record dict with road_km, sea_km, etc.

    Returns:
        Dict mapping mode name to distance in km.
    """
    return {
        mode: float(record.get(col, 0.0) or 0.0)
        for mode, col in _MODE_DISTANCE_COLUMNS.items()
    }


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

            logger.info("Loaded %d materials", len(self.material_db.materials))
            errors = validate_aliases(set(self.material_db.materials.keys()))
            if errors:
                logger.warning("%d alias targets missing from reference DB", len(errors))
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

            logger.info("Loaded %d material-process combos", len(self.processing_db.combinations))
        except Exception as e:
            logger.error("Failed to load processing database: %s", e)
            return False

        try:
            steps_df = pd.read_parquet(self.config.processing_steps_path)
            for _, row in steps_df.iterrows():
                name = str(row.get('process_name', ''))
                try:
                    ef = float(row.get(
                        'carbon_footprint_kgCO2e_per_kg', 0
                    ))
                    self.step_ef_lookup[name] = ef
                except (ValueError, TypeError):
                    continue
            logger.info("Loaded %d step EFs", len(self.step_ef_lookup))
        except FileNotFoundError:
            logger.warning("Processing steps not found, step fallback disabled")
        except Exception as e:
            logger.warning("Failed to load processing steps: %s", e)

        return True

    @staticmethod
    def _parse_json_array(value: Any) -> List[Any]:
        """Parse JSON array from string or pass through list."""
        if isinstance(value, list):
            return value
        if not value or value == '':
            return []
        try:
            return json.loads(str(value))
        except json.JSONDecodeError:
            try:
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

        # 2. Transport (enriched actual-distance or logit fallback)
        if self.config.use_enriched_transport:
            mode_dists = _extract_mode_distances(record)
            cf_transport, _, mode_fracs, weighted_ef = (
                components.calculate_transport_from_actuals(
                    total_weight, mode_dists,
                    TRANSPORT_EMISSION_FACTORS))
            result.cf_transport_kg_co2e = cf_transport
            result.transport_mode_probabilities = mode_dists
            result.transport_mode_fractions = mode_fracs
            result.weighted_ef_g_co2e_tkm = weighted_ef
        else:
            cf_transport, mode_probs, weighted_ef = (
                components.calculate_transport_logit(
                    total_weight, transport_distance,
                    self.transport_model))
            result.cf_transport_kg_co2e = cf_transport
            result.transport_mode_probabilities = mode_probs
            result.weighted_ef_g_co2e_tkm = weighted_ef

        # 3. Processing (with per-material step routing if available)
        material_step_routing = record.get(
            'material_step_routing', None
        )
        cf_processing, proc_notes = (
            components.calculate_processing(
                materials, material_weights, preprocessing_steps,
                self.material_db, self.processing_db,
                self.step_ef_lookup,
                material_step_routing=material_step_routing,
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

    _VALIDATION_LIMITS = [
        ('cf_raw_materials_kg_co2e', 50.0, 'raw materials'),
        ('cf_transport_kg_co2e', 10.0, 'transport'),
        ('cf_processing_kg_co2e', 30.0, 'processing'),
        ('cf_packaging_kg_co2e', 2.0, 'packaging'),
        ('cf_total_kg_co2e', 80.0, 'total'),
    ]

    def validate_result(
        self, result: CalculationResult
    ) -> List[str]:
        """Validate calculation result for sanity."""
        issues = []
        for attr, limit, label in self._VALIDATION_LIMITS:
            val = getattr(result, attr)
            if val < 0:
                issues.append(f"Negative {label} footprint")
            elif val > limit:
                issues.append(
                    f"{label.title()} footprint > {limit} kgCO2e"
                )
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
