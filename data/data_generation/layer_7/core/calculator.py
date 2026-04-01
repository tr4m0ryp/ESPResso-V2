"""Water Footprint Calculator for Layer 7.

Loads water reference databases and computes per-record water
footprints using AWARE-weighted consumption factors.

Primary classes:
    WaterFootprintCalculator -- Loads databases, delegates to components.

Dependencies:
    _db_loader module for CSV loading (split for 300-line limit).
    databases module for database wrappers.
    components module for individual component calculators.
"""

import ast
import json
import logging
from typing import Any, Dict, List, Optional

from data.data_generation.layer_7.config.config import Layer7Config
from data.data_generation.layer_7.core.databases import (
    WaterMaterialDatabase,
    WaterProcessingDatabase,
    WaterPackagingDatabase,
    AWAREDatabase,
    WaterCalculationResult,
)
from data.data_generation.layer_7.core._db_loader import (
    load_materials_db,
    load_processing_db,
    load_packaging_db,
    load_aware_databases,
)
from data.data_generation.layer_7.core.country_resolver import (
    load_country_aliases,
)
from data.data_generation.layer_7.core import components

logger = logging.getLogger(__name__)


class WaterFootprintCalculator:
    """Main calculator: loads water databases, delegates to components."""

    def __init__(self, config: Layer7Config):
        """Initialize calculator with configuration.

        Args:
            config: Layer 7 configuration.
        """
        self.config = config
        self.material_db: Optional[WaterMaterialDatabase] = None
        self.processing_db: Optional[WaterProcessingDatabase] = None
        self.packaging_db: Optional[WaterPackagingDatabase] = None
        self.aware_agri_db: Optional[AWAREDatabase] = None
        self.aware_nonagri_db: Optional[AWAREDatabase] = None
        self.country_aliases: Dict[str, str] = {}

        self.records_processed = 0
        self.records_with_warnings = 0
        self._match_stats = {'matches': 0, 'misses': 0}

    def load_databases(self) -> bool:
        """Load all water reference databases from CSV files.

        Returns:
            True if all databases loaded successfully.
        """
        try:
            self.material_db = load_materials_db(self.config)
            self.processing_db = load_processing_db(self.config)
            self.packaging_db = load_packaging_db(self.config)
            agri, nonagri = load_aware_databases(self.config)
            self.aware_agri_db = agri
            self.aware_nonagri_db = nonagri
            self.country_aliases = load_country_aliases(
                self.config.aware_aliases_path
            )
            return True
        except Exception as e:
            logger.error("Failed to load databases: %s", e)
            return False

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
        self, record: Dict[str, Any]
    ) -> WaterCalculationResult:
        """Calculate complete water footprint for a single record.

        Args:
            record: Input record dictionary with materials, weights,
                preprocessing_steps, packaging, and transport_legs.

        Returns:
            WaterCalculationResult with all component values.
        """
        result = WaterCalculationResult()

        materials = self._parse_json_array(
            record.get('materials', '[]')
        )
        weights = self._parse_json_array(
            record.get('material_weights_kg', '[]')
        )
        steps = self._parse_json_array(
            record.get('preprocessing_steps', '[]')
        )
        pkg_cats = self._parse_json_array(
            record.get('packaging_categories', '[]')
        )
        pkg_masses = self._parse_json_array(
            record.get('packaging_masses_kg', '[]')
        )
        transport_legs = record.get('transport_legs', '[]')

        # Derive per-category masses from total when missing
        if not pkg_masses and pkg_cats:
            total_pkg = record.get('total_packaging_mass_kg', 0)
            try:
                total_pkg = float(total_pkg)
            except (TypeError, ValueError):
                total_pkg = 0.0
            if total_pkg > 0 and len(pkg_cats) > 0:
                per_cat = total_pkg / len(pkg_cats)
                pkg_masses = [per_cat] * len(pkg_cats)

        try:
            weights = [float(w) for w in weights]
            pkg_masses = [float(m) for m in pkg_masses]
        except (TypeError, ValueError) as e:
            result.calculation_notes.append(
                f"Weight conversion error: {e}"
            )
            result.is_valid = False
            return result

        # 1. Raw materials (AWARE agri weighted)
        wf_raw, raw_notes = components.calculate_raw_materials_water(
            materials, weights,
            self.material_db, self.aware_agri_db,
            transport_legs, self.country_aliases,
            self._match_stats,
        )
        result.wf_raw_materials_m3_world_eq = wf_raw
        result.calculation_notes.extend(raw_notes)

        # 2. Processing (AWARE nonagri weighted)
        wf_proc, proc_notes = components.calculate_processing_water(
            materials, weights, steps,
            self.processing_db, self.aware_nonagri_db,
            transport_legs, self.country_aliases,
        )
        result.wf_processing_m3_world_eq = wf_proc
        result.calculation_notes.extend(proc_notes)

        # 3. Packaging (no AWARE)
        wf_pack, pack_notes = components.calculate_packaging_water(
            pkg_cats, pkg_masses, self.packaging_db,
        )
        result.wf_packaging_m3_world_eq = wf_pack
        result.calculation_notes.extend(pack_notes)

        # 4. Total (no adjustment, no transport)
        result.wf_total_m3_world_eq = (
            components.calculate_total_water(
                wf_raw, wf_proc, wf_pack
            )
        )

        self.records_processed += 1
        if result.calculation_notes:
            self.records_with_warnings += 1

        return result

    _VALIDATION_LIMITS = [
        ('wf_raw_materials_m3_world_eq', 500.0, 'raw materials'),
        ('wf_processing_m3_world_eq', 200.0, 'processing'),
        ('wf_packaging_m3_world_eq', 5.0, 'packaging'),
        ('wf_total_m3_world_eq', 700.0, 'total'),
    ]

    def validate_result(
        self, result: WaterCalculationResult
    ) -> List[str]:
        """Validate calculation result for sanity.

        Args:
            result: Water footprint calculation result.

        Returns:
            List of validation issue descriptions.
        """
        issues = []
        for attr, limit, label in self._VALIDATION_LIMITS:
            val = getattr(result, attr)
            if val < 0:
                issues.append(
                    f"Negative {label} water footprint"
                )
            elif val > limit:
                issues.append(
                    f"{label.title()} water footprint "
                    f"> {limit} m3"
                )
        return issues

    def get_statistics(self) -> Dict[str, Any]:
        """Get calculation statistics.

        Returns:
            Dictionary with processing counts and match rate.
        """
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
            ),
        }
