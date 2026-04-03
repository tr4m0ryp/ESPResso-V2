"""
Distribution checker for Layer 5 statistical validation.

Monitors material, category, transport, and packaging distributions
for over-representation and coverage issues.
"""

from collections import Counter
from typing import Any, Dict, List

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import CompleteProductRecord


class DistributionChecker:
    """Monitors dataset distribution constraints."""

    def __init__(self, config: Layer5Config):
        self.config = config
        self.material_counts: Counter = Counter()
        self.category_counts: Counter = Counter()
        self.transport_type_counts: Counter = Counter()
        self.packaging_category_counts: Counter = Counter()

    def check_distributions(
        self, record: CompleteProductRecord
    ) -> Dict[str, Any]:
        """Check distribution constraints and coverage."""
        issues: List[str] = []

        material_ok = self._check_material_distribution(record, issues)
        category_ok = self._check_category_distribution(record, issues)
        transport_ok = self._check_transport_distribution(record, issues)
        packaging_ok = self._check_packaging_distribution(record, issues)

        return {
            "material_ok": material_ok,
            "category_ok": category_ok,
            "transport_ok": transport_ok,
            "packaging_ok": packaging_ok,
            "issues": issues,
        }

    def _check_material_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check material distribution constraints."""
        for material in record.materials:
            self.material_counts[material.lower()] += 1

        total_materials = sum(self.material_counts.values())
        if total_materials > 0:
            max_pct = self.config.max_single_material_pct
            for material, count in self.material_counts.items():
                percentage = count / total_materials
                if percentage > max_pct:
                    issues.append(
                        f"Material '{material}' over-represented: "
                        f"{percentage:.1%}"
                    )
                    return False

        return True

    def _check_category_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check category distribution constraints."""
        self.category_counts[record.subcategory_id] += 1
        return True

    def _check_transport_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check transport distribution constraints."""
        self.transport_type_counts[record.supply_chain_type] += 1

        total_transport = sum(self.transport_type_counts.values())
        if total_transport > 100:
            for transport_type, count in self.transport_type_counts.items():
                percentage = count / total_transport
                if percentage > 0.80:
                    issues.append(
                        f"Transport type '{transport_type}' "
                        f"over-represented: {percentage:.1%}"
                    )
                    return False

        return True

    def _check_packaging_distribution(
        self, record: CompleteProductRecord, issues: List[str]
    ) -> bool:
        """Check packaging distribution constraints."""
        for category in record.packaging_categories:
            self.packaging_category_counts[category] += 1

        total_packaging = sum(self.packaging_category_counts.values())
        if total_packaging > 100:
            for category, count in self.packaging_category_counts.items():
                percentage = count / total_packaging
                if percentage > 0.80:
                    issues.append(
                        f"Packaging category '{category}' "
                        f"over-represented: {percentage:.1%}"
                    )
                    return False

        return True

    def reset(self) -> None:
        """Reset distribution tracking state."""
        self.material_counts = Counter()
        self.category_counts = Counter()
        self.transport_type_counts = Counter()
        self.packaging_category_counts = Counter()
