"""
Layer 1 output reader for Layer 2.

Reads product compositions from Layer 1 output Parquet.
"""

import json

import pandas as pd
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Iterator, Any


@dataclass
class Layer1Record:
    """Represents a single record from Layer 1 output."""
    category_id: str
    category_name: str
    subcategory_id: str
    subcategory_name: str
    materials: List[str]
    material_weights_kg: List[float]
    material_percentages: List[int]
    total_weight_kg: float

    # Internal tracking
    _row_index: int = 0

    @classmethod
    def from_csv_row(cls, row: Dict[str, str], row_index: int = 0) -> "Layer1Record":
        """Create from CSV row dictionary."""
        # Parse JSON arrays
        materials = row.get('materials', '[]')
        if isinstance(materials, str):
            materials = json.loads(materials)

        weights = row.get('material_weights_kg', '[]')
        if isinstance(weights, str):
            weights = json.loads(weights)
        weights = [float(w) for w in weights]

        percentages = row.get('material_percentages', '[]')
        if isinstance(percentages, str):
            percentages = json.loads(percentages)
        percentages = [int(p) for p in percentages]

        total_weight = row.get('total_weight_kg', '0')
        if isinstance(total_weight, str):
            total_weight = float(total_weight)

        return cls(
            category_id=row.get('category_id', '').strip(),
            category_name=row.get('category_name', '').strip(),
            subcategory_id=row.get('subcategory_id', '').strip(),
            subcategory_name=row.get('subcategory_name', '').strip(),
            materials=materials,
            material_weights_kg=weights,
            material_percentages=percentages,
            total_weight_kg=total_weight,
            _row_index=row_index
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'category_id': self.category_id,
            'category_name': self.category_name,
            'subcategory_id': self.subcategory_id,
            'subcategory_name': self.subcategory_name,
            'materials': json.dumps(self.materials),
            'material_weights_kg': json.dumps(self.material_weights_kg),
            'material_percentages': json.dumps(self.material_percentages),
            'total_weight_kg': self.total_weight_kg
        }

    def format_materials_with_weights(self) -> str:
        """Format materials with their weights for prompt."""
        lines = []
        for i, (mat, weight, pct) in enumerate(zip(
            self.materials, self.material_weights_kg, self.material_percentages
        )):
            lines.append(f"- {mat}: {weight:.3f} kg ({pct}%)")
        return "\n".join(lines)

    def get_material_weight(self, material_name: str) -> Optional[float]:
        """Get weight for a specific material."""
        for mat, weight in zip(self.materials, self.material_weights_kg):
            if mat.lower() == material_name.lower():
                return weight
        return None


class Layer1Reader:
    """Reads Layer 1 output CSV file."""

    def __init__(self, csv_path: Path):
        self.csv_path = csv_path
        self._records: Optional[List[Layer1Record]] = None
        self._total_count: Optional[int] = None

    def _ensure_loaded(self) -> None:
        """Ensure records are loaded."""
        if self._records is None:
            self._load_all()

    def _load_all(self) -> None:
        """Load all records into memory."""
        self._records = []
        try:
            df = pd.read_parquet(self.csv_path)
        except Exception:
            df = pd.read_csv(self.csv_path)
        for i, (_, row) in enumerate(df.iterrows()):
            try:
                row_dict = {
                    k: str(v) if not isinstance(v, (list, dict))
                    else json.dumps(v)
                    for k, v in row.items()
                }
                record = Layer1Record.from_csv_row(
                    row_dict, row_index=i
                )
                self._records.append(record)
            except Exception as e:
                print(f"Warning: Failed to parse row {i}: {e}")
        self._total_count = len(self._records)

    def get_total_count(self) -> int:
        """Get total number of records."""
        if self._total_count is None:
            try:
                df = pd.read_parquet(self.csv_path)
            except Exception:
                df = pd.read_csv(self.csv_path)
            self._total_count = len(df)
        return self._total_count

    def read_records(self) -> List[Layer1Record]:
        """Get all records (alias for get_all_records)."""
        return self.get_all_records()

    def get_all_records(self) -> List[Layer1Record]:
        """Get all records."""
        self._ensure_loaded()
        return self._records

    def get_record(self, index: int) -> Optional[Layer1Record]:
        """Get a specific record by index."""
        self._ensure_loaded()
        if 0 <= index < len(self._records):
            return self._records[index]
        return None

    def iterate_records(self) -> Iterator[Layer1Record]:
        """Iterate over records."""
        self._ensure_loaded()
        yield from self._records

    def iterate_batches(self, batch_size: int) -> Iterator[List[Layer1Record]]:
        """Iterate over records in batches."""
        batch = []
        for record in self.iterate_records():
            batch.append(record)
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch

    def get_unique_materials(self) -> List[str]:
        """Get list of unique materials across all records."""
        self._ensure_loaded()
        materials = set()
        for record in self._records:
            materials.update(record.materials)
        return sorted(list(materials))

    def get_records_by_category(self, category_id: str) -> List[Layer1Record]:
        """Get all records for a specific category."""
        self._ensure_loaded()
        return [r for r in self._records if r.category_id == category_id or r.subcategory_id == category_id]

    def __len__(self) -> int:
        return self.get_total_count()

    def __iter__(self) -> Iterator[Layer1Record]:
        return self.iterate_records()
