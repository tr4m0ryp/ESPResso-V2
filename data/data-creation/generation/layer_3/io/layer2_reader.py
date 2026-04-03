"""
Layer 2 data reader for Layer 3 Transport Scenario Generator.

Reads and parses the output from Layer 2 (preprocessing paths) to use as input
for transport scenario generation.
"""

import json
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Any
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class Layer2Record:
    """Represents a Layer 2 output record (preprocessing paths)."""
    # From Layer 1
    category_id: str
    category_name: str
    subcategory_id: str
    subcategory_name: str
    materials: List[str]
    material_weights_kg: List[float]
    material_percentages: List[int]
    total_weight_kg: float
    
    # From Layer 2
    preprocessing_path_id: str
    preprocessing_steps: List[str]
    step_material_mapping: Dict[str, List[str]]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
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
            "step_material_mapping": json.dumps(self.step_material_mapping),
        }


class Layer2DataReader:
    """Reads and validates Layer 2 output data."""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._validate_file()

    def _validate_file(self) -> None:
        """Validate that the input file exists and is readable."""
        if not self.file_path.exists():
            raise FileNotFoundError(f"Layer 2 output file not found: {self.file_path}")
        
        if not self.file_path.is_file():
            raise ValueError(f"Layer 2 output path is not a file: {self.file_path}")

    def read_records(self) -> List[Layer2Record]:
        """Read all records (alias for read_all_records)."""
        return self.read_all_records()

    def read_all_records(self) -> List[Layer2Record]:
        """Read all records from the Layer 2 output file."""
        logger.info("Reading Layer 2 data from %s", self.file_path)

        try:
            try:
                df = pd.read_parquet(self.file_path)
            except Exception:
                df = pd.read_csv(self.file_path)
            logger.info(
                "Loaded %d records from Layer 2 output", len(df)
            )

            parsed_records = []
            for _, row in df.iterrows():
                try:
                    record = self._parse_record(row)
                    parsed_records.append(record)
                except Exception as e:
                    logger.warning("Failed to parse record: %s", e)
                    continue

            logger.info(
                "Successfully parsed %d valid records",
                len(parsed_records)
            )
            return parsed_records

        except Exception as e:
            logger.error("Failed to read Layer 2 data: %s", e)
            raise

    @staticmethod
    def _parse_json_field(value: Any) -> Any:
        """Parse a field that may be a JSON string or native type."""
        if isinstance(value, (list, dict)):
            return value
        return json.loads(str(value))

    def _parse_record(self, row: Any) -> Layer2Record:
        """Parse a single row into a Layer2Record."""
        try:
            materials = self._parse_json_field(row['materials'])
            material_weights_kg = self._parse_json_field(
                row['material_weights_kg']
            )
            material_percentages = self._parse_json_field(
                row['material_percentages']
            )
            preprocessing_steps = self._parse_json_field(
                row['preprocessing_steps']
            )
            step_material_mapping = self._parse_json_field(
                row['step_material_mapping']
            )

            return Layer2Record(
                category_id=str(row['category_id']),
                category_name=str(row['category_name']),
                subcategory_id=str(row['subcategory_id']),
                subcategory_name=str(row['subcategory_name']),
                materials=materials,
                material_weights_kg=material_weights_kg,
                material_percentages=material_percentages,
                total_weight_kg=float(row['total_weight_kg']),
                preprocessing_path_id=str(
                    row['preprocessing_path_id']
                ),
                preprocessing_steps=preprocessing_steps,
                step_material_mapping=step_material_mapping
            )

        except (json.JSONDecodeError, KeyError, ValueError) as e:
            raise ValueError(
                "Invalid record format: %s" % e
            )

    def read_records_batch(self, batch_size: int = 1000) -> List[Layer2Record]:
        """
        Read records in batches for memory-efficient processing.
        
        Note: This is a simplified implementation. For very large files,
        consider using pandas chunk reading or streaming approach.
        """
        all_records = self.read_all_records()
        
        for i in range(0, len(all_records), batch_size):
            batch = all_records[i:i + batch_size]
            logger.info(f"Yielding batch {i//batch_size + 1}: {len(batch)} records")
            yield batch

    def get_record_count(self) -> int:
        """Get the total number of records in the file."""
        try:
            try:
                df = pd.read_parquet(self.file_path)
            except Exception:
                df = pd.read_csv(self.file_path)
            return len(df)
        except Exception as e:
            logger.error("Failed to count records: %s", e)
            return 0

    def validate_record_integrity(self, record: Layer2Record) -> bool:
        """Validate the integrity of a parsed record."""
        try:
            # Check array lengths match
            if not (len(record.materials) == len(record.material_weights_kg) == len(record.material_percentages)):
                return False
            
            # Check weights are positive
            if any(w <= 0 for w in record.material_weights_kg):
                return False
            
            # Check percentages sum to ~100
            if abs(sum(record.material_percentages) - 100) > 2:
                return False
            
            # Check preprocessing steps exist
            if not record.preprocessing_steps:
                return False
            
            # Check step material mapping consistency
            if not record.step_material_mapping:
                return False
            
            # Verify all materials are covered in step mapping
            materials_in_mapping = set()
            for material_list in record.step_material_mapping.values():
                materials_in_mapping.update(material_list)
            
            record_materials = set(record.materials)
            if not record_materials.issubset(materials_in_mapping):
                return False
            
            return True
            
        except Exception as e:
            logger.warning(f"Record validation failed: {e}")
            return False

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics of the Layer 2 data."""
        try:
            try:
                df = pd.read_parquet(self.file_path)
            except Exception:
                df = pd.read_csv(self.file_path)

            if df.empty:
                return {"total_records": 0}

            stats = {
                "total_records": len(df),
                "unique_categories": df['category_id'].nunique(),
                "unique_subcategories": df['subcategory_id'].nunique(),
                "unique_preprocessing_paths": df[
                    'preprocessing_path_id'
                ].nunique(),
                "avg_total_weight": float(
                    df['total_weight_kg'].mean()
                ),
                "min_total_weight": float(
                    df['total_weight_kg'].min()
                ),
                "max_total_weight": float(
                    df['total_weight_kg'].max()
                ),
            }
            return stats

        except Exception as e:
            logger.error("Failed to generate summary stats: %s", e)
            return {"error": str(e)}