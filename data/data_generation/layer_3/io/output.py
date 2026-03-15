"""
Output handling for Layer 3 Transport Scenario Generator (V2).

Handles CSV writing with the 13-column V2 schema, Parquet output,
checkpointing, and progress tracking.
"""

import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
import time

from data.data_generation.layer_3.models.models import Layer3Record

logger = logging.getLogger(__name__)

# V2 output schema: 11 carried forward + 2 added by Layer 3
HEADERS = [
    # Carried forward from L1/L2 (11)
    "category_id",
    "category_name",
    "subcategory_id",
    "subcategory_name",
    "materials",
    "material_weights_kg",
    "material_percentages",
    "total_weight_kg",
    "preprocessing_path_id",
    "preprocessing_steps",
    "step_material_mapping",
    # Added by Layer 3 (2)
    "transport_legs",
    "total_distance_km",
]


@dataclass
class ProgressTracker:
    """Tracks generation progress and statistics."""

    total_records: int = 0
    processed_records: int = 0
    generated_scenarios: int = 0
    failed_records: int = 0
    start_time: float = field(default_factory=time.time)

    def update(self, processed: int = 0, generated: int = 0, failed: int = 0):
        """Update progress counters."""
        self.processed_records += processed
        self.generated_scenarios += generated
        self.failed_records += failed

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        elapsed_time = time.time() - self.start_time

        stats = {
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "generated_scenarios": self.generated_scenarios,
            "failed_records": self.failed_records,
            "completion_rate": (
                self.processed_records / max(self.total_records, 1)
            ),
            "avg_scenarios_per_record": (
                self.generated_scenarios / max(self.processed_records, 1)
            ),
            "failure_rate": (
                self.failed_records / max(self.processed_records, 1)
            ),
            "elapsed_time_seconds": elapsed_time,
            "records_per_second": (
                self.processed_records / max(elapsed_time, 1)
            ),
        }

        return stats

    def print_progress(self):
        """Print current progress to console."""
        stats = self.get_stats()

        print(
            f"\rProgress: {stats['processed_records']}/{stats['total_records']}"
            f" records ({stats['completion_rate']:.1%}) | "
            f"{stats['generated_scenarios']} scenarios generated | "
            f"{stats['failed_records']} failed | "
            f"{stats['records_per_second']:.1f} rec/s",
            end="",
            flush=True,
        )


class OutputWriter:
    """Handles CSV/Parquet output writing and checkpointing for V2 schema."""

    def __init__(self, config, progress_tracker: Optional[ProgressTracker] = None):
        self.config = config
        self.progress_tracker = progress_tracker or ProgressTracker()
        self.output_path = config.output_path
        self.checkpoint_dir = config.checkpoint_dir

        # Ensure directories exist
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # V2 CSV headers (13 columns)
        self.headers = list(HEADERS)

    def write_records(self, records: List[Layer3Record], mode: str = "append") -> int:
        """Write records to CSV file using the V2 13-column schema.

        Each record is serialized via Layer3Record.to_dict(), which converts
        list/dict fields and transport_legs to JSON strings.
        """
        if not records:
            return 0

        try:
            record_dicts = [record.to_dict() for record in records]

            file_mode = (
                "a" if mode == "append" and self.output_path.exists() else "w"
            )

            with open(
                self.output_path, file_mode, newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.headers)

                if file_mode == "w":
                    writer.writeheader()

                writer.writerows(record_dicts)

            self.progress_tracker.update(generated=len(records))

            logger.info("Wrote %d records to %s", len(records), self.output_path)
            return len(records)

        except Exception as e:
            logger.error("Failed to write records: %s", e)
            return 0

    def write_parquet(
        self,
        records: List[Layer3Record],
        output_path: Optional[Path] = None,
    ) -> bool:
        """Write records to Parquet format with snappy compression.

        Args:
            records: Layer3Record instances to write.
            output_path: Destination path. Defaults to the CSV output path
                with a .parquet extension.

        Returns:
            True on success, False on failure.
        """
        if not records:
            logger.warning("No records to write to Parquet")
            return False

        import pandas as pd

        if output_path is None:
            output_path = self.output_path.with_suffix(".parquet")

        try:
            record_dicts = [record.to_dict() for record in records]
            df = pd.DataFrame(record_dicts, columns=self.headers)

            output_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)

            logger.info(
                "Wrote %d records to Parquet: %s", len(records), output_path
            )
            return True

        except Exception as e:
            logger.error("Failed to write Parquet: %s", e)
            return False

    def write_checkpoint(
        self, records: List[Layer3Record], checkpoint_name: str
    ) -> bool:
        """Write checkpoint file."""
        if not records:
            return False

        try:
            checkpoint_path = self.checkpoint_dir / f"{checkpoint_name}.csv"

            record_dicts = [record.to_dict() for record in records]

            with open(
                checkpoint_path, "w", newline="", encoding="utf-8"
            ) as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=self.headers)
                writer.writeheader()
                writer.writerows(record_dicts)

            logger.info(
                "Checkpoint saved: %s (%d records)",
                checkpoint_path,
                len(records),
            )
            return True

        except Exception as e:
            logger.error("Failed to write checkpoint %s: %s", checkpoint_name, e)
            return False

    def load_checkpoint(
        self, checkpoint_name: str
    ) -> Optional[List[Layer3Record]]:
        """Load records from a checkpoint file."""
        checkpoint_path = self.checkpoint_dir / f"{checkpoint_name}.csv"

        if not checkpoint_path.exists():
            return None

        try:
            records = []

            with open(checkpoint_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    record = self._parse_layer3_record(row)
                    if record:
                        records.append(record)

            logger.info(
                "Loaded %d records from checkpoint %s",
                len(records),
                checkpoint_name,
            )
            return records

        except Exception as e:
            logger.error(
                "Failed to load checkpoint %s: %s", checkpoint_name, e
            )
            return None

    def _parse_layer3_record(
        self, row_data: Dict[str, Any]
    ) -> Optional[Layer3Record]:
        """Parse a CSV row into a Layer3Record using the V2 schema."""
        try:
            return Layer3Record.from_dict(row_data)
        except (json.JSONDecodeError, KeyError, ValueError) as e:
            logger.warning("Failed to parse Layer 3 record: %s", e)
            return None

    def get_output_summary(self) -> Dict[str, Any]:
        """Get summary of the output file with V2 metrics.

        Reports average legs per record, average distance, unique
        locations across all legs, and category distribution.
        """
        if not self.output_path.exists():
            return {"exists": False}

        try:
            records = []
            with open(self.output_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    records.append(dict(row))

            if not records:
                return {"exists": True, "total_records": 0}

            total_records = len(records)
            unique_categories = len(
                set(r["category_id"] for r in records)
            )
            unique_preprocessing_paths = len(
                set(r["preprocessing_path_id"] for r in records)
            )

            # Distance statistics
            distances = [
                float(r["total_distance_km"]) for r in records
            ]
            avg_distance = sum(distances) / len(distances)
            min_distance = min(distances)
            max_distance = max(distances)

            # Transport legs statistics
            total_legs = 0
            all_locations = set()
            for r in records:
                try:
                    legs = json.loads(r["transport_legs"])
                    total_legs += len(legs)
                    for leg in legs:
                        from_loc = leg.get("from_location", "")
                        to_loc = leg.get("to_location", "")
                        if from_loc:
                            all_locations.add(from_loc)
                        if to_loc:
                            all_locations.add(to_loc)
                except (json.JSONDecodeError, TypeError):
                    pass

            avg_legs_per_record = total_legs / max(total_records, 1)

            summary = {
                "exists": True,
                "total_records": total_records,
                "unique_categories": unique_categories,
                "unique_preprocessing_paths": unique_preprocessing_paths,
                "avg_distance_km": avg_distance,
                "min_distance_km": min_distance,
                "max_distance_km": max_distance,
                "total_transport_legs": total_legs,
                "avg_legs_per_record": avg_legs_per_record,
                "unique_locations": len(all_locations),
            }

            return summary

        except Exception as e:
            logger.error("Failed to get output summary: %s", e)
            return {"exists": True, "error": str(e)}

    def cleanup_checkpoints(self, keep_last: int = 3) -> int:
        """Clean up old checkpoint files, keeping the most recent ones."""
        try:
            checkpoint_files = sorted(
                self.checkpoint_dir.glob("*.csv"),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            )

            removed_count = 0
            for checkpoint_file in checkpoint_files[keep_last:]:
                checkpoint_file.unlink()
                removed_count += 1

            logger.info("Cleaned up %d old checkpoint files", removed_count)
            return removed_count

        except Exception as e:
            logger.error("Failed to cleanup checkpoints: %s", e)
            return 0

    def export_sample_data(
        self, sample_size: int = 100, output_path: Optional[Path] = None
    ) -> bool:
        """Export a sample of the data for review."""
        if not self.output_path.exists():
            return False

        if output_path is None:
            output_path = self.config.output_dir / "layer_3_sample.csv"

        try:
            records = []
            with open(self.output_path, "r", encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    records.append(dict(row))

            if len(records) <= sample_size:
                sample_records = records
            else:
                import random

                sample_records = random.sample(records, sample_size)

            if sample_records:
                with open(
                    output_path, "w", newline="", encoding="utf-8"
                ) as csvfile:
                    writer = csv.DictWriter(
                        csvfile, fieldnames=self.headers
                    )
                    writer.writeheader()
                    writer.writerows(sample_records)

                logger.info(
                    "Exported sample of %d records to %s",
                    len(sample_records),
                    output_path,
                )
                return True
            else:
                return False

        except Exception as e:
            logger.error("Failed to export sample data: %s", e)
            return False
