"""
Output handling for Layer 2.

Manages CSV writing, checkpointing, and progress tracking.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from ..config.config import Layer2Config
from ..core.generator import Layer2Record

logger = logging.getLogger(__name__)


class OutputWriter:
    """Handles writing Layer 2 records to CSV and managing checkpoints."""

    CSV_FIELDNAMES = [
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
        "step_material_mapping"
    ]

    def __init__(self, config: Layer2Config):
        self.config = config
        self.config.ensure_directories()
        self._current_batch: List[Layer2Record] = []
        self._total_written = 0
        self._output_file: Optional[Path] = None
        self._csv_file = None
        self._csv_writer = None

    def initialize_output(self, output_path: Optional[Path] = None) -> None:
        """Initialize the output CSV file."""
        self._output_file = output_path or self.config.output_path

        # Check if file exists and has content
        file_exists = self._output_file.exists() and self._output_file.stat().st_size > 0

        if file_exists:
            # Count existing records
            with open(self._output_file, 'r', encoding='utf-8') as f:
                self._total_written = sum(1 for _ in f) - 1  # Subtract header
            logger.info(f"Resuming from existing file with {self._total_written} records")

            # Open for append
            self._csv_file = open(self._output_file, 'a', encoding='utf-8', newline='')
            self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=self.CSV_FIELDNAMES)
        else:
            # Create new file with header
            self._csv_file = open(self._output_file, 'w', encoding='utf-8', newline='')
            self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=self.CSV_FIELDNAMES)
            self._csv_writer.writeheader()
            logger.info(f"Created new output file: {self._output_file}")

    def write_record(self, record: Layer2Record) -> None:
        """Write a single record to the output file.

        Used by sequential mode. For parallel mode, use write_batch() instead
        to reduce lock acquisitions and flush atomically.
        """
        if self._csv_writer is None:
            raise RuntimeError("Output not initialized. Call initialize_output() first.")

        self._csv_writer.writerow(record.to_dict())
        self._total_written += 1
        self._current_batch.append(record)

        # Flush periodically to avoid data loss on crash
        if self._total_written % 100 == 0:
            self._csv_file.flush()

    def write_batch(self, records: List[Layer2Record]) -> None:
        """Write a batch of records atomically then flush.

        All rows are written in one locked call so parallel workers
        never interleave rows or contend on per-record flushes.
        """
        if self._csv_writer is None:
            raise RuntimeError("Output not initialized. Call initialize_output() first.")

        for record in records:
            self._csv_writer.writerow(record.to_dict())
            self._total_written += 1
            self._current_batch.append(record)
        self._csv_file.flush()

    def should_checkpoint(self) -> bool:
        """Check if a checkpoint should be created."""
        return len(self._current_batch) >= self.config.checkpoint_interval

    def create_checkpoint(self, layer1_index: int = 0) -> Path:
        """Create a checkpoint of current progress."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = self.config.checkpoint_dir / f"checkpoint_{timestamp}_{self._total_written}.json"

        checkpoint_data = {
            "timestamp": timestamp,
            "total_written": self._total_written,
            "last_batch_size": len(self._current_batch),
            "last_layer1_index": layer1_index,
            "output_file": str(self._output_file),
        }

        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2)

        logger.info(f"Checkpoint created: {checkpoint_file}")

        # Clear current batch
        self._current_batch = []

        # Flush output
        if self._csv_file:
            self._csv_file.flush()

        return checkpoint_file

    def get_latest_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Get the most recent checkpoint data."""
        checkpoint_files = sorted(self.config.checkpoint_dir.glob("checkpoint_*.json"))

        if not checkpoint_files:
            return None

        latest = checkpoint_files[-1]
        with open(latest, 'r', encoding='utf-8') as f:
            return json.load(f)

    def close(self) -> None:
        """Close the output file."""
        if self._csv_file:
            self._csv_file.flush()
            self._csv_file.close()
            self._csv_file = None
            self._csv_writer = None

    @property
    def total_written(self) -> int:
        """Get total number of records written."""
        return self._total_written

    def __enter__(self):
        self.initialize_output()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


class ProgressTracker:
    """Tracks generation progress and provides statistics."""

    def __init__(self):
        self.start_time: Optional[datetime] = None
        self.layer1_records_processed = 0
        self.layer1_records_total = 0
        self.layer2_records_generated = 0
        self.layer2_records_valid = 0
        self.layer2_records_invalid = 0
        self.api_calls = 0
        self.api_errors = 0

    def start(self, total_layer1_records: int) -> None:
        """Start tracking progress."""
        self.start_time = datetime.now()
        self.layer1_records_total = total_layer1_records

    def record_layer1_processed(self) -> None:
        """Record that a Layer 1 record was processed."""
        self.layer1_records_processed += 1

    def record_layer2_generated(self, count: int, valid_count: int) -> None:
        """Record generated Layer 2 records."""
        self.layer2_records_generated += count
        self.layer2_records_valid += valid_count
        self.layer2_records_invalid += (count - valid_count)

    def record_api_call(self, success: bool) -> None:
        """Record an API call."""
        self.api_calls += 1
        if not success:
            self.api_errors += 1

    def get_progress_percent(self) -> float:
        """Get progress as percentage."""
        if self.layer1_records_total == 0:
            return 0.0
        return (self.layer1_records_processed / self.layer1_records_total) * 100

    def get_elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None:
            return 0.0
        return (datetime.now() - self.start_time).total_seconds()

    def get_estimated_remaining(self) -> Optional[float]:
        """Get estimated remaining time in seconds."""
        if self.layer1_records_processed == 0 or self.start_time is None:
            return None

        elapsed = self.get_elapsed_time()
        rate = self.layer1_records_processed / elapsed
        remaining = self.layer1_records_total - self.layer1_records_processed

        return remaining / rate

    def get_expansion_factor(self) -> float:
        """Get average expansion factor (L2 records per L1 record)."""
        if self.layer1_records_processed == 0:
            return 0.0
        return self.layer2_records_generated / self.layer1_records_processed

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        elapsed = self.get_elapsed_time()
        remaining = self.get_estimated_remaining()

        return {
            "layer1_processed": self.layer1_records_processed,
            "layer1_total": self.layer1_records_total,
            "progress_percent": self.get_progress_percent(),
            "layer2_generated": self.layer2_records_generated,
            "layer2_valid": self.layer2_records_valid,
            "layer2_invalid": self.layer2_records_invalid,
            "expansion_factor": self.get_expansion_factor(),
            "validation_rate": (
                self.layer2_records_valid / self.layer2_records_generated * 100
                if self.layer2_records_generated > 0 else 0
            ),
            "api_calls": self.api_calls,
            "api_errors": self.api_errors,
            "elapsed_seconds": elapsed,
            "estimated_remaining_seconds": remaining,
            "rate_l1_per_minute": (
                self.layer1_records_processed / elapsed * 60 if elapsed > 0 else 0
            ),
        }

    def print_progress(self) -> None:
        """Print current progress to logger."""
        stats = self.get_stats()
        remaining = stats["estimated_remaining_seconds"]
        remaining_str = f"{remaining/60:.1f} min" if remaining else "calculating..."

        logger.info(
            f"Progress: {stats['progress_percent']:.1f}% "
            f"(L1: {stats['layer1_processed']}/{stats['layer1_total']}) | "
            f"L2 generated: {stats['layer2_valid']}/{stats['layer2_generated']} | "
            f"Expansion: {stats['expansion_factor']:.1f}x | "
            f"ETA: {remaining_str}"
        )
