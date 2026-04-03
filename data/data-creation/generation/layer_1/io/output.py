"""
Output handling for Layer 1.

Manages CSV writing, checkpointing, and progress tracking.
"""

import csv
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any

from ..config.config import Layer1Config
from ..core.generator import ProductComposition

logger = logging.getLogger(__name__)


class OutputWriter:
    """Handles writing compositions to CSV and managing checkpoints."""

    CSV_FIELDNAMES = [
        "category_id",
        "category_name",
        "subcategory_id",
        "subcategory_name",
        "materials",
        "material_weights_kg",
        "material_percentages",
        "total_weight_kg"
    ]

    def __init__(self, config: Layer1Config):
        self.config = config
        self.config.ensure_directories()
        self._current_batch: List[ProductComposition] = []
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

    def write_composition(self, composition: ProductComposition) -> None:
        """Write a single composition to the output file."""
        if self._csv_writer is None:
            raise RuntimeError("Output not initialized. Call initialize_output() first.")

        self._csv_writer.writerow(composition.to_dict())
        self._total_written += 1
        self._current_batch.append(composition)

        # Flush periodically
        if self._total_written % 100 == 0:
            self._csv_file.flush()

    def write_batch(self, compositions: List[ProductComposition]) -> None:
        """Write a batch of compositions."""
        for comp in compositions:
            self.write_composition(comp)

    def should_checkpoint(self) -> bool:
        """Check if a checkpoint should be created."""
        return len(self._current_batch) >= self.config.checkpoint_interval

    def create_checkpoint(self) -> Path:
        """Create a checkpoint of current progress."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkpoint_file = self.config.checkpoint_dir / f"checkpoint_{timestamp}_{self._total_written}.json"

        checkpoint_data = {
            "timestamp": timestamp,
            "total_written": self._total_written,
            "last_batch_size": len(self._current_batch),
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
        """Get total number of compositions written."""
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
        self.items_processed = 0
        self.items_total = 0
        self.compositions_generated = 0
        self.compositions_valid = 0
        self.compositions_invalid = 0
        self.api_calls = 0
        self.api_errors = 0

    def start(self, total_items: int) -> None:
        """Start tracking progress."""
        self.start_time = datetime.now()
        self.items_total = total_items

    def record_item_processed(self) -> None:
        """Record that an item was processed."""
        self.items_processed += 1

    def record_composition(self, valid: bool) -> None:
        """Record a generated composition."""
        self.compositions_generated += 1
        if valid:
            self.compositions_valid += 1
        else:
            self.compositions_invalid += 1

    def record_api_call(self, success: bool) -> None:
        """Record an API call."""
        self.api_calls += 1
        if not success:
            self.api_errors += 1

    def get_progress_percent(self) -> float:
        """Get progress as percentage."""
        if self.items_total == 0:
            return 0.0
        return (self.items_processed / self.items_total) * 100

    def get_elapsed_time(self) -> float:
        """Get elapsed time in seconds."""
        if self.start_time is None:
            return 0.0
        return (datetime.now() - self.start_time).total_seconds()

    def get_estimated_remaining(self) -> Optional[float]:
        """Get estimated remaining time in seconds."""
        if self.items_processed == 0 or self.start_time is None:
            return None

        elapsed = self.get_elapsed_time()
        rate = self.items_processed / elapsed
        remaining_items = self.items_total - self.items_processed

        return remaining_items / rate

    def get_stats(self) -> Dict[str, Any]:
        """Get current statistics."""
        elapsed = self.get_elapsed_time()
        remaining = self.get_estimated_remaining()

        return {
            "items_processed": self.items_processed,
            "items_total": self.items_total,
            "progress_percent": self.get_progress_percent(),
            "compositions_generated": self.compositions_generated,
            "compositions_valid": self.compositions_valid,
            "compositions_invalid": self.compositions_invalid,
            "validation_rate": (
                self.compositions_valid / self.compositions_generated * 100
                if self.compositions_generated > 0 else 0
            ),
            "api_calls": self.api_calls,
            "api_errors": self.api_errors,
            "api_success_rate": (
                (self.api_calls - self.api_errors) / self.api_calls * 100
                if self.api_calls > 0 else 0
            ),
            "elapsed_seconds": elapsed,
            "estimated_remaining_seconds": remaining,
            "rate_items_per_minute": (
                self.items_processed / elapsed * 60 if elapsed > 0 else 0
            ),
        }

    def print_progress(self) -> None:
        """Print current progress to logger."""
        stats = self.get_stats()
        remaining = stats["estimated_remaining_seconds"]
        remaining_str = f"{remaining/60:.1f} min" if remaining else "calculating..."

        logger.info(
            f"Progress: {stats['progress_percent']:.1f}% "
            f"({stats['items_processed']}/{stats['items_total']}) | "
            f"Valid: {stats['compositions_valid']}/{stats['compositions_generated']} | "
            f"ETA: {remaining_str}"
        )
