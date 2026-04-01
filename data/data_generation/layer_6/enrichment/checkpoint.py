"""Checkpoint I/O for Layer 6 enrichment pipeline.

Handles writing per-batch checkpoint CSVs, loading completed record IDs
for resume, and merging all checkpoints into a single DataFrame at the
end of processing.

Used by EnrichmentOrchestrator to persist progress between runs and
survive interruptions.
"""

import csv
import logging
import shutil
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

from data.data_generation.layer_6.enrichment.config import EnrichmentConfig

logger = logging.getLogger(__name__)

# Columns written to each checkpoint CSV
CHECKPOINT_COLUMNS = [
    'record_id', 'road_km', 'sea_km', 'rail_km',
    'air_km', 'inland_waterway_km', 'is_valid',
]

MODE_COLUMNS = ['road_km', 'sea_km', 'rail_km', 'air_km', 'inland_waterway_km']


class CheckpointManager:
    """Manages checkpoint files for the enrichment pipeline."""

    def __init__(self, config: EnrichmentConfig):
        self.config = config
        self.checkpoint_dir = Path(config.checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._batch_counter = 0
        self._records_since_checkpoint = 0
        self._pending_rows: List[Dict[str, Any]] = []
        logger.info(
            "CheckpointManager initialized, dir: %s", self.checkpoint_dir
        )

    # ------------------------------------------------------------------
    # Resume: load previously completed records
    # ------------------------------------------------------------------

    def load_completed_ids(self) -> Set[str]:
        """Scan checkpoint directory for already-processed record IDs.

        Returns:
            Set of record_id strings from valid checkpoint rows.
        """
        ids: Set[str] = set()
        checkpoint_files = sorted(self.checkpoint_dir.glob('enrichment_batch_*.csv'))

        if not checkpoint_files:
            logger.info("No existing checkpoints found -- starting fresh")
            return ids

        for fpath in checkpoint_files:
            try:
                with open(fpath, 'r', encoding='utf-8') as fh:
                    reader = csv.DictReader(fh)
                    for row in reader:
                        rid = row.get('record_id', '')
                        is_valid = row.get('is_valid', 'False')
                        if rid and is_valid.lower() == 'true':
                            ids.add(rid)
            except Exception as exc:
                logger.warning(
                    "Failed to read checkpoint %s: %s", fpath.name, exc
                )

        logger.info(
            "Loaded %d completed record IDs from %d checkpoint files",
            len(ids), len(checkpoint_files)
        )
        # Update batch counter to continue numbering from where we left off
        self._batch_counter = len(checkpoint_files)
        return ids

    # ------------------------------------------------------------------
    # Write: accumulate and flush checkpoint rows
    # ------------------------------------------------------------------

    def add_results(
        self,
        results: List[Dict[str, Any]],
        is_valid_flags: List[bool],
    ) -> None:
        """Buffer validated extraction results for checkpointing.

        Args:
            results: List of dicts with keys id, road_km, etc.
            is_valid_flags: Parallel list of validation pass/fail bools.
        """
        with self._lock:
            for result, valid in zip(results, is_valid_flags):
                row = {
                    'record_id': result.get('id', ''),
                    'road_km': result.get('road_km', 0.0),
                    'sea_km': result.get('sea_km', 0.0),
                    'rail_km': result.get('rail_km', 0.0),
                    'air_km': result.get('air_km', 0.0),
                    'inland_waterway_km': result.get('inland_waterway_km', 0.0),
                    'is_valid': valid,
                }
                self._pending_rows.append(row)
                self._records_since_checkpoint += 1

    def should_checkpoint(self) -> bool:
        """Return True if enough records have accumulated to flush."""
        with self._lock:
            return self._records_since_checkpoint >= self.config.checkpoint_interval

    def flush(self) -> Optional[str]:
        """Write pending rows to a checkpoint CSV (thread-safe).

        Returns:
            Path to the written checkpoint file, or None if nothing to write.
        """
        with self._lock:
            if not self._pending_rows:
                return None

            self._batch_counter += 1
            filename = f"enrichment_batch_{self._batch_counter:04d}.csv"
            fpath = self.checkpoint_dir / filename
            rows_to_write = list(self._pending_rows)
            self._pending_rows = []
            self._records_since_checkpoint = 0

        try:
            with open(fpath, 'w', newline='', encoding='utf-8') as fh:
                writer = csv.DictWriter(fh, fieldnames=CHECKPOINT_COLUMNS)
                writer.writeheader()
                writer.writerows(rows_to_write)

            logger.info("Checkpoint written: %s (%d rows)", filename, len(rows_to_write))
            return str(fpath)

        except Exception as exc:
            logger.error("Failed to write checkpoint %s: %s", filename, exc)
            raise

    def force_flush(self) -> Optional[str]:
        """Flush any remaining rows regardless of interval threshold."""
        return self.flush()

    # ------------------------------------------------------------------
    # Merge: combine all checkpoints into a single DataFrame
    # ------------------------------------------------------------------

    def merge_checkpoints(self) -> pd.DataFrame:
        """Load all checkpoint CSVs and merge into one DataFrame.

        Only rows with is_valid=True are included.

        Returns:
            DataFrame with columns: record_id + mode distance columns.
        """
        checkpoint_files = sorted(
            self.checkpoint_dir.glob('enrichment_batch_*.csv')
        )
        if not checkpoint_files:
            logger.warning("No checkpoint files found to merge")
            return pd.DataFrame(columns=CHECKPOINT_COLUMNS)

        frames = []
        for fpath in checkpoint_files:
            try:
                df = pd.read_csv(fpath)
                valid = df[df['is_valid'].astype(str).str.lower() == 'true']
                frames.append(valid)
            except Exception as exc:
                logger.warning("Skipping bad checkpoint %s: %s", fpath, exc)

        if not frames:
            return pd.DataFrame(columns=CHECKPOINT_COLUMNS)

        merged = pd.concat(frames, ignore_index=True)
        # Deduplicate by record_id (keep last occurrence in case of retries)
        merged = merged.drop_duplicates(subset='record_id', keep='last')

        logger.info(
            "Merged %d valid records from %d checkpoint files",
            len(merged), len(checkpoint_files)
        )
        return merged[['record_id'] + MODE_COLUMNS]

    def cleanup(self) -> None:
        """Remove checkpoint directory after successful merge."""
        try:
            shutil.rmtree(self.checkpoint_dir)
            logger.info("Cleaned up checkpoint directory: %s", self.checkpoint_dir)
        except Exception as exc:
            logger.warning("Failed to clean up checkpoints: %s", exc)
