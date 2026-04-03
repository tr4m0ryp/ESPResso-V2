"""
Output writer for Layer 4: Packaging Material Estimation.

Writes Layer4Record instances to Parquet with snappy compression,
supports checkpoint-based resumption, and can merge checkpoints into
the final output file.
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from ..config.config import Layer4Config
from ..models.models import Layer4Record

logger = logging.getLogger(__name__)

# V2 output schema: 13 columns from Layer 3 + 3 added by Layer 4
HEADERS: List[str] = [
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
    # Added by Layer 4 (3)
    "packaging_categories",
    "packaging_masses_kg",
    "packaging_reasoning",
]

# Zero-padded width used for checkpoint file names (e.g. checkpoint_000500.parquet)
_CHECKPOINT_INDEX_WIDTH = 6


def _checkpoint_stem(index: int) -> str:
    """Return the checkpoint filename stem for a given index."""
    return f"checkpoint_{index:0{_CHECKPOINT_INDEX_WIDTH}d}"


class OutputWriter:
    """Writes Layer 4 output records to Parquet files with checkpoint support."""

    def __init__(self, config: Layer4Config):
        self.config = config
        self.output_path = config.output_path
        self.checkpoint_dir = config.checkpoint_dir

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Primary write
    # ------------------------------------------------------------------

    def write_records(self, records: List[Layer4Record]) -> None:
        """Convert records to dicts, build a DataFrame, and write Parquet.

        Overwrites the final output file at config.output_path.

        Args:
            records: Layer4Record instances to persist.
        """
        if not records:
            logger.warning("write_records called with empty record list; nothing written")
            return

        record_dicts = [r.to_dict() for r in records]
        df = pd.DataFrame(record_dicts, columns=HEADERS)

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(
            self.output_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        logger.info(
            "Wrote %d records to %s", len(records), self.output_path
        )

    # ------------------------------------------------------------------
    # Checkpoint management
    # ------------------------------------------------------------------

    def write_checkpoint(
        self, records: List[Layer4Record], checkpoint_index: int
    ) -> None:
        """Write a checkpoint Parquet file and its companion metadata JSON.

        Files written:
          - <checkpoint_dir>/checkpoint_NNNNNN.parquet
          - <checkpoint_dir>/checkpoint_NNNNNN_meta.json

        The metadata file records the checkpoint_index and record count
        so callers can resume without scanning the Parquet file.

        Args:
            records: Batch of Layer4Record instances completed so far.
            checkpoint_index: The 0-based index of the *next* record to
                process (i.e. how many records have been handled in total).
        """
        if not records:
            logger.warning(
                "write_checkpoint called with empty record list at index %d; skipping",
                checkpoint_index,
            )
            return

        stem = _checkpoint_stem(checkpoint_index)
        parquet_path = self.checkpoint_dir / f"{stem}.parquet"
        meta_path = self.checkpoint_dir / f"{stem}_meta.json"

        record_dicts = [r.to_dict() for r in records]
        df = pd.DataFrame(record_dicts, columns=HEADERS)
        df.to_parquet(
            parquet_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )

        meta = {
            "checkpoint_index": checkpoint_index,
            "record_count": len(records),
        }
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        logger.info(
            "Checkpoint saved: %s (%d records, next_index=%d)",
            parquet_path,
            len(records),
            checkpoint_index,
        )

    def merge_checkpoints(self) -> None:
        """Concatenate all checkpoint Parquet files and write the final output.

        After a successful merge the checkpoint Parquet files (and their
        companion _meta.json files) are deleted from the checkpoint
        directory.

        Raises:
            FileNotFoundError: If no checkpoint files are found.
        """
        checkpoint_files = sorted(
            self.checkpoint_dir.glob("checkpoint_*.parquet")
        )
        if not checkpoint_files:
            raise FileNotFoundError(
                f"No checkpoint files found in {self.checkpoint_dir}"
            )

        frames: List[pd.DataFrame] = []
        for path in checkpoint_files:
            frames.append(pd.read_parquet(path))
            logger.debug("Loaded checkpoint %s (%d rows)", path.name, len(frames[-1]))

        merged = pd.concat(frames, ignore_index=True)
        # Re-apply canonical column order
        merged = merged.reindex(columns=HEADERS)

        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(
            self.output_path,
            engine="pyarrow",
            compression="snappy",
            index=False,
        )
        logger.info(
            "Merged %d checkpoint(s) into %s (%d total records)",
            len(checkpoint_files),
            self.output_path,
            len(merged),
        )

        # Clean up checkpoint files
        for path in checkpoint_files:
            path.unlink()
            meta = path.with_name(path.stem + "_meta.json")
            if meta.exists():
                meta.unlink()

        logger.info("Deleted %d checkpoint file(s)", len(checkpoint_files))

    def get_last_checkpoint_index(self) -> int:
        """Return the highest checkpoint index found in the checkpoint directory.

        Scans for files matching ``checkpoint_NNNNNN.parquet`` and
        extracts the numeric suffix.  Returns 0 if no checkpoints exist,
        meaning processing should start from the beginning.
        """
        checkpoint_files = list(
            self.checkpoint_dir.glob("checkpoint_*.parquet")
        )
        if not checkpoint_files:
            return 0

        max_index = 0
        for path in checkpoint_files:
            # Stem looks like "checkpoint_000500"
            stem = path.stem  # "checkpoint_000500"
            parts = stem.split("_", 1)
            if len(parts) == 2 and parts[1].isdigit():
                idx = int(parts[1])
                if idx > max_index:
                    max_index = idx

        logger.info(
            "Last checkpoint index found: %d (in %s)",
            max_index,
            self.checkpoint_dir,
        )
        return max_index

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def get_output_summary(self) -> Dict[str, Any]:
        """Read the final output Parquet and return summary statistics.

        Returns a dict with keys:
          - total_records (int)
          - columns (list[str])
          - mean_total_weight_kg (float)
          - mean_total_distance_km (float)
          - mean_packaging_mass_kg (float)
          - mean_packaging_ratio (float): mean(total_packaging / total_weight)

        Returns an empty dict with ``exists=False`` if the output file
        has not been written yet.
        """
        if not self.output_path.exists():
            return {"exists": False}

        try:
            df = pd.read_parquet(self.output_path)

            if df.empty:
                return {"exists": True, "total_records": 0, "columns": list(df.columns)}

            # Compute mean packaging mass from the packaging_masses_kg JSON column
            def _total_packaging(raw) -> float:
                """Sum the packaging masses for a single record."""
                if isinstance(raw, list):
                    return sum(float(v) for v in raw)
                try:
                    return sum(float(v) for v in json.loads(str(raw)))
                except (json.JSONDecodeError, TypeError, ValueError):
                    return 0.0

            packaging_totals = df["packaging_masses_kg"].apply(_total_packaging)
            weights = df["total_weight_kg"].astype(float)
            distances = df["total_distance_km"].astype(float)

            # Packaging ratio: avoid divide-by-zero per record
            ratios = packaging_totals / weights.replace(0, float("nan"))

            return {
                "exists": True,
                "total_records": len(df),
                "columns": list(df.columns),
                "mean_total_weight_kg": float(weights.mean()),
                "mean_total_distance_km": float(distances.mean()),
                "mean_packaging_mass_kg": float(packaging_totals.mean()),
                "mean_packaging_ratio": float(ratios.mean()),
            }

        except Exception as exc:
            logger.error("Failed to build output summary: %s", exc)
            return {"exists": True, "error": str(exc)}
