"""
Input reader for Layer 4: Packaging Material Estimation.

Reads Layer 3 Parquet output and exposes it as dicts ready for
Layer4Record.from_layer3() construction.
"""

import logging
from typing import Any, Dict, Iterator

import pandas as pd

from ..config.config import Layer4Config

logger = logging.getLogger(__name__)


class Layer3Reader:
    """Reads Layer 3 transport scenario output for Layer 4 consumption."""

    def __init__(self, config: Layer4Config):
        self.config = config
        self.input_path = config.layer3_output_path

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def read_all(self) -> pd.DataFrame:
        """Read the full Layer 3 Parquet file and return a DataFrame.

        Logs the number of records loaded.

        Raises:
            FileNotFoundError: If the Layer 3 output file does not exist.
        """
        if not self.input_path.exists():
            raise FileNotFoundError(
                f"Layer 3 output not found: {self.input_path}"
            )

        df = pd.read_parquet(self.input_path)
        logger.info(
            "Read %d records from Layer 3 output: %s",
            len(df),
            self.input_path,
        )
        return df

    def iter_records(self) -> Iterator[Dict[str, Any]]:
        """Yield each Layer 3 record as a plain dict.

        Fields are returned exactly as stored in the Parquet file —
        list/dict fields may be JSON strings or native Python objects
        depending on how Layer 3 wrote them.  Layer4Record.from_layer3()
        handles both representations.
        """
        df = self.read_all()
        for record in df.to_dict(orient="records"):
            yield record

    def get_record_count(self) -> int:
        """Return the number of records without loading the full DataFrame."""
        if not self.input_path.exists():
            raise FileNotFoundError(
                f"Layer 3 output not found: {self.input_path}"
            )

        # read_parquet with columns=[] still fetches row-group metadata;
        # using pyarrow directly avoids loading any column data.
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(self.input_path)
        return pf.metadata.num_rows

    def read_from_checkpoint(self, start_index: int) -> Iterator[Dict[str, Any]]:
        """Yield records starting from *start_index* (0-based), skipping earlier rows.

        Args:
            start_index: Number of leading records to skip.  Pass 0 to
                iterate all records.
        """
        df = self.read_all()
        if start_index >= len(df):
            logger.info(
                "Checkpoint start_index %d >= total records %d; nothing to process",
                start_index,
                len(df),
            )
            return

        logger.info(
            "Resuming from checkpoint: skipping %d records, processing %d remaining",
            start_index,
            len(df) - start_index,
        )
        for record in df.iloc[start_index:].to_dict(orient="records"):
            yield record
