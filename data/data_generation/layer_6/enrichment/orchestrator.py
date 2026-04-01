"""Enrichment orchestrator for Layer 6 transport distance extraction.

Coordinates the LLM enrichment pipeline: load, batch, call, validate,
checkpoint, retry, and write the final enriched dataset.
"""

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from data.data_generation.layer_6.enrichment.checkpoint import (
    CheckpointManager, MODE_COLUMNS,
)
from data.data_generation.layer_6.enrichment.client import EnrichmentClient
from data.data_generation.layer_6.enrichment.config import EnrichmentConfig
from data.data_generation.layer_6.enrichment.data_joiner import (
    join_transport_legs,
)
from data.data_generation.layer_6.enrichment.prompt_builder import (
    build_batch_prompt, get_system_prompt,
)
from data.data_generation.layer_6.enrichment.validator import (
    FailedRecordCollector, validate_extraction,
)

logger = logging.getLogger(__name__)


class EnrichmentOrchestrator:
    """Orchestrates LLM transport distance enrichment for Layer 6."""

    def __init__(self, config: EnrichmentConfig):
        self.config = config
        self.client = EnrichmentClient(config)
        self.collector = FailedRecordCollector()
        self.checkpoint_mgr = CheckpointManager(config)
        self._system_prompt = get_system_prompt()
        self._stats_lock = threading.Lock()
        self._stats: Dict[str, int] = {
            'total_input': 0, 'already_done': 0,
            'batches_processed': 0, 'passed_validation': 0,
            'failed_validation': 0, 'retried': 0,
            'retry_passed': 0, 'skipped': 0, 'api_errors': 0,
        }

    def run(self) -> str:
        """Run the full enrichment pipeline. Returns output path."""
        start_time = time.time()
        logger.info("Starting Layer 6 transport distance enrichment")

        self.config.validate()
        df = join_transport_legs(
            self.config.layer5_path, self.config.layer4_path
        )
        self._stats['total_input'] = len(df)

        # Resume from existing checkpoints
        completed_ids = self.checkpoint_mgr.load_completed_ids()
        if completed_ids:
            before = len(df)
            df = df[~df['record_id'].isin(completed_ids)]
            self._stats['already_done'] = before - len(df)
            logger.info(
                "Resuming: %d done, %d remaining",
                self._stats['already_done'], len(df),
            )

        if len(df) > 0:
            self._process_batches(df)

        self._retry_failures()
        self.checkpoint_mgr.force_flush()

        output_path = self._merge_and_write()
        duration = time.time() - start_time
        logger.info("Enrichment complete in %.1fs: %s", duration, output_path)
        self._write_summary(output_path, duration)
        return output_path

    def _inc_stat(self, key: str, count: int = 1) -> None:
        with self._stats_lock:
            self._stats[key] += count

    def _process_batches(self, df: pd.DataFrame) -> None:
        """Process all batches with flat parallel executor."""
        records = df.to_dict('records')
        bs = self.config.batch_size
        batches = [records[i:i + bs] for i in range(0, len(records), bs)]
        num_workers = self.config.num_workers
        logger.info(
            "Processing %d records in %d batches (size %d, %d workers)",
            len(records), len(batches), bs, num_workers,
        )
        completed = 0
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(self._process_single_batch, batch): idx
                for idx, batch in enumerate(batches, 1)
            }
            for future in as_completed(futures):
                idx = futures[future]
                try:
                    future.result()
                except Exception as exc:
                    logger.error("Batch %d/%d failed: %s",
                                 idx, len(batches), exc)
                    self._inc_stat('api_errors')
                completed += 1
                self._inc_stat('batches_processed')
                if self.checkpoint_mgr.should_checkpoint():
                    self.checkpoint_mgr.flush()
                if completed % 100 == 0:
                    logger.info(
                        "Progress: %d/%d batches, %d passed, "
                        "%d failed, %d errors",
                        completed, len(batches),
                        self._stats['passed_validation'],
                        self._stats['failed_validation'],
                        self._stats['api_errors'],
                    )
        self.checkpoint_mgr.force_flush()

    def _process_single_batch(self, batch: List[Dict]) -> None:
        """Run LLM extraction + validation on one batch."""
        prompt = build_batch_prompt(batch)
        try:
            extractions = self.client.extract_transport_distances(
                self._system_prompt, prompt
            )
        except Exception as exc:
            logger.warning("API failed for %d records: %s", len(batch), exc)
            self._stats['api_errors'] += 1
            for rec in batch:
                self.collector.add_failure(rec, None)
            return

        results, flags = self._validate_extractions(batch, extractions)
        if results:
            self.checkpoint_mgr.add_results(results, flags)

    def _validate_extractions(
        self,
        batch: List[Dict],
        extractions: List[Dict],
        is_retry: bool = False,
    ) -> Tuple[List[Dict], List[bool]]:
        """Match extractions to batch records and validate each.

        Returns (valid_results, valid_flags) for checkpoint storage.
        """
        extraction_map = {str(e.get('id', '')): e for e in extractions}
        valid_results: List[Dict] = []
        valid_flags: List[bool] = []

        pass_key = 'retry_passed' if is_retry else 'passed_validation'
        fail_key = 'skipped' if is_retry else 'failed_validation'

        for rec in batch:
            rid = str(rec.get('record_id', ''))
            extracted = extraction_map.get(rid)

            if extracted is None:
                if not is_retry:
                    self.collector.add_failure(rec, None)
                self._inc_stat(fail_key)
                continue

            total_km = float(rec.get('total_distance_km', 0.0))
            vr = validate_extraction(
                extracted, total_km, self.config.distance_tolerance
            )
            valid_results.append(extracted)
            valid_flags.append(vr.is_valid)

            if vr.is_valid:
                self._inc_stat(pass_key)
            else:
                if not is_retry:
                    self.collector.add_failure(rec, vr)
                self._inc_stat(fail_key)

        return valid_results, valid_flags

    def _retry_failures(self) -> None:
        """Retry all failed records once. Records that fail twice are skipped."""
        retry_batch = self.collector.get_retry_batch()
        if not retry_batch:
            return
        self._stats['retried'] = len(retry_batch)
        logger.info("Retrying %d failed records", len(retry_batch))
        bs = self.config.batch_size
        batches = [retry_batch[i:i + bs] for i in range(0, len(retry_batch), bs)]
        for batch in batches:
            prompt = build_batch_prompt(batch)
            try:
                extractions = self.client.extract_transport_distances(
                    self._system_prompt, prompt
                )
            except Exception as exc:
                logger.warning("Retry API call failed: %s", exc)
                self._stats['skipped'] += len(batch)
                continue

            results, flags = self._validate_extractions(
                batch, extractions, is_retry=True
            )
            if results:
                self.checkpoint_mgr.add_results(results, flags)

        logger.info("Retry: %d passed, %d skipped",
                    self._stats['retry_passed'], self._stats['skipped'])

    def _merge_and_write(self) -> str:
        """Merge checkpoint data into the full dataset and write parquet."""
        full_df = join_transport_legs(
            self.config.layer5_path, self.config.layer4_path
        )
        enriched = self.checkpoint_mgr.merge_checkpoints()

        if enriched.empty:
            logger.warning("No enriched data -- writing base dataset")
            full_df.to_parquet(
                self.config.output_path, compression='gzip', index=False)
            return self.config.output_path
        merged = full_df.merge(enriched, on='record_id', how='left')
        for col in MODE_COLUMNS:
            merged[col] = merged[col].fillna(0.0)

        output_path = self.config.output_path
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(output_path, compression='gzip', index=False)
        logger.info(
            "Wrote enriched dataset: %s (%d rows, %d cols)",
            output_path, len(merged), len(merged.columns),
        )
        return output_path

    def _write_summary(self, output_path: str, duration: float) -> str:
        """Write a JSON summary of the enrichment run."""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'duration_seconds': round(duration, 1),
            'output_file': output_path,
            'stats': self._stats,
            'config': {
                'batch_size': self.config.batch_size,
                'checkpoint_interval': self.config.checkpoint_interval,
                'max_retries': self.config.max_retries,
                'distance_tolerance': self.config.distance_tolerance,
                'model': self.config.api_model,
            },
        }
        summary_path = str(
            Path(self.config.output_dir) / 'enrichment_summary.json'
        )
        Path(summary_path).parent.mkdir(parents=True, exist_ok=True)
        with open(summary_path, 'w', encoding='utf-8') as fh:
            json.dump(summary, fh, indent=2)
        logger.info("Wrote enrichment summary: %s", summary_path)
        return summary_path
