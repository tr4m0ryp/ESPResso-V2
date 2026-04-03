"""Layer 3 Orchestrator (V2) -- Transport Leg Generation Pipeline."""

import argparse
import logging
import sys
import threading
from typing import Any, Dict, List, Optional

from data.data_generation.layer_3.config.config import Layer3Config
from data.data_generation.layer_3.models.models import Layer3Record
from data.data_generation.layer_3.core.generator import TransportGenerator
from data.data_generation.layer_3.core.deterministic_validator import DeterministicValidator
from data.data_generation.layer_3.core.semantic_validator import SemanticValidator
from data.data_generation.layer_3.core.statistical_validator import StatisticalValidator
from data.data_generation.layer_3.prompts.builder import PromptBuilder
from data.data_generation.layer_3.clients.api_client import Layer3Client
from data.data_generation.layer_3.io.output import OutputWriter, ProgressTracker
from data.data_generation.layer_3.io.layer2_reader import Layer2DataReader, Layer2Record
from data.data_generation.shared.parallel_processor import ParallelProcessor, ProcessingResult

logger = logging.getLogger(__name__)


class Layer3Orchestrator:
    """Orchestrates the V2 Layer 3 transport leg generation pipeline."""

    def __init__(self, config: Optional[Layer3Config] = None):
        self.config = config or Layer3Config()
        self._layer2_reader: Optional[Layer2DataReader] = None
        self._api_client: Optional[Layer3Client] = None
        self._generator: Optional[TransportGenerator] = None
        self._deterministic_validator: Optional[DeterministicValidator] = None
        self._semantic_validator: Optional[SemanticValidator] = None
        self._statistical_validator: Optional[StatisticalValidator] = None
        self._prompt_builder: Optional[PromptBuilder] = None
        self._output_writer: Optional[OutputWriter] = None
        self._progress_tracker: Optional[ProgressTracker] = None
        self._discarded_log: List[Dict[str, Any]] = []

    # -- initialization ----------------------------------------------------

    def initialize(self) -> None:
        """Initialize all V2 components."""
        logger.info("Initializing Layer 3 V2 components...")
        self.config.ensure_directories()

        self._layer2_reader = Layer2DataReader(self.config.layer2_output_path)
        record_count = self._layer2_reader.get_record_count()
        if record_count == 0:
            raise ValueError("No Layer 2 records found")
        logger.info("Found %d Layer 2 records", record_count)

        self._init_api_client()
        if self._api_client is None:
            raise RuntimeError("API client required for V2 pipeline")

        self._prompt_builder = PromptBuilder(self.config)
        self._prompt_builder.get_system_prompt()  # cache it

        self._generator = TransportGenerator(
            config=self.config,
            api_client=self._api_client,
            prompt_builder=self._prompt_builder,
        )
        self._deterministic_validator = DeterministicValidator(self.config)
        self._semantic_validator = SemanticValidator(
            self.config, self._api_client
        )
        self._statistical_validator = StatisticalValidator(self.config)
        self._progress_tracker = ProgressTracker(total_records=record_count)
        self._output_writer = OutputWriter(self.config, self._progress_tracker)
        logger.info("Layer 3 V2 initialization complete")

    def _init_api_client(self) -> None:
        """Attempt to initialize the API client."""
        if not self.config.has_api_key():
            logger.info("No API key configured")
            return
        try:
            self._api_client = Layer3Client(self.config)
            if not self._api_client.test_connection():
                logger.warning("API connection test failed")
                self._api_client = None
        except Exception as exc:
            logger.warning("Failed to initialize API client: %s", exc)
            self._api_client = None

    # -- main entry point --------------------------------------------------

    def run_generation(
        self,
        batch_size: Optional[int] = None,
        max_records: Optional[int] = None,
        resume_from_checkpoint: Optional[str] = None,
        parallel_workers: Optional[int] = None,
        requests_per_minute: Optional[int] = None,
        retry_failed: bool = False,
    ) -> bool:
        """Run the complete V2 pipeline. Returns True on success."""
        if parallel_workers is None:
            parallel_workers = self.config.parallel_workers
        if requests_per_minute is None:
            requests_per_minute = self.config.effective_rate_limit
        try:
            if not self._is_initialized():
                self.initialize()
            batch_size = batch_size or self.config.batch_size

            logger.info("Starting V2 generation: workers=%d, rpm=%d",
                        parallel_workers, requests_per_minute)

            start_record = 0
            if resume_from_checkpoint:
                cp = self._output_writer.load_checkpoint(resume_from_checkpoint)
                if cp:
                    start_record = len(cp)
                    logger.info("Resuming from record %d", start_record)

            all_records = self._layer2_reader.read_all_records()
            if max_records:
                all_records = all_records[:max_records]

            if retry_failed:
                all_records = self._filter_to_failed(all_records)
                logger.info("Retry mode: %d failed records to process",
                            len(all_records))

            self._progress_tracker.total_records = len(all_records)
            records = all_records[start_record:]
            self._write_lock = threading.Lock()

            if parallel_workers > 1:
                ok_count = self._run_parallel(
                    records, parallel_workers, requests_per_minute)
            else:
                ok_count = self._run_sequential(records, batch_size)

            print()
            self._log_batch_statistics()
            self._write_discard_log()
            logger.info("Generation complete: %d records", ok_count)
            return True
        except Exception as exc:
            logger.error("Generation failed: %s", exc)
            return False

    # -- per-record pipeline -----------------------------------------------

    def _process_single_record(
        self, record: Layer2Record,
    ) -> Optional[Layer3Record]:
        """Process one record through generate -> validate -> accept/reject."""
        pid = record.preprocessing_path_id

        # Generate
        try:
            l3 = self._generator.generate_for_record(record)
        except Exception as exc:
            logger.error("Generation failed for %s: %s", pid, exc)
            return None

        # Deterministic validate + correct
        det = self._deterministic_validator.validate_and_correct(l3)
        if not det.is_valid:
            logger.warning("Deterministic reject %s: %s",
                           pid, "; ".join(det.errors[:3]))
            return None
        rec = det.corrected_record or l3

        # Semantic validate (two-pass)
        sem = self._semantic_validator.validate(rec)
        if sem.recommendation == "reject":
            regen = self._generator.regenerate_with_feedback(
                record, sem.issues_found)
            if regen:
                sem2 = self._semantic_validator.validate(regen)
                if sem2.recommendation != "reject":
                    return regen
            logger.warning("Discarding %s after failed regeneration", pid)
            self._discarded_log.append({
                "layer": 3,
                "record": pid,
                "justification_pass1": "; ".join(sem.issues_found[:3]) if sem.issues_found else "semantic reject",
                "justification_pass2": "regeneration failed",
            })
            return None
        return rec

    # -- parallel processing -----------------------------------------------

    def _run_parallel(self, records: List[Layer2Record],
                      workers: int, rpm: int) -> int:
        """Run generation in parallel with rate limiting."""
        processor = ParallelProcessor(
            max_workers=workers, requests_per_minute=rpm,
            show_progress=True, enable_pause=True,
        )
        success_count = 0
        cp_counter = 0
        cp_interval = getattr(self.config, "checkpoint_interval", 5000)

        def process_fn(record: Layer2Record) -> Optional[Layer3Record]:
            try:
                return self._process_single_record(record)
            except Exception as exc:
                logger.error("Error processing %s: %s",
                             record.preprocessing_path_id, exc)
                return None

        def on_complete(result: ProcessingResult) -> None:
            nonlocal success_count, cp_counter
            if result.success and result.output_data is not None:
                rec = result.output_data
                with self._write_lock:
                    n = self._output_writer.write_records([rec])
                    if n:
                        success_count += n
                        cp_counter += n
                        self._statistical_validator.validate_record(rec)
                    self._progress_tracker.update(processed=1)
                    if cp_counter >= cp_interval:
                        self._output_writer.write_checkpoint(
                            [rec], "parallel_%d" % success_count)
                        cp_counter = 0
            else:
                with self._write_lock:
                    self._progress_tracker.update(failed=1)

        processor.process_batch(records, process_fn, on_complete)

        stats = processor.get_stats()
        logger.info("Parallel done: %d/%d ok, %d retries, %.2f items/s",
                     stats.get("success", 0), stats.get("total", 0),
                     stats.get("retries", 0),
                     stats.get("items_per_second", 0))
        return success_count

    # -- sequential fallback -----------------------------------------------

    def _run_sequential(self, records: List[Layer2Record],
                        batch_size: int) -> int:
        """Process records sequentially in batches."""
        success_count = 0
        total = len(records)
        for bn, si in enumerate(range(0, total, batch_size)):
            ei = min(si + batch_size, total)
            batch = records[si:ei]
            results: List[Layer3Record] = []
            for record in batch:
                r = self._process_single_record(record)
                if r is not None:
                    results.append(r)
                    self._statistical_validator.validate_record(r)
            if results:
                written = self._output_writer.write_records(results)
                success_count += written
                self._output_writer.write_checkpoint(
                    results, "batch_%04d" % (bn + 1))
            self._progress_tracker.update(processed=len(batch))
            self._progress_tracker.print_progress()
        return success_count

    def _log_batch_statistics(self) -> None:
        """Log statistical batch summary after all records are processed."""
        s = self._statistical_validator.get_batch_summary()
        logger.info("=== Statistical Batch Summary ===")
        logger.info("Records: %d total, %d unique, %d exact dup, %d near dup",
                     s.get("total_records", 0), s.get("unique_records", 0),
                     s.get("exact_duplicates", 0), s.get("near_duplicates", 0))
        ds = s.get("distance_statistics", {})
        if ds:
            logger.info("Distance -- mean:%.1f med:%.1f min:%.1f max:%.1f sd:%.1f km",
                         ds.get("mean",0), ds.get("median",0), ds.get("min",0),
                         ds.get("max",0), ds.get("stdev",0))
        if s.get("missing_modes"):
            logger.warning("Missing modes: %s", ", ".join(s["missing_modes"]))

    def _write_discard_log(self) -> None:
        """Write discarded records to a JSONL file."""
        import json as _json
        from datetime import datetime
        if not self._discarded_log:
            return
        discard_path = self.config.output_dir / "reality_check_discards.jsonl"
        with open(discard_path, "w") as f:
            for entry in self._discarded_log:
                entry["timestamp"] = datetime.now().isoformat()
                f.write(_json.dumps(entry) + "\n")
        logger.info("Wrote %d discards to %s",
                     len(self._discarded_log), discard_path)

    def _filter_to_failed(self, all_records: List[Layer2Record]) -> List[Layer2Record]:
        """Filter to only records not yet in the output file."""
        import csv
        output_path = self.config.output_path
        if not output_path.exists():
            return all_records
        existing_ids = set()
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    existing_ids.add(row.get("preprocessing_path_id", ""))
        except Exception as e:
            logger.warning("Could not read output for retry filter: %s", e)
            return all_records
        failed = [r for r in all_records if r.preprocessing_path_id not in existing_ids]
        logger.info("Found %d existing, %d missing from %d total",
                     len(existing_ids), len(failed), len(all_records))
        return failed

    def _is_initialized(self) -> bool:
        return all([self._layer2_reader, self._api_client, self._generator,
                    self._deterministic_validator, self._semantic_validator,
                    self._statistical_validator, self._prompt_builder,
                    self._output_writer, self._progress_tracker])

    def get_progress_stats(self) -> Dict[str, Any]:
        return self._progress_tracker.get_stats() if self._progress_tracker else {}


def main():
    """CLI entry point for Layer 3 V2 generation."""
    p = argparse.ArgumentParser(description="Layer 3 Transport Leg Generator (V2)")
    p.add_argument("--batch-size", type=int)
    p.add_argument("--max-records", type=int)
    p.add_argument("--resume-from", type=str)
    p.add_argument("--workers", type=int)
    p.add_argument("--rate-limit", type=int)
    p.add_argument("--retry-failed", action="store_true",
                   help="Only process records missing from existing output")
    p.add_argument("--log-level", default="INFO",
                   choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = p.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    try:
        ok = Layer3Orchestrator().run_generation(
            batch_size=args.batch_size, max_records=args.max_records,
            resume_from_checkpoint=args.resume_from,
            parallel_workers=args.workers, requests_per_minute=args.rate_limit,
            retry_failed=args.retry_failed)
        if not ok:
            sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(1)


if __name__ == "__main__":
    main()
