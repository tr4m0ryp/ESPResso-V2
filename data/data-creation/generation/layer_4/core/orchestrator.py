"""Layer 4 Orchestrator (V2) -- Batch + parallel packaging generation."""

import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

from data.data_generation.layer_4.config.config import Layer4Config
from data.data_generation.layer_4.models.models import Layer4Record
from data.data_generation.layer_4.clients.api_client import Layer4Client
from data.data_generation.layer_4.prompts.builder import PromptBuilder
from data.data_generation.layer_4.core.generator import PackagingGenerator
from data.data_generation.layer_4.core.validator import PackagingValidator
from data.data_generation.layer_4.core.reporting import (
    log_batch_summary,
    log_output_summary,
)
from data.data_generation.layer_4.io.input_reader import Layer3Reader
from data.data_generation.layer_4.io.writer import OutputWriter

logger = logging.getLogger(__name__)


class Layer4Orchestrator:
    """Orchestrates the Layer 4 packaging configuration generation pipeline."""

    def __init__(self, config: Optional[Layer4Config] = None):
        self.config = config or Layer4Config()
        self._reader: Optional[Layer3Reader] = None
        self._client: Optional[Layer4Client] = None
        self._prompt_builder: Optional[PromptBuilder] = None
        self._generator: Optional[PackagingGenerator] = None
        self._validator: Optional[PackagingValidator] = None
        self._writer: Optional[OutputWriter] = None

    # -- initialization ----------------------------------------------------

    def initialize(self) -> None:
        """Initialize all pipeline components. Raises on any failure."""
        logger.info("Initializing Layer 4 V2 components...")
        self.config.ensure_directories()

        self._reader = Layer3Reader(self.config)
        if not self.config.layer3_output_path.exists():
            raise FileNotFoundError(
                "Layer 3 input file not found: %s"
                % self.config.layer3_output_path
            )
        logger.info(
            "Layer3Reader: %d records at %s",
            self._reader.get_record_count(),
            self.config.layer3_output_path,
        )

        self._client = Layer4Client(self.config)
        if not self._client.test_connection():
            raise RuntimeError(
                "Layer4Client API connection test failed. "
                "Check API key and base URL."
            )

        self._prompt_builder = PromptBuilder(self.config)
        self._prompt_builder.get_system_prompt()

        self._generator = PackagingGenerator(
            config=self.config,
            api_client=self._client,
            prompt_builder=self._prompt_builder,
        )
        self._validator = PackagingValidator(self.config)
        self._writer = OutputWriter(self.config)
        logger.info("Layer 4 V2 initialization complete")

    # -- main entry point --------------------------------------------------

    def run_generation(
        self,
        max_records: Optional[int] = None,
        resume: bool = False,
        workers: Optional[int] = None,
        batch_size: Optional[int] = None,
        rate_limit: Optional[int] = None,
    ) -> bool:
        """Run the complete pipeline. Returns True on success."""
        if workers is not None:
            object.__setattr__(self.config, "_cli_parallel_workers", workers)
        if batch_size is not None:
            object.__setattr__(self.config, "products_per_batch", batch_size)
        if rate_limit is not None:
            object.__setattr__(self.config, "_cli_rate_limit", rate_limit)
        try:
            return self._run(max_records=max_records, resume=resume)
        except Exception as exc:
            logger.error("Generation pipeline failed: %s", exc, exc_info=True)
            return False

    # -- internal pipeline -------------------------------------------------

    def _run(self, max_records: Optional[int], resume: bool) -> bool:
        """Inner pipeline (raises on error)."""
        start_index = 0
        if resume:
            start_index = self._writer.get_last_checkpoint_index()
            if start_index > 0:
                logger.info("Resuming from checkpoint index %d", start_index)
            else:
                logger.info("No checkpoint found; starting from the beginning")

        if start_index > 0:
            records_iter = self._reader.read_from_checkpoint(start_index)
        else:
            records_iter = self._reader.iter_records()

        # Collect records into batches of products_per_batch
        ppb = self.config.products_per_batch
        all_records: List[Dict[str, Any]] = []
        for record in records_iter:
            if max_records is not None and len(all_records) >= max_records:
                break
            all_records.append(record)

        batches: List[List[Dict[str, Any]]] = [
            all_records[i : i + ppb]
            for i in range(0, len(all_records), ppb)
        ]

        logger.info(
            "Grouped %d records into %d batches of up to %d",
            len(all_records),
            len(batches),
            ppb,
        )

        workers = getattr(self.config, "_cli_parallel_workers", None)
        if workers is None:
            workers = self.config.parallel_workers
        rpm = getattr(self.config, "_cli_rate_limit", None)
        if rpm is None:
            rpm = self.config.effective_rate_limit

        if workers > 1:
            accepted, skipped = self._run_parallel(batches, workers, rpm)
        else:
            accepted, skipped = self._run_sequential(batches)

        total_processed = len(all_records)

        # Merge checkpoints and report
        self._writer.merge_checkpoints()
        log_batch_summary(self._validator.validate_batch_summary())
        log_output_summary(self._writer.get_output_summary())

        logger.info(
            "Generation complete: %d processed, %d accepted, %d skipped",
            total_processed,
            accepted,
            skipped,
        )
        return True

    # -- batch processing --------------------------------------------------

    def _process_single_batch(
        self, records: List[Dict[str, Any]]
    ) -> Tuple[List[Layer4Record], int]:
        """Process one batch: batch generate -> validate -> individual retry.

        Returns (accepted_records, skipped_count).
        """
        batch_results = self._generator.generate_for_batch(records)
        accepted: List[Layer4Record] = []
        skipped = 0

        for i, (record, result) in enumerate(zip(records, batch_results)):
            name = record.get("subcategory_name", "unknown")

            # Individual retry for records the batch missed
            if result is None:
                result = self._generator.generate_for_record(record)
                if result is None:
                    logger.warning("Skipping '%s': generation returned None", name)
                    skipped += 1
                    continue

            # Validate
            validation = self._validator.validate(result)
            if not validation.is_valid:
                result = self._generator.regenerate_with_feedback(
                    record, validation.errors
                )
                if result is None:
                    logger.warning("Skipping '%s': regeneration returned None", name)
                    skipped += 1
                    continue
                validation = self._validator.validate(result)
                if not validation.is_valid:
                    logger.warning(
                        "Skipping '%s' after correction: %s",
                        name,
                        "; ".join(validation.errors[:3]),
                    )
                    skipped += 1
                    continue

            accepted.append(result)
            for warning in validation.warnings:
                logger.warning("Record '%s' warning: %s", name, warning)

        return accepted, skipped

    # -- sequential runner -------------------------------------------------

    def _run_sequential(
        self, batches: List[List[Dict[str, Any]]]
    ) -> Tuple[int, int]:
        """Process batches one at a time. Returns (accepted, skipped)."""
        total_accepted = 0
        total_skipped = 0
        checkpoint_offset = 0

        for batch_idx, batch in enumerate(batches):
            accepted, skipped = self._process_single_batch(batch)
            total_accepted += len(accepted)
            total_skipped += skipped

            if accepted:
                checkpoint_offset += len(accepted)
                self._writer.write_checkpoint(accepted, checkpoint_offset)

            if (batch_idx + 1) % 10 == 0 or batch_idx == len(batches) - 1:
                logger.info(
                    "Progress: %d/%d batches, %d accepted, %d skipped",
                    batch_idx + 1,
                    len(batches),
                    total_accepted,
                    total_skipped,
                )

        return total_accepted, total_skipped

    # -- parallel runner ---------------------------------------------------

    def _run_parallel(
        self,
        batches: List[List[Dict[str, Any]]],
        workers: int,
        rpm: int,
    ) -> Tuple[int, int]:
        """Process batches in parallel using the shared ParallelProcessor."""
        from data.data_generation.shared.parallel_processor import (
            ParallelProcessor,
            ProcessingResult,
        )

        logger.info(
            "Starting parallel processing: %d workers, %d RPM",
            workers,
            rpm,
        )

        processor = ParallelProcessor(
            max_workers=workers,
            requests_per_minute=rpm,
            show_progress=True,
            enable_pause=True,
        )

        total_accepted = 0
        total_skipped = 0
        checkpoint_offset = 0
        write_lock = threading.Lock()

        def process_func(batch: List[Dict[str, Any]]):
            return self._process_single_batch(batch)

        def on_complete(result: ProcessingResult):
            nonlocal total_accepted, total_skipped, checkpoint_offset
            if not result.success or result.output_data is None:
                return
            accepted, skipped = result.output_data
            with write_lock:
                total_accepted += len(accepted)
                total_skipped += skipped
                if accepted:
                    checkpoint_offset += len(accepted)
                    self._writer.write_checkpoint(accepted, checkpoint_offset)

        processor.process_batch(batches, process_func, on_complete)

        stats = processor.get_stats()
        logger.info(
            "Parallel processing complete: %d success, %d failed, %d retries",
            stats.get("success", 0),
            stats.get("failed", 0),
            stats.get("retries", 0),
        )

        return total_accepted, total_skipped
