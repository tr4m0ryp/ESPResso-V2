"""
Main orchestrator for Layer 2 generation.

Coordinates all components to generate preprocessing paths.
Supports parallel processing with rate limiting for API calls.
"""

import logging
import sys
import threading
from pathlib import Path
from typing import Optional, List

from ..config.config import Layer2Config
from ..models.processing_data import ProcessingStepsDatabase, MaterialProcessCombinations
from ..io.layer1_reader import Layer1Reader, Layer1Record
from ..clients.api_client import Layer2Client, MultiKeyClientPool
from ..core.generator import PreprocessingPathGenerator, Layer2Record
from ..core.validator import PathValidator
from ..io.output import OutputWriter, ProgressTracker
from ...shared.reality_checker import RealityChecker
from ...shared.reality_check_models import RealityCheckStats, RecordCheckResult
from ..prompts import reality_check_prompts as rc_prompts

# Support both relative and absolute imports for parallel_processor
try:
    from data.data_generation.shared.parallel_processor import ParallelProcessor, ProcessingResult
except ImportError:
    from data.data_generation.parallel_processor import ParallelProcessor, ProcessingResult

logger = logging.getLogger(__name__)


class Layer2Orchestrator:
    """
    Orchestrates the Layer 2 preprocessing path generation pipeline.

    Coordinates:
    - Reading Layer 1 output
    - Loading processing data (steps, combinations)
    - Generating preprocessing paths via API
    - Validating and correcting outputs
    - Writing to CSV with checkpointing
    - Progress tracking and reporting
    """

    def __init__(self, config: Optional[Layer2Config] = None):
        self.config = config or Layer2Config()

        # Components (initialized lazily)
        self._processing_steps_db: Optional[ProcessingStepsDatabase] = None
        self._material_process_combos: Optional[MaterialProcessCombinations] = None
        self._layer1_reader: Optional[Layer1Reader] = None
        self._client_pool: Optional[MultiKeyClientPool] = None
        self._api_client: Optional[Layer2Client] = None
        self._generator: Optional[PreprocessingPathGenerator] = None
        self._validator: Optional[PathValidator] = None
        self._output_writer: Optional[OutputWriter] = None
        self._progress: Optional[ProgressTracker] = None
        self._reality_checker: Optional[RealityChecker] = None
        self._rc_stats = RealityCheckStats()

    def initialize(self) -> None:
        """Initialize all components."""
        logger.info("Initializing Layer 2 components...")

        # Ensure directories exist
        self.config.ensure_directories()

        # Load processing steps database
        logger.info(f"Loading processing steps from {self.config.processing_steps_path}")
        self._processing_steps_db = ProcessingStepsDatabase(self.config.processing_steps_path)
        logger.info(f"Loaded {len(self._processing_steps_db)} processing steps")

        # Load material-process combinations
        logger.info(f"Loading material-process combinations from {self.config.material_process_combinations_path}")
        self._material_process_combos = MaterialProcessCombinations(
            self.config.material_process_combinations_path
        )
        logger.info(f"Loaded {len(self._material_process_combos)} material-process combinations")

        # Load Layer 1 output
        logger.info(f"Loading Layer 1 output from {self.config.layer1_output_path}")
        self._layer1_reader = Layer1Reader(self.config.layer1_output_path)
        logger.info(f"Found {self._layer1_reader.get_total_count()} Layer 1 records")

        # Initialize API client with multi-key support
        logger.info("Initializing Layer 2 API client(s)...")
        try:
            api_keys = self.config.api_keys
            if len(api_keys) > 1:
                logger.info(f"Multi-key mode: {len(api_keys)} API keys available")
                self._client_pool = MultiKeyClientPool(self.config)
                self._api_client = Layer2Client(self.config, client_pool=self._client_pool)
            else:
                logger.info("Single-key mode: 1 API key available")
                self._api_client = Layer2Client(self.config)
        except ValueError as e:
            logger.warning(f"API key setup issue: {e}")
            self._api_client = Layer2Client(self.config)

        # Create generator
        self._generator = PreprocessingPathGenerator(
            config=self.config,
            processing_steps_db=self._processing_steps_db,
            material_process_combos=self._material_process_combos,
            api_client=self._api_client
        )

        # Create validator
        self._validator = PathValidator(
            self.config,
            self._processing_steps_db,
            self._material_process_combos
        )

        # Create output writer
        self._output_writer = OutputWriter(self.config)

        # Create progress tracker
        self._progress = ProgressTracker()

        # Create reality checker using the underlying FunctionClient
        self._reality_checker = RealityChecker(
            api_client=self._api_client.client,
            format_records_fn=rc_prompts.format_batch,
            validation_prompt_fn=rc_prompts.get_validation_prompt,
            system_prompt=rc_prompts.SYSTEM_PROMPT,
        )

        logger.info("Layer 2 initialization complete")

    def _deduplicate_records(self, records: List[Layer1Record]) -> List[Layer1Record]:
        """
        Deduplicate Layer 1 records by (subcategory_id, materials) combination.

        Keeps only the first record for each unique combination to avoid
        generating duplicate preprocessing paths for identical products.
        """
        seen = set()
        unique_records = []

        for record in records:
            # Create key from subcategory and materials (sorted for consistency)
            materials_key = tuple(sorted(record.materials))
            key = (record.subcategory_id, materials_key)

            if key not in seen:
                seen.add(key)
                unique_records.append(record)

        return unique_records

    def run(
        self,
        paths_per_product: Optional[int] = None,
        start_index: int = 0,
        max_records: Optional[int] = None,
        dry_run: bool = False,
        parallel_workers: Optional[int] = None,
        requests_per_minute: Optional[int] = None
    ) -> None:
        """
        Run the full generation pipeline.

        Args:
            paths_per_product: Fixed number of paths per product (None = from config)
            start_index: Layer 1 record index to start from (for resuming)
            max_records: Maximum Layer 1 records to process (None = all)
            dry_run: If True, don't write output or call API
            parallel_workers: Number of parallel workers (None = from config, default: 80)
            requests_per_minute: API rate limit (None = auto-calculated from number of API keys)
        """
        # Use config defaults if not specified
        if parallel_workers is None:
            parallel_workers = self.config.parallel_workers
        if requests_per_minute is None:
            requests_per_minute = self.config.total_rate_limit
        if self._layer1_reader is None:
            self.initialize()

        # Get Layer 1 records and deduplicate first (before applying checkpoint),
        # so checkpoint indices refer to the stable deduped list.
        all_records = self._layer1_reader.get_all_records()
        original_count = len(all_records)
        all_records = self._deduplicate_records(all_records)
        if len(all_records) < original_count:
            logger.info(f"Deduplicated {original_count} -> {len(all_records)} unique product combinations")

        # Check for checkpoint to resume from (index into deduped list)
        checkpoint = self._output_writer.get_latest_checkpoint()
        if checkpoint and start_index == 0:
            start_index = checkpoint.get("last_layer1_index", 0) + 1
            logger.info(f"Resuming from checkpoint at deduped index {start_index}")

        records_to_process = all_records[start_index:]

        if max_records:
            records_to_process = records_to_process[:max_records]

        total_records = len(records_to_process)
        logger.info(f"Processing {total_records} Layer 1 records (starting from index {start_index})")
        logger.info(f"Parallel workers: {parallel_workers}, Rate limit: {requests_per_minute} req/min")

        if dry_run:
            logger.info("DRY RUN - no output will be written")
            self._run_dry(records_to_process, paths_per_product)
            return

        # Initialize output
        self._output_writer.initialize_output()
        self._progress.start(total_records)

        # Thread lock for safe writing
        self._write_lock = threading.Lock()

        try:
            if parallel_workers > 1:
                self._run_parallel(
                    records_to_process, 
                    start_index, 
                    paths_per_product,
                    parallel_workers,
                    requests_per_minute
                )
            else:
                # Sequential fallback
                for i, record in enumerate(records_to_process):
                    actual_index = start_index + i
                    self._process_record(record, actual_index, paths_per_product)

                    if self._output_writer.should_checkpoint():
                        self._output_writer.create_checkpoint(layer1_index=actual_index)

                    if (i + 1) % 10 == 0:
                        self._progress.print_progress()

        except KeyboardInterrupt:
            logger.warning("Generation interrupted by user")
            self._output_writer.create_checkpoint(
                layer1_index=start_index + self._progress.layer1_records_processed - 1
            )

        finally:
            self._output_writer.close()
            self._write_discard_log()
            self._print_final_stats()

    def _run_parallel(
        self,
        records: List[Layer1Record],
        start_index: int,
        paths_per_product: Optional[int],
        parallel_workers: int,
        requests_per_minute: int
    ) -> None:
        """Run generation in parallel with rate limiting."""
        logger.info(f"Starting parallel processing with {parallel_workers} workers")

        # Create parallel processor
        processor = ParallelProcessor(
            max_workers=parallel_workers,
            requests_per_minute=requests_per_minute,
            show_progress=True,
            enable_pause=True
        )

        # Prepare items with index info
        items = [(i, record, start_index + i) for i, record in enumerate(records)]

        def process_single_record(item):
            """Process a single record (called by parallel processor)."""
            idx, record, actual_index = item
            return self._process_record_parallel(record, actual_index, paths_per_product)

        def on_complete(result: ProcessingResult):
            """Callback for completed items."""
            if result.success and result.output_data:
                layer2_records = result.output_data
                with self._write_lock:
                    self._output_writer.write_batch(layer2_records)
                    self._progress.record_layer2_generated(
                        count=len(layer2_records),
                        valid_count=len(layer2_records)
                    )
                    self._progress.record_layer1_processed()

                    # Checkpoint periodically
                    if self._output_writer.should_checkpoint():
                        _, _, actual_index = result.input_data
                        self._output_writer.create_checkpoint(layer1_index=actual_index)
            else:
                with self._write_lock:
                    self._progress.record_api_call(success=False)
                    self._progress.record_layer1_processed()

        # Run parallel processing
        results = processor.process_batch(items, process_single_record, on_complete)

        # Log stats
        stats = processor.get_stats()
        logger.info(f"Parallel processing complete: {stats['success']}/{stats['total']} succeeded, "
                   f"{stats['retries']} retries, {stats.get('items_per_second', 0):.2f} items/sec")

    def _validate_with_regen(
        self,
        l1_record: Layer1Record,
        layer2_records: List[Layer2Record],
    ) -> List[Layer2Record]:
        """Validate records, regenerating those with warnings instead of correcting.

        Pass 1: validate without correction, collect records with warnings.
        Pass 2: regenerate warned records via LLM with specific feedback,
                then apply algorithmic correction as a safety net.
        Falls back to correcting originals if regeneration fails.
        """
        clean = []
        to_regen = []
        regen_feedback = []

        for i, record in enumerate(layer2_records):
            result = self._validator.validate(record)
            if not result.is_valid:
                # Structural errors (empty path_id, empty mapping) -- discard
                logger.debug("Discarding structurally broken record %d", i)
                continue
            if not result.warnings:
                clean.append(record)
            else:
                to_regen.append(record)
                regen_feedback.append(RecordCheckResult(
                    record_index=i,
                    passed=False,
                    justification="; ".join(result.warnings),
                    improvement_hint="Regenerate with valid steps in correct manufacturing order",
                    raw_record=record,
                ))

        if not to_regen:
            return clean

        logger.info(
            "Validation: %d clean, %d need regeneration for %s",
            len(clean), len(to_regen), l1_record.subcategory_id,
        )

        regen = self._generator.regenerate_with_feedback(
            l1_record, regen_feedback
        )
        if regen:
            regen_valid, _ = self._validator.batch_validate(regen, correct=True)
            return clean + regen_valid

        # Regeneration failed -- fall back to correcting originals
        logger.warning("Regeneration failed for %s, falling back to correction",
                       l1_record.subcategory_id)
        corrected, _ = self._validator.batch_validate(to_regen, correct=True)
        return clean + corrected

    def _process_record_parallel(
        self,
        record: Layer1Record,
        record_index: int,
        paths_per_product: Optional[int]
    ) -> List[Layer2Record]:
        """Process a single record for parallel execution (returns records instead of writing)."""
        logger.debug(f"Processing Layer 1 record {record_index}: {record.subcategory_id}")

        try:
            # Generate Layer 2 records
            layer2_records = self._generator.generate_layer2_records(
                record,
                num_paths=paths_per_product
            )

            if not layer2_records:
                logger.debug(f"No paths generated for record {record_index}")
                return []

            # Validate with regeneration (Approach B)
            valid_records = self._validate_with_regen(record, layer2_records)

            # Two-pass reality check
            valid_records = self._reality_check_two_pass(
                record, valid_records
            )

            return valid_records

        except Exception as e:
            logger.error(f"Error processing record {record_index}: {e}")
            return []

    def _process_record(
        self,
        record: Layer1Record,
        record_index: int,
        paths_per_product: Optional[int]
    ) -> None:
        """Process a single Layer 1 record, generating preprocessing paths."""
        logger.debug(f"Processing Layer 1 record {record_index}: {record.subcategory_id}")

        try:
            # Generate Layer 2 records
            layer2_records = self._generator.generate_layer2_records(
                record,
                num_paths=paths_per_product
            )
            self._progress.record_api_call(success=True)

            if not layer2_records:
                logger.debug(f"No paths generated for record {record_index}")
                self._progress.record_layer1_processed()
                return

            # Validate with regeneration (Approach B)
            valid_records = self._validate_with_regen(record, layer2_records)

            # Two-pass reality check
            valid_records = self._reality_check_two_pass(
                record, valid_records
            )

            # Write valid records
            for l2_record in valid_records:
                self._output_writer.write_record(l2_record)

            # Track statistics
            self._progress.record_layer2_generated(
                count=len(layer2_records),
                valid_count=len(valid_records)
            )

        except Exception as e:
            logger.error(f"Error processing record {record_index}: {e}")
            self._progress.record_api_call(success=False)

        self._progress.record_layer1_processed()

    def _reality_check_two_pass(
        self,
        l1_record: Layer1Record,
        valid_records: List[Layer2Record],
    ) -> List[Layer2Record]:
        """Run two-pass reality check on Layer 2 records."""
        if not valid_records:
            return valid_records

        check_result = self._reality_checker.check_batch(valid_records)
        final_valid = [r.raw_record for r in check_result.passed_records]

        self._rc_stats.total_checked += check_result.total_checked
        self._rc_stats.total_passed_first += len(check_result.passed_records)

        if check_result.failed_records:
            logger.info(
                "Reality check pass 1: %d/%d passed for %s",
                len(check_result.passed_records),
                check_result.total_checked,
                l1_record.subcategory_id,
            )

            self._rc_stats.total_regenerated += len(check_result.failed_records)
            regen = self._generator.regenerate_with_feedback(
                l1_record, check_result.failed_records
            )

            regen_valid, _ = self._validator.batch_validate(regen, correct=True)

            if regen_valid:
                pass2 = self._reality_checker.check_batch(regen_valid)
                final_valid.extend(
                    [r.raw_record for r in pass2.passed_records]
                )
                self._rc_stats.total_passed_second += len(pass2.passed_records)

                for d in pass2.failed_records:
                    logger.warning("Discarding (pass 2): %s", d.justification)
                    self._rc_stats.total_discarded += 1
                    self._rc_stats.discarded_log.append({
                        "layer": 2,
                        "record": l1_record.subcategory_id,
                        "justification_pass1": "regenerated",
                        "justification_pass2": d.justification,
                    })
            else:
                for f in check_result.failed_records:
                    self._rc_stats.total_discarded += 1
                    self._rc_stats.discarded_log.append({
                        "layer": 2,
                        "record": l1_record.subcategory_id,
                        "justification_pass1": f.justification,
                        "justification_pass2": "regeneration failed",
                    })

        return final_valid

    def _write_discard_log(self) -> None:
        """Write discarded records to a JSONL file."""
        import json as _json
        from datetime import datetime
        if not self._rc_stats.discarded_log:
            return
        discard_path = self.config.output_dir / "reality_check_discards.jsonl"
        with open(discard_path, "w") as f:
            for entry in self._rc_stats.discarded_log:
                entry["timestamp"] = datetime.now().isoformat()
                f.write(_json.dumps(entry) + "\n")
        logger.info("Wrote %d discards to %s", len(self._rc_stats.discarded_log), discard_path)

    def _run_dry(
        self,
        records: List[Layer1Record],
        paths_per_product: Optional[int]
    ) -> None:
        """Run a dry run without API calls or output."""
        logger.info("=== DRY RUN SUMMARY ===")
        logger.info(f"Layer 1 records to process: {len(records)}")
        
        actual_paths = paths_per_product or self.config.paths_per_product
        logger.info(f"Paths per product: {actual_paths}")
        logger.info(f"Batch size: {self.config.batch_size}")

        expected_output = len(records) * actual_paths
        logger.info(f"Expected Layer 2 output: ~{expected_output:,} records")

        # Show sample records
        logger.info("\nSample Layer 1 records to process:")
        for record in records[:5]:
            valid_steps_count = sum(
                len(self._material_process_combos.get_valid_steps_for_material(mat))
                for mat in record.materials
            )
            logger.info(
                f"  {record.subcategory_id}: {len(record.materials)} materials, "
                f"~{valid_steps_count} valid step combinations"
            )

        # Show processing stats
        logger.info("\nProcessing data stats:")
        logger.info(f"  Processing steps: {len(self._processing_steps_db)}")
        logger.info(f"  Material-process combinations: {len(self._material_process_combos)}")

        # Estimate token usage
        combo_text = self._material_process_combos.format_compact_for_prompt()
        steps_text = self._processing_steps_db.format_for_prompt()
        estimated_tokens = (len(combo_text) + len(steps_text)) // 4

        logger.info(f"\nEstimated context usage per request: ~{estimated_tokens:,} tokens")
        logger.info("")
        
        # Batch processing info
        num_batches = (len(records) + self.config.batch_size - 1) // self.config.batch_size
        logger.info(f"\nBatch processing: {num_batches} batches of {self.config.batch_size} records")

    def _print_final_stats(self) -> None:
        """Print final statistics including deduplication."""
        stats = self._progress.get_stats()
        dedup_stats = self._generator.get_deduplication_stats()

        logger.info("\n" + "=" * 60)
        logger.info("LAYER 2 GENERATION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Layer 1 records processed: {stats['layer1_processed']}")
        logger.info(f"Layer 2 records generated: {stats['layer2_generated']}")
        logger.info(f"Layer 2 records valid: {stats['layer2_valid']}")
        logger.info(f"Layer 2 records invalid: {stats['layer2_invalid']}")
        logger.info(f"Average expansion factor: {stats['expansion_factor']:.2f}x")
        logger.info(f"Validation rate: {stats['validation_rate']:.1f}%")
        logger.info(f"API calls: {stats['api_calls']}")
        logger.info(f"API errors: {stats['api_errors']}")
        logger.info("-" * 60)
        logger.info(f"Generation Stats:")
        logger.info(f"  Total paths generated: {dedup_stats['total_generated']}")
        logger.info(f"  Unique path IDs: {dedup_stats['unique_path_ids']}")

        # Reality check statistics
        rc = self._rc_stats
        if rc.total_checked > 0:
            logger.info("-" * 60)
            p1_rate = rc.total_passed_first / rc.total_checked * 100
            logger.info(f"Reality check -- Pass 1 pass rate: {p1_rate:.1f}%")
            logger.info(f"Reality check -- Guided regeneration: {rc.total_regenerated} records")
            if rc.total_regenerated > 0:
                p2_rate = rc.total_passed_second / rc.total_regenerated * 100
                logger.info(f"Reality check -- Pass 2 pass rate: {p2_rate:.1f}%")
            logger.info(f"Reality check -- Permanently discarded: {rc.total_discarded} records")

        logger.info("-" * 60)
        logger.info(f"Total time: {stats['elapsed_seconds']/60:.1f} minutes")
        logger.info(f"Output file: {self.config.output_path}")
        logger.info("=" * 60)

    def health_check(self) -> bool:
        """Verify all components are working."""
        logger.info("Running health check...")

        # Check files exist
        if not self.config.layer1_output_path.exists():
            logger.error(f"Layer 1 output not found: {self.config.layer1_output_path}")
            return False

        if not self.config.processing_steps_path.exists():
            logger.error(f"Processing steps file not found: {self.config.processing_steps_path}")
            return False

        if not self.config.material_process_combinations_path.exists():
            logger.error(f"Material-process combinations file not found: {self.config.material_process_combinations_path}")
            return False

        # Initialize components
        try:
            self.initialize()
        except Exception as e:
            logger.error(f"Initialization failed: {e}")
            return False

        # Check API
        logger.info("Checking API connection...")
        if not self._api_client.health_check():
            logger.error("API health check failed")
            return False

        logger.info("Health check passed!")
        return True


def setup_logging(verbose: bool = False) -> None:
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )


def main():
    """Main entry point for Layer 2 generation."""
    import argparse

    parser = argparse.ArgumentParser(description="Layer 2: Preprocessing Path Generator")
    parser.add_argument(
        "--paths-per-product",
        type=int,
        default=None,
        help="Fixed number of paths per product (default: from config)"
    )
    parser.add_argument(
        "--start-index",
        type=int,
        default=0,
        help="Layer 1 record index to start from"
    )
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="Maximum Layer 1 records to process"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: from PARALLEL_WORKERS env or 80)"
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=None,
        help="API requests per minute (default: auto-calculated from number of API keys)"
    )
    parser.add_argument(
        "--sequential",
        action="store_true",
        help="Disable parallel processing"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without generating or writing output"
    )
    parser.add_argument(
        "--health-check",
        action="store_true",
        help="Run health check only"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    orchestrator = Layer2Orchestrator()

    if args.health_check:
        success = orchestrator.health_check()
        sys.exit(0 if success else 1)

    orchestrator.run(
        paths_per_product=args.paths_per_product,
        start_index=args.start_index,
        max_records=args.max_records,
        dry_run=args.dry_run,
        parallel_workers=1 if args.sequential else args.workers,
        requests_per_minute=args.rate_limit
    )


if __name__ == "__main__":
    main()
