"""
Main orchestrator for Layer 1 generation.

Coordinates all components to generate product compositions.
Supports batch generation (100 products per API call) and parallel processing.
"""

import logging
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Optional, List, Tuple

from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

from ..config.config import Layer1Config
from ..models.materials import MaterialDatabase, MaterialCategoryMapper
from ..models.taxonomy import TaxonomyLoader, TaxonomyItem
from ..clients.api_client import Layer1Client
from .generator import (
    ProductCompositionGenerator,
    ProductComposition,
    composition_fingerprint,
    deduplicate_batch,
    verify_batch_count,
)
from .validator import CompositionValidator
from ..io.output import OutputWriter, ProgressTracker
from ...shared.reality_checker import RealityChecker
from ...shared.reality_check_models import RealityCheckStats
from ..prompts import reality_check_prompts as rc_prompts

logger = logging.getLogger(__name__)


class Layer1Orchestrator:
    """
    Orchestrates the Layer 1 product composition generation pipeline.

    Coordinates:
    - Loading reference data (taxonomy, materials)
    - Generating compositions via API
    - Validating and normalizing outputs
    - Writing to CSV with checkpointing
    - Progress tracking and reporting
    """

    def __init__(self, config: Optional[Layer1Config] = None):
        self.config = config or Layer1Config()

        # Components (initialized lazily)
        self._material_db: Optional[MaterialDatabase] = None
        self._category_mapper: Optional[MaterialCategoryMapper] = None
        self._taxonomy: Optional[TaxonomyLoader] = None
        self._api_client: Optional[Layer1Client] = None
        self._generator: Optional[ProductCompositionGenerator] = None
        self._validator: Optional[CompositionValidator] = None
        self._output_writer: Optional[OutputWriter] = None
        self._progress: Optional[ProgressTracker] = None
        self._reality_checker: Optional[RealityChecker] = None
        self._rc_stats = RealityCheckStats()

    def initialize(self) -> None:
        """Initialize all components."""
        logger.info("Initializing Layer 1 components...")

        # Ensure directories exist
        self.config.ensure_directories()

        # Load material database
        logger.info(f"Loading materials from {self.config.materials_path}")
        self._material_db = MaterialDatabase(self.config.materials_path)
        logger.info(f"Loaded {len(self._material_db)} materials")

        # Create category mapper
        self._category_mapper = MaterialCategoryMapper(self._material_db)
        logger.info(f"Material categories: {self._category_mapper.get_category_stats()}")

        # Load taxonomy
        logger.info(f"Loading taxonomy from {self.config.taxonomy_path}")
        self._taxonomy = TaxonomyLoader(self.config.taxonomy_path)
        logger.info(f"Taxonomy stats: {self._taxonomy.get_stats()}")

        # Initialize API client
        logger.info("Initializing API client...")
        self._api_client = Layer1Client(self.config)

        # Create generator
        self._generator = ProductCompositionGenerator(
            config=self.config,
            material_db=self._material_db,
            category_mapper=self._category_mapper,
            taxonomy=self._taxonomy,
            api_client=self._api_client
        )

        # Create validator
        self._validator = CompositionValidator(self.config, self._material_db)

        # Create output writer
        self._output_writer = OutputWriter(self.config)

        # Create progress tracker
        self._progress = ProgressTracker()

        # Create reality checker using the same underlying FunctionClient
        self._reality_checker = RealityChecker(
            api_client=self._api_client.client,
            format_records_fn=rc_prompts.format_batch,
            validation_prompt_fn=rc_prompts.get_validation_prompt,
            system_prompt=rc_prompts.SYSTEM_PROMPT,
        )

        logger.info("Layer 1 initialization complete")

    def run(
        self,
        products_per_category: int = 100,
        categories: Optional[List[str]] = None,
        dry_run: bool = False,
        parallel_workers: int = 4,
        use_batch: bool = True
    ) -> None:
        """
        Run the full generation pipeline.

        Args:
            products_per_category: Number of products to generate per category
            categories: Specific category IDs to process (None = all)
            dry_run: If True, don't write output or call API
            parallel_workers: Number of parallel workers for processing categories
            use_batch: If True, generate all products per category in one API call
        """
        if self._taxonomy is None:
            self.initialize()

        # Get items to process
        if categories:
            items = [
                item for item in self._taxonomy.get_all_generation_targets()
                if item.full_id in categories or item.subcategory_id in categories
            ]
        else:
            items = self._taxonomy.get_all_generation_targets()

        total_items = len(items)
        logger.info(f"Processing {total_items} taxonomy items, {products_per_category} products each")
        logger.info(f"Mode: {'BATCH' if use_batch else 'SEQUENTIAL'} generation with {parallel_workers} parallel workers")

        if dry_run:
            logger.info("DRY RUN - no output will be written")
            self._run_dry(items, products_per_category)
            return

        # Initialize output
        self._output_writer.initialize_output()
        self._progress.start(total_items * products_per_category)

        # Lock for thread-safe output writing
        self._write_lock = threading.Lock()

        try:
            if parallel_workers > 1:
                self._run_parallel(items, products_per_category, parallel_workers, use_batch)
            else:
                self._run_sequential(items, products_per_category, use_batch)

        except KeyboardInterrupt:
            logger.warning("Generation interrupted by user")
            self._output_writer.create_checkpoint()

        finally:
            self._output_writer.close()
            self._write_discard_log()
            self._print_final_stats()

    def _run_sequential(
        self,
        items: List[TaxonomyItem],
        products_per_category: int,
        use_batch: bool
    ) -> None:
        """Run generation sequentially (single-threaded)."""
        for item in items:
            if use_batch:
                self._process_item_batch(item, products_per_category)
            else:
                self._process_item(item, products_per_category)

            if self._output_writer.should_checkpoint():
                self._output_writer.create_checkpoint()

            self._progress.print_progress()

    def _run_parallel(
        self,
        items: List[TaxonomyItem],
        products_per_category: int,
        parallel_workers: int,
        use_batch: bool
    ) -> None:
        """Run generation in parallel across multiple workers."""
        logger.info(f"Starting parallel processing with {parallel_workers} workers")

        with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
            # Submit all items to the thread pool
            if use_batch:
                futures = {
                    executor.submit(self._process_item_batch_parallel, item, products_per_category): item
                    for item in items
                }
            else:
                futures = {
                    executor.submit(self._process_item_parallel, item, products_per_category): item
                    for item in items
                }

            # Process results as they complete
            completed = 0
            for future in as_completed(futures):
                item = futures[future]
                try:
                    compositions, valid_count, invalid_count = future.result()

                    # Thread-safe writing
                    with self._write_lock:
                        for composition in compositions:
                            self._output_writer.write_composition(composition)

                        # Update progress
                        for _ in range(valid_count):
                            self._progress.record_composition(valid=True)
                        for _ in range(invalid_count):
                            self._progress.record_composition(valid=False)

                        completed += 1
                        if completed % 5 == 0 or completed == len(items):
                            self._progress.print_progress()

                        if self._output_writer.should_checkpoint():
                            self._output_writer.create_checkpoint()

                except Exception as e:
                    logger.error(f"Error processing {item.full_id}: {e}")

    def _process_item_batch_parallel(
        self,
        item: TaxonomyItem,
        count: int
    ) -> Tuple[List[ProductComposition], int, int]:
        """Process a single item using stratified batch generation (for parallel execution)."""
        logger.info("[Parallel] Processing: %s - %s (%d products)", item.full_id, item.full_name, count)

        seen_fingerprints = set()
        valid_compositions = []
        invalid_count = 0

        try:
            # Single stratified batch call
            compositions = self._generator.generate_batch_for_item(item, count)
            self._progress.record_api_call(success=True)

            # Count verification
            compositions, shortfall = verify_batch_count(compositions, count, item.full_id)

            # Structural validation
            validated = []
            for comp in compositions:
                result = self._validator.validate_and_normalize(comp)
                if result.is_valid and result.normalized_composition:
                    validated.append(result.normalized_composition)
                else:
                    invalid_count += 1

            # Deduplication
            unique, dupes_removed = deduplicate_batch(validated, seen_fingerprints)
            valid_compositions.extend(unique)

            # Fill shortfall (from count verification + dedup + validation failures)
            total_shortfall = count - len(valid_compositions)
            if total_shortfall > 0:
                logger.info(
                    "[Parallel] %s: filling %d missing records",
                    item.full_id, total_shortfall,
                )
                fill = self._generator.generate_fill_batch(
                    item, total_shortfall, seen_fingerprints
                )
                self._progress.record_api_call(success=True)
                for comp in fill:
                    result = self._validator.validate_and_normalize(comp)
                    if result.is_valid and result.normalized_composition:
                        fp = composition_fingerprint(result.normalized_composition)
                        if fp not in seen_fingerprints:
                            seen_fingerprints.add(fp)
                            valid_compositions.append(result.normalized_composition)
                        else:
                            invalid_count += 1
                    else:
                        invalid_count += 1

            # Two-pass reality check
            valid_compositions = self._reality_check_two_pass(
                item, valid_compositions
            )
            valid_count = len(valid_compositions)

            logger.info(
                "[Parallel] %s: %d final, %d deduped, %d structurally invalid",
                item.full_id, valid_count, dupes_removed, invalid_count,
            )

        except Exception as e:
            logger.error("Error in batch processing %s: %s", item.full_id, e)
            self._progress.record_api_call(success=False)

        return valid_compositions, len(valid_compositions), invalid_count

    def _process_item_parallel(
        self,
        item: TaxonomyItem,
        count: int
    ) -> Tuple[List[ProductComposition], int, int]:
        """Process a single item one product at a time (for parallel execution)."""
        logger.info(f"[Parallel] Processing: {item.full_id} - {item.full_name} ({count} products)")

        valid_compositions = []
        valid_count = 0
        invalid_count = 0

        for i in range(count):
            try:
                composition = self._generator.generate_for_item(item)
                self._progress.record_api_call(success=True)

                if composition is None:
                    invalid_count += 1
                    continue

                result = self._validator.validate_and_normalize(composition)
                if result.is_valid and result.normalized_composition:
                    valid_compositions.append(result.normalized_composition)
                    valid_count += 1
                else:
                    invalid_count += 1

            except Exception as e:
                logger.error(f"Error processing {item.full_id}: {e}")
                self._progress.record_api_call(success=False)
                invalid_count += 1

        # Two-pass reality check
        valid_compositions = self._reality_check_two_pass(
            item, valid_compositions
        )
        valid_count = len(valid_compositions)

        return valid_compositions, valid_count, invalid_count

    def _process_item_batch(self, item: TaxonomyItem, count: int) -> None:
        """Process a single taxonomy item using stratified batch generation."""
        logger.info("Processing (batch): %s - %s (%d products)", item.full_id, item.full_name, count)

        seen_fingerprints = set()

        try:
            # Single stratified batch call
            compositions = self._generator.generate_batch_for_item(item, count)
            self._progress.record_api_call(success=True)

            # Count verification
            compositions, shortfall = verify_batch_count(compositions, count, item.full_id)

            # Structural validation
            validated = []
            for composition in compositions:
                result = self._validator.validate_and_normalize(composition)
                if result.is_valid and result.normalized_composition:
                    validated.append(result.normalized_composition)
                else:
                    self._progress.record_composition(valid=False)

            # Deduplication
            unique, dupes_removed = deduplicate_batch(validated, seen_fingerprints)
            valid_compositions = list(unique)

            # Fill shortfall
            total_shortfall = count - len(valid_compositions)
            if total_shortfall > 0:
                logger.info("Filling %d missing products for %s", total_shortfall, item.full_id)
                fill = self._generator.generate_fill_batch(
                    item, total_shortfall, seen_fingerprints
                )
                self._progress.record_api_call(success=True)
                for comp in fill:
                    result = self._validator.validate_and_normalize(comp)
                    if result.is_valid and result.normalized_composition:
                        fp = composition_fingerprint(result.normalized_composition)
                        if fp not in seen_fingerprints:
                            seen_fingerprints.add(fp)
                            valid_compositions.append(result.normalized_composition)

            # Two-pass reality check
            valid_compositions = self._reality_check_two_pass(
                item, valid_compositions
            )

            # Write survivors
            for comp in valid_compositions:
                self._output_writer.write_composition(comp)
                self._progress.record_composition(valid=True)

            valid_count = len(valid_compositions)
            logger.info(
                "Batch completed for %s: %d/%d valid, %d deduped",
                item.full_id, valid_count, count, dupes_removed,
            )

        except Exception as e:
            logger.error("Error in batch processing %s: %s", item.full_id, e)
            self._progress.record_api_call(success=False)

        self._progress.record_item_processed()

    def _process_item(self, item: TaxonomyItem, count: int) -> None:
        """Process a single taxonomy item, generating multiple compositions."""
        logger.info(f"Processing: {item.full_id} - {item.full_name} ({count} products)")

        valid_compositions = []

        for i in range(count):
            try:
                composition = self._generator.generate_for_item(item)
                self._progress.record_api_call(success=True)

                if composition is None:
                    self._progress.record_composition(valid=False)
                    continue

                result = self._validator.validate_and_normalize(composition)
                if result.is_valid and result.normalized_composition:
                    valid_compositions.append(result.normalized_composition)
                else:
                    self._progress.record_composition(valid=False)

            except Exception as e:
                logger.error(f"Error processing {item.full_id}: {e}")
                self._progress.record_api_call(success=False)
                self._progress.record_composition(valid=False)

        # Two-pass reality check
        valid_compositions = self._reality_check_two_pass(
            item, valid_compositions
        )

        # Write survivors
        for comp in valid_compositions:
            self._output_writer.write_composition(comp)
            self._progress.record_composition(valid=True)

        self._progress.record_item_processed()

    def _reality_check_two_pass(
        self,
        item: 'TaxonomyItem',
        valid_compositions: List[ProductComposition],
    ) -> List[ProductComposition]:
        """Run two-pass reality check: validate, regenerate failures, re-validate."""
        if not valid_compositions:
            return valid_compositions

        # Pass 1
        check_result = self._reality_checker.check_batch(valid_compositions)
        final_valid = [r.raw_record for r in check_result.passed_records]

        self._rc_stats.total_checked += check_result.total_checked
        self._rc_stats.total_passed_first += len(check_result.passed_records)

        if check_result.failed_records:
            logger.info(
                "Reality check pass 1: %d/%d passed for %s",
                len(check_result.passed_records),
                check_result.total_checked,
                item.full_id,
            )

            # Guided regeneration
            self._rc_stats.total_regenerated += len(check_result.failed_records)
            regen = self._generator.regenerate_with_feedback(
                item, check_result.failed_records
            )

            # Structural validate regenerated
            regen_valid = []
            for comp in regen:
                result = self._validator.validate_and_normalize(comp)
                if result.is_valid and result.normalized_composition:
                    regen_valid.append(result.normalized_composition)

            # Pass 2
            if regen_valid:
                pass2 = self._reality_checker.check_batch(regen_valid)
                final_valid.extend(
                    [r.raw_record for r in pass2.passed_records]
                )
                self._rc_stats.total_passed_second += len(pass2.passed_records)

                # Log permanent discards
                for d in pass2.failed_records:
                    logger.warning(
                        "Discarding (pass 2): %s", d.justification
                    )
                    self._rc_stats.total_discarded += 1
                    self._rc_stats.discarded_log.append({
                        "layer": 1,
                        "item": item.full_id,
                        "justification_pass1": "regenerated",
                        "justification_pass2": d.justification,
                    })
            else:
                # All regenerated failed structural validation
                for f in check_result.failed_records:
                    self._rc_stats.total_discarded += 1
                    self._rc_stats.discarded_log.append({
                        "layer": 1,
                        "item": item.full_id,
                        "justification_pass1": f.justification,
                        "justification_pass2": "regeneration failed structural validation",
                    })

        return final_valid

    def _write_discard_log(self) -> None:
        """Write discarded records to a JSONL file."""
        import json
        from datetime import datetime
        if not self._rc_stats.discarded_log:
            return
        discard_path = self.config.output_dir / "reality_check_discards.jsonl"
        with open(discard_path, "w") as f:
            for entry in self._rc_stats.discarded_log:
                entry["timestamp"] = datetime.now().isoformat()
                f.write(json.dumps(entry) + "\n")
        logger.info("Wrote %d discards to %s", len(self._rc_stats.discarded_log), discard_path)

    def _run_dry(self, items: List[TaxonomyItem], products_per_category: int) -> None:
        """Run a dry run without API calls or output."""
        logger.info("=== DRY RUN SUMMARY ===")
        logger.info(f"Total taxonomy items: {len(items)}")
        logger.info(f"Products per category: {products_per_category}")
        logger.info(f"Expected total products: {len(items) * products_per_category}")

        # Show sample of items
        all_materials = self._category_mapper.get_all_textile_materials()
        logger.info("\nTotal materials in pool: %d", len(all_materials))
        logger.info("\nSample items to process:")
        for item in items[:10]:
            logger.info("  %s: %s", item.full_id, item.full_name)

        # Show category statistics
        logger.info("\nCategory breakdown:")
        by_main = {}
        for item in items:
            main = item.main_category
            by_main[main] = by_main.get(main, 0) + 1

        for main, count in by_main.items():
            logger.info(f"  {main}: {count} items ({count * products_per_category} products)")

    def _print_final_stats(self) -> None:
        """Print final statistics."""
        stats = self._progress.get_stats()

        logger.info("\n" + "=" * 50)
        logger.info("LAYER 1 GENERATION COMPLETE")
        logger.info("=" * 50)
        logger.info(f"Total compositions generated: {stats['compositions_generated']}")
        logger.info(f"Valid compositions: {stats['compositions_valid']}")
        logger.info(f"Invalid compositions: {stats['compositions_invalid']}")
        logger.info(f"Validation rate: {stats['validation_rate']:.1f}%")
        logger.info(f"API calls: {stats['api_calls']}")
        logger.info(f"API errors: {stats['api_errors']}")

        # Reality check statistics
        rc = self._rc_stats
        if rc.total_checked > 0:
            logger.info("-" * 50)
            p1_rate = rc.total_passed_first / rc.total_checked * 100 if rc.total_checked else 0
            logger.info(f"Reality check -- Pass 1 pass rate: {p1_rate:.1f}%")
            logger.info(f"Reality check -- Guided regeneration: {rc.total_regenerated} records")
            if rc.total_regenerated > 0:
                p2_rate = rc.total_passed_second / rc.total_regenerated * 100
                logger.info(f"Reality check -- Pass 2 pass rate: {p2_rate:.1f}%")
            logger.info(f"Reality check -- Permanently discarded: {rc.total_discarded} records")

        logger.info(f"Total time: {stats['elapsed_seconds']/60:.1f} minutes")
        logger.info(f"Output file: {self.config.output_path}")
        logger.info("=" * 50)

    def health_check(self) -> bool:
        """Verify all components are working."""
        logger.info("Running health check...")

        # Check files exist
        if not self.config.taxonomy_path.exists():
            logger.error(f"Taxonomy file not found: {self.config.taxonomy_path}")
            return False

        if not self.config.materials_path.exists():
            logger.error(f"Materials file not found: {self.config.materials_path}")
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
    """Main entry point for Layer 1 generation."""
    import argparse

    parser = argparse.ArgumentParser(description="Layer 1: Product Composition Generator")
    parser.add_argument(
        "--products-per-category",
        type=int,
        default=100,
        help="Number of products to generate per category"
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        help="Specific category IDs to process"
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
    parser.add_argument(
        "--parallel", "-p",
        type=int,
        default=4,
        help="Number of parallel workers (default: 4)"
    )
    parser.add_argument(
        "--no-batch",
        action="store_true",
        help="Disable batch generation (generate one product at a time)"
    )

    args = parser.parse_args()

    setup_logging(args.verbose)

    orchestrator = Layer1Orchestrator()

    if args.health_check:
        success = orchestrator.health_check()
        sys.exit(0 if success else 1)

    orchestrator.run(
        products_per_category=args.products_per_category,
        categories=args.categories,
        dry_run=args.dry_run,
        parallel_workers=args.parallel,
        use_batch=not args.no_batch
    )


if __name__ == "__main__":
    main()
