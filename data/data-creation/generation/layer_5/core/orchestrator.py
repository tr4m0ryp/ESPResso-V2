"""Layer 5 Orchestrator (V2): Five-stage validation pipeline."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional

from data.data_generation.layer_5.clients.api_client import Layer5Client
from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.core.coherence_validator import CoherenceValidator
from data.data_generation.layer_5.core.decision_maker import DecisionMaker
from data.data_generation.layer_5.core.passport_verifier import PassportVerifier
from data.data_generation.layer_5.core.sampled_reward_scorer import SampledRewardScorer
from data.data_generation.layer_5.core.statistical_validator import StatisticalValidator
from data.data_generation.layer_5.io.data_loader import load_records
from data.data_generation.layer_5.io.writer_incremental import IncrementalValidationOutputWriter
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord, CompleteValidationResult,
    ValidationPipelineStats, ValidationSummary,
)

logger = logging.getLogger(__name__)


class Layer5Orchestrator:
    """Orchestrates the V2 five-stage validation pipeline."""

    def __init__(self, config: Optional[Layer5Config] = None):
        self.config = config or Layer5Config()
        self._client = None
        self._passport_verifier = None
        self._coherence_validator = None
        self._statistical_validator = None
        self._reward_scorer = None
        self._decision_maker = None
        self._writer = None
        self.stats = ValidationPipelineStats()

    def initialize(self) -> None:
        """Lazily init all components and test the API connection."""
        self.config.ensure_directories()
        self._client = Layer5Client(self.config)
        self._client.test_connection()
        self._passport_verifier = PassportVerifier(self.config)
        self._coherence_validator = CoherenceValidator(self.config, self._client)
        self._statistical_validator = StatisticalValidator(self.config)
        self._reward_scorer = SampledRewardScorer(self.config, self._client)
        self._decision_maker = DecisionMaker(self.config)
        self._writer = IncrementalValidationOutputWriter(self.config)
        logger.info("All pipeline components initialized")

    # -- Main pipeline -------------------------------------------------

    def run_pipeline(
        self,
        max_records: Optional[int] = None,
        workers: Optional[int] = None,
        rate_limit: Optional[int] = None,
        resume: bool = False,
    ) -> Dict[str, Any]:
        """Run the complete five-stage validation pipeline."""
        self.stats = ValidationPipelineStats()
        self.stats.start_time = datetime.now().isoformat()
        if self._client is None:
            self.initialize()

        workers = workers or self.config.parallel_workers
        rate_limit = rate_limit or self.config.effective_rate_limit

        try:
            records = load_records(self.config, max_records)
            if not records:
                return self._error_result("No records loaded from Layer 4")

            if resume:
                done = self._load_completed_ids()
                records = [r for r in records
                           if f"{r.subcategory_id}_{r.preprocessing_path_id}" not in done]
                logger.info("Resume: %d done, %d remaining", len(done), len(records))
                if not records:
                    return self._error_result("All records already completed")

            total = len(records)
            logger.info("Loaded %d records for validation", total)

            bs = self.config.batch_size
            batches = [records[i:i + bs] for i in range(0, total, bs)]

            if workers > 1:
                all_results = self._run_parallel(
                    batches, total, workers, rate_limit
                )
            else:
                all_results = self._run_sequential(batches, total)

            summary = self._build_summary(all_results)
            output_files = self._writer.merge_final_outputs(summary)
            summary_file = self._writer.write_validation_summary(all_results, self.stats)
            self.stats.end_time = datetime.now().isoformat()
            return self._success_result(output_files, summary_file)

        except Exception as exc:
            logger.error("Pipeline failed: %s", exc)
            self.stats.end_time = datetime.now().isoformat()
            return self._error_result(str(exc))

    def _run_sequential(
        self,
        batches: List[List[CompleteProductRecord]],
        total: int,
    ) -> List[CompleteValidationResult]:
        """Process batches sequentially (original behavior)."""
        all_results: List[CompleteValidationResult] = []
        global_idx = 0

        for batch_num, batch in enumerate(batches, 1):
            logger.info("Batch %d/%d (%d records)", batch_num, len(batches), len(batch))
            try:
                results = self._process_batch(batch, global_idx, total)
                for r in results:
                    self._update_stats(r)
                all_results.extend(results)
                self._writer.write_batch(results, batch_num)
            except Exception as exc:
                logger.error("Batch %d failed: %s", batch_num, exc)
            global_idx += len(batch)

        return all_results

    def _run_parallel(
        self,
        batches: List[List[CompleteProductRecord]],
        total: int,
        workers: int,
        rpm: int,
    ) -> List[CompleteValidationResult]:
        """Process batches in parallel using the shared ParallelProcessor."""
        from data.data_generation.shared.parallel_processor import (
            ParallelProcessor,
            ProcessingResult,
        )

        logger.info("Starting parallel processing: %d workers, %d RPM", workers, rpm)
        # API client handles rate limiting; disable ParallelProcessor's limiter
        processor = ParallelProcessor(
            max_workers=workers, requests_per_minute=999_999,
            show_progress=True, enable_pause=True,
        )

        all_results: List[CompleteValidationResult] = []
        lock = threading.Lock()
        counter = [0]

        def process_func(batch):
            with lock:
                counter[0] += 1
                idx = counter[0]
            return self._process_batch(batch, idx * self.config.batch_size, total)

        def on_complete(result: ProcessingResult):
            if not result.success or result.output_data is None:
                return
            with lock:
                all_results.extend(result.output_data)
                self._writer.write_batch(result.output_data, len(all_results))

        processor.process_batch(batches, process_func, on_complete)

        stats = processor.get_stats()
        logger.info("Parallel complete: %d ok, %d fail, %d retries",
                     stats.get("success", 0), stats.get("failed", 0), stats.get("retries", 0))

        # Update pipeline stats from all results
        for result in all_results:
            self._update_stats(result)

        return all_results

    # -- Per-batch processing ------------------------------------------

    def _process_batch(
        self, batch: List[CompleteProductRecord], global_idx: int, total: int,
    ) -> List[CompleteValidationResult]:
        """Run stages 1-5 on a batch and return results."""
        # Stage 1: Passport verification (CPU-bound, fast, returns list)
        passport_list = self._passport_verifier.verify_batch(batch)

        # Stage 2: Cross-layer coherence (parallel chunks, list-based)
        coherence_list: list = []
        cbs = self.config.coherence_batch_size
        chunks = [(i, batch[i:i + cbs]) for i in range(0, len(batch), cbs)]
        chunk_results_map: Dict[int, list] = {}
        with ThreadPoolExecutor(max_workers=min(len(chunks), 10)) as pool:
            futures = {pool.submit(self._coherence_validator.validate_batch, c): s
                       for s, c in chunks}
            for f in as_completed(futures):
                chunk_results_map[futures[f]] = f.result()
        for start, _ in sorted(chunks):
            coherence_list.extend(chunk_results_map.get(start, []))

        # Stage 3: Statistical quality (sequential -- maintains ordered state)
        stat_list = [self._statistical_validator.validate_record(r) for r in batch]

        # Stage 4: Sampled reward scoring (parallel for sampled records)
        reward_list = [None] * len(batch)
        sampled_indices = [i for i in range(len(batch))
                          if self.config.should_sample_for_reward(global_idx + i, total)]
        for i in range(len(batch)):
            if i not in sampled_indices:
                reward_list[i] = self._reward_scorer.score_if_sampled(
                    batch[i], global_idx + i, total)
        if sampled_indices:
            with ThreadPoolExecutor(max_workers=min(len(sampled_indices), 10)) as pool:
                def _score(idx):
                    return idx, self._reward_scorer.score_if_sampled(
                        batch[idx], global_idx + idx, total)
                for idx, result in pool.map(_score, sampled_indices):
                    reward_list[idx] = result

        # Stage 5: Final decision (all lookups by position)
        results: List[CompleteValidationResult] = []
        for i, rec in enumerate(batch):
            coh = coherence_list[i] if i < len(coherence_list) else None
            results.append(self._decision_maker.decide(
                rec, passport_list[i],
                coh, stat_list[i], reward_list[i]))
        return results

    # -- Statistics and summary ----------------------------------------

    def _update_stats(self, result: CompleteValidationResult) -> None:
        """Increment pipeline counters from a single result."""
        self.stats.records_processed += 1
        decision = result.final_decision
        if decision == "accept":
            self.stats.records_accepted += 1
        elif decision == "review":
            self.stats.records_in_review += 1
        elif decision == "reject":
            self.stats.records_rejected += 1
        if not result.passport.is_valid:
            self.stats.passport_failures += 1
        if result.reward and result.reward.was_sampled:
            self.stats.sampled_records += 1

    def _build_summary(self, results: List[CompleteValidationResult]) -> ValidationSummary:
        """Build a ValidationSummary from accumulated results."""
        n = len(results)
        cnt = lambda d: sum(1 for r in results if r.final_decision == d)
        acc, rev, rej = cnt("accept"), cnt("review"), cnt("reject")
        sc = [r.final_score for r in results if r.final_score > 0]
        sv = self._statistical_validator
        return ValidationSummary(
            total_records_processed=n, accepted_records=acc,
            review_queue_records=rev, rejected_records=rej,
            acceptance_rate=acc/n if n else 0, review_rate=rev/n if n else 0,
            rejection_rate=rej/n if n else 0,
            average_plausibility_score=sum(sc)/len(sc) if sc else 0,
            sampled_reward_records=sum(1 for r in results if r.reward and r.reward.was_sampled),
            estimated_dataset_quality=self._reward_scorer.get_dataset_quality_estimate() or 0,
            passport_failures=self.stats.passport_failures,
            distribution_coverage=sv.get_statistical_summary(),
            duplicates_removed=sv.exact_duplicates + sv.near_duplicates,
            outliers_flagged=sum(1 for r in results if r.statistical and r.statistical.is_outlier))

    # -- Result dict builders ------------------------------------------

    def _success_result(
        self, output_files: Dict[str, str], summary_file: str
    ) -> Dict[str, Any]:
        """Return dict for a successful pipeline run."""
        s, e = self.stats.start_time, self.stats.end_time
        dur = 0.0
        if s and e:
            dur = (datetime.fromisoformat(e) - datetime.fromisoformat(s)).total_seconds()
        st = self.stats
        return {
            "success": True, "output_files": output_files, "summary_file": summary_file,
            "statistics": {
                "duration_seconds": dur, "total_records": st.records_processed,
                "accepted": st.records_accepted, "review": st.records_in_review,
                "rejected": st.records_rejected,
                "acceptance_rate": st.get_acceptance_rate(),
                "rejection_rate": st.get_rejection_rate(),
                "passport_failures": st.passport_failures,
                "sampled_records": st.sampled_records,
            },
            "timestamp": e,
        }

    @staticmethod
    def _error_result(message: str) -> Dict[str, Any]:
        return {"success": False, "error": message, "timestamp": datetime.now().isoformat()}

    def _load_completed_ids(self) -> set:
        """Load record IDs already written to temp files for resume."""
        import csv as _csv, glob as _glob
        ids = set()
        for f in _glob.glob(str(self.config.output_dir / "temp_files" / "*.csv")):
            with open(f, "r", encoding="utf-8") as fh:
                for row in _csv.DictReader(fh):
                    if row.get("record_id"):
                        ids.add(row["record_id"])
        return ids

    # -- Public helpers ------------------------------------------------

    def test_api_connection(self) -> bool:
        """Test whether the LLM API is reachable."""
        if self._client is None:
            self._client = Layer5Client(self.config)
        return self._client.test_connection()
