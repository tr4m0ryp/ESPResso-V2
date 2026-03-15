"""
Layer 5 Orchestrator (V2): Five-stage validation pipeline.

Coordinates passport verification, cross-layer coherence, statistical
quality, sampled reward scoring, and final decision-making with
incremental batch I/O.
"""

import ast, csv, json, logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from data.data_generation.layer_5.clients.api_client import Layer5Client
from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.core.coherence_validator import CoherenceValidator
from data.data_generation.layer_5.core.decision_maker import DecisionMaker
from data.data_generation.layer_5.core.passport_verifier import PassportVerifier
from data.data_generation.layer_5.core.sampled_reward_scorer import SampledRewardScorer
from data.data_generation.layer_5.core.statistical_validator import StatisticalValidator
from data.data_generation.layer_5.io.writer_incremental import IncrementalValidationOutputWriter
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord, CompleteValidationResult,
    ValidationPipelineStats, ValidationSummary,
)

logger = logging.getLogger(__name__)


def _parse_list(val: str) -> list:
    """Parse a JSON or Python-literal list string; return [] on failure."""
    if not val:
        return []
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        try:
            return ast.literal_eval(val)
        except (ValueError, SyntaxError):
            return []


class Layer5Orchestrator:
    """Orchestrates the V2 five-stage validation pipeline."""

    def __init__(self, config: Optional[Layer5Config] = None):
        self.config = config or Layer5Config()
        self._client: Optional[Layer5Client] = None
        self._passport_verifier: Optional[PassportVerifier] = None
        self._coherence_validator: Optional[CoherenceValidator] = None
        self._statistical_validator: Optional[StatisticalValidator] = None
        self._reward_scorer: Optional[SampledRewardScorer] = None
        self._decision_maker: Optional[DecisionMaker] = None
        self._writer: Optional[IncrementalValidationOutputWriter] = None
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

    def run_pipeline(self, max_records: Optional[int] = None) -> Dict[str, Any]:
        """Run the complete five-stage validation pipeline.

        Returns a dict with ``success``, output file paths, and statistics.
        """
        self.stats = ValidationPipelineStats()
        self.stats.start_time = datetime.now().isoformat()
        if self._client is None:
            self.initialize()

        try:
            records = self._load_records(max_records)
            if not records:
                return self._error_result("No records loaded from Layer 4")

            total = len(records)
            logger.info("Loaded %d records for validation", total)

            bs = self.config.batch_size
            batches = [records[i:i + bs] for i in range(0, total, bs)]
            all_results: List[CompleteValidationResult] = []
            global_idx = 0

            for batch_num, batch in enumerate(batches, 1):
                logger.info("Batch %d/%d (%d records)", batch_num, len(batches), len(batch))
                try:
                    results = self._process_batch(batch, global_idx, total)
                    all_results.extend(results)
                    self._writer.write_batch(results, batch_num)
                except Exception as exc:
                    logger.error("Batch %d failed: %s", batch_num, exc)
                global_idx += len(batch)

            summary = self._build_summary(all_results)
            output_files = self._writer.merge_final_outputs(summary)
            summary_file = self._writer.write_validation_summary(all_results, self.stats)
            self.stats.end_time = datetime.now().isoformat()
            return self._success_result(output_files, summary_file)

        except Exception as exc:
            logger.error("Pipeline failed: %s", exc)
            self.stats.end_time = datetime.now().isoformat()
            return self._error_result(str(exc))

    # -- Per-batch processing ------------------------------------------

    def _process_batch(
        self, batch: List[CompleteProductRecord], global_idx: int, total: int,
    ) -> List[CompleteValidationResult]:
        """Run stages 1-5 on a batch and return results."""
        # Stage 1: Passport verification
        passport_results = self._passport_verifier.verify_batch(batch)

        # Stage 2: Cross-layer coherence (chunked by coherence_batch_size)
        coherence_results: Dict[str, Any] = {}
        cbs = self.config.coherence_batch_size
        for i in range(0, len(batch), cbs):
            coherence_results.update(
                self._coherence_validator.validate_batch(batch[i:i + cbs])
            )

        # Stage 3: Statistical quality (sequential, maintains state)
        stat_results = {
            r.subcategory_id: self._statistical_validator.validate_record(r)
            for r in batch
        }

        # Stage 4: Sampled reward scoring
        reward_results = {}
        for off, rec in enumerate(batch):
            reward_results[rec.subcategory_id] = (
                self._reward_scorer.score_if_sampled(rec, global_idx + off, total)
            )

        # Stage 5: Final decision
        results: List[CompleteValidationResult] = []
        for rec in batch:
            sid = rec.subcategory_id
            result = self._decision_maker.decide(
                rec, passport_results[sid],
                coherence_results.get(sid), stat_results.get(sid),
                reward_results.get(sid),
            )
            results.append(result)
            self._update_stats(result)
        return results

    # -- Data loading --------------------------------------------------

    def _load_records(self, max_records: Optional[int]) -> List[CompleteProductRecord]:
        """Load CompleteProductRecords from the Layer 4 CSV output."""
        path = self.config.complete_dataset_path
        if not Path(path).exists():
            logger.error("Dataset not found: %s", path)
            return []
        logger.info("Loading dataset from %s", path)
        records: List[CompleteProductRecord] = []
        with open(path, "r", encoding="utf-8") as fh:
            for i, row in enumerate(csv.DictReader(fh)):
                try:
                    records.append(self._row_to_record(row, i))
                except Exception as exc:
                    logger.warning("Skipping row %d: %s", i, exc)
                if max_records and len(records) >= max_records:
                    break
                if (i + 1) % 10_000 == 0:
                    logger.info("Loaded %d rows...", i + 1)
        logger.info("Loaded %d records total", len(records))
        return records

    @staticmethod
    def _row_to_record(row: Dict[str, str], idx: int) -> CompleteProductRecord:
        """Parse a single CSV row into a CompleteProductRecord."""
        g = row.get
        p = _parse_list
        return CompleteProductRecord(
            category_id=g("category_id", f"cat_{idx}"),
            category_name=g("category_name", ""),
            subcategory_id=g("subcategory_id", f"subcat_{idx}"),
            subcategory_name=g("subcategory_name", ""),
            materials=p(g("materials", "[]")),
            material_weights_kg=p(g("material_weights_kg", "[]")),
            material_percentages=p(g("material_percentages", "[]")),
            total_weight_kg=float(g("total_weight_kg", 0.0)),
            preprocessing_path_id=g("preprocessing_path_id", f"pp_{idx}"),
            preprocessing_steps=p(g("preprocessing_steps", "[]")),
            transport_scenario_id=g("transport_scenario_id", f"ts_{idx}"),
            total_transport_distance_km=float(g("total_transport_distance_km", 0.0)),
            supply_chain_type=g("supply_chain_type", "medium_haul"),
            transport_items=p(g("transport_items", "[]")),
            transport_modes=p(g("transport_modes", "[]")),
            transport_distances_kg=p(g("transport_distances_kg", "[]")),
            transport_emissions_kg_co2e=p(g("transport_emissions_kg_co2e", "[]")),
            packaging_config_id=g("packaging_config_id", f"pkg_{idx}"),
            packaging_items=p(g("packaging_items", "[]")),
            packaging_categories=p(g("packaging_categories", "[]")),
            packaging_masses_kg=p(g("packaging_masses_kg", "[]")),
            total_packaging_mass_kg=float(g("total_packaging_mass_kg", 0.0)),
        )

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
        acc = sum(1 for r in results if r.final_decision == "accept")
        rev = sum(1 for r in results if r.final_decision == "review")
        rej = sum(1 for r in results if r.final_decision == "reject")
        scores = [r.final_score for r in results if r.final_score > 0]
        avg = sum(scores) / len(scores) if scores else 0.0
        samp = sum(1 for r in results if r.reward and r.reward.was_sampled)
        eq = self._reward_scorer.get_dataset_quality_estimate() or 0.0
        sv = self._statistical_validator
        return ValidationSummary(
            total_records_processed=n,
            accepted_records=acc, review_queue_records=rev, rejected_records=rej,
            acceptance_rate=acc / n if n else 0.0,
            review_rate=rev / n if n else 0.0,
            rejection_rate=rej / n if n else 0.0,
            average_plausibility_score=avg,
            sampled_reward_records=samp, estimated_dataset_quality=eq,
            passport_failures=self.stats.passport_failures,
            distribution_coverage=sv.get_statistical_summary(),
            duplicates_removed=sv.exact_duplicates + sv.near_duplicates,
            outliers_flagged=sum(
                1 for r in results if r.statistical and r.statistical.is_outlier
            ),
        )

    # -- Result dict builders ------------------------------------------

    def _success_result(self, output_files: Dict[str, str], summary_file: str) -> Dict[str, Any]:
        """Return dict for a successful pipeline run."""
        duration = 0.0
        if self.stats.start_time and self.stats.end_time:
            s = datetime.fromisoformat(self.stats.start_time)
            e = datetime.fromisoformat(self.stats.end_time)
            duration = (e - s).total_seconds()
        return {
            "success": True, "output_files": output_files,
            "summary_file": summary_file,
            "statistics": {
                "duration_seconds": duration,
                "total_records": self.stats.records_processed,
                "accepted": self.stats.records_accepted,
                "review": self.stats.records_in_review,
                "rejected": self.stats.records_rejected,
                "acceptance_rate": self.stats.get_acceptance_rate(),
                "rejection_rate": self.stats.get_rejection_rate(),
                "passport_failures": self.stats.passport_failures,
                "sampled_records": self.stats.sampled_records,
            },
            "timestamp": self.stats.end_time,
        }

    @staticmethod
    def _error_result(message: str) -> Dict[str, Any]:
        return {"success": False, "error": message, "timestamp": datetime.now().isoformat()}

    # -- Public helpers ------------------------------------------------

    def test_api_connection(self) -> bool:
        """Test whether the LLM API is reachable."""
        if self._client is None:
            self._client = Layer5Client(self.config)
        return self._client.test_connection()
