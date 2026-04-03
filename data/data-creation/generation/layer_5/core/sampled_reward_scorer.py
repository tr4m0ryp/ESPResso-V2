"""
Sampled Reward Scorer for Layer 5: Quality Estimation via Sampling

Scores only 1-5% of records via LLM reward API and estimates dataset
quality from the sample distribution, drastically reducing API calls.
"""

import logging
import statistics
import threading
from typing import Dict, List, Optional, Any

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.clients.api_client import Layer5Client
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord,
    SampledRewardResult,
)

logger = logging.getLogger(__name__)


class SampledRewardScorer:
    """Scores a deterministic sample of records for dataset quality estimation.

    Uses config.should_sample_for_reward() for reproducible sampling, then
    delegates to api_client.generate_reward_score() for LLM-based quality
    assessment. Maintains running statistics to estimate dataset quality.
    """

    def __init__(self, config: Layer5Config, api_client: Layer5Client):
        self.config = config
        self.api_client = api_client
        self._lock = threading.Lock()
        self.sampled_scores: List[float] = []
        self.total_records_seen: int = 0
        self.total_records_sampled: int = 0

    def score_if_sampled(
        self,
        record: CompleteProductRecord,
        record_index: int,
        total_records: int,
    ) -> SampledRewardResult:
        """Score the record if it falls within the sample.

        Increments total_records_seen every call. If selected by
        config.should_sample_for_reward(), scores via LLM reward API
        and accumulates into running statistics.
        """
        with self._lock:
            self.total_records_seen += 1

        if not self.config.should_sample_for_reward(
            record_index, total_records
        ):
            return SampledRewardResult(
                was_sampled=False,
                reward_score=None,
                quality_interpretation=None,
                dataset_estimated_quality=self.get_dataset_quality_estimate(),
            )

        # Record falls within sample -- score it (API call outside lock)
        score = self._score_record(record)

        if score is not None:
            with self._lock:
                self.sampled_scores.append(score)
                self.total_records_sampled += 1
            interpretation = self.get_quality_interpretation(score)
            logger.info(
                "Reward score for record %d: %.3f (%s) "
                "[sampled %d / %d seen]",
                record_index,
                score,
                interpretation,
                self.total_records_sampled,
                self.total_records_seen,
            )
            return SampledRewardResult(
                was_sampled=True,
                reward_score=score,
                quality_interpretation=interpretation,
                dataset_estimated_quality=(
                    self.get_dataset_quality_estimate()
                ),
            )

        # API returned None (no keys configured or persistent failure)
        logger.warning(
            "Reward scoring returned None for record %d; "
            "marking as sampled but without a score",
            record_index,
        )
        return SampledRewardResult(
            was_sampled=True,
            reward_score=None,
            quality_interpretation=None,
            dataset_estimated_quality=self.get_dataset_quality_estimate(),
        )

    def _score_record(self, record: CompleteProductRecord) -> Optional[float]:
        """Score a single record via api_client.generate_reward_score()."""
        context = self._build_reward_context(record)
        return self.api_client.generate_reward_score(context)

    def _build_reward_context(
        self, record: CompleteProductRecord
    ) -> str:
        """Build compact context string summarising all four layers."""
        # Materials summary
        material_parts = []
        for i, mat in enumerate(record.materials):
            weight = (
                record.material_weights_kg[i]
                if i < len(record.material_weights_kg)
                else 0.0
            )
            pct = (
                record.material_percentages[i]
                if i < len(record.material_percentages)
                else 0.0
            )
            material_parts.append(f"{mat}: {weight:.3f}kg ({pct:.1f}%)")

        materials_str = ", ".join(material_parts) if material_parts else "N/A"

        # Transport summary
        transport_parts = []
        for i, mode in enumerate(record.transport_modes):
            dist = (
                record.transport_distances_kg[i]
                if i < len(record.transport_distances_kg)
                else 0.0
            )
            emissions = (
                record.transport_emissions_kg_co2e[i]
                if i < len(record.transport_emissions_kg_co2e)
                else 0.0
            )
            transport_parts.append(
                f"{mode}: {dist:.0f}km, {emissions:.4f}kg CO2e"
            )

        transport_str = (
            ", ".join(transport_parts) if transport_parts else "N/A"
        )

        # Packaging summary
        packaging_parts = []
        for i, cat in enumerate(record.packaging_categories):
            mass = (
                record.packaging_masses_kg[i]
                if i < len(record.packaging_masses_kg)
                else 0.0
            )
            packaging_parts.append(f"{cat}: {mass:.4f}kg")

        packaging_str = (
            ", ".join(packaging_parts) if packaging_parts else "N/A"
        )

        return (
            f"Category: {record.category_name} / {record.subcategory_name}\n"
            f"Materials: {materials_str}\n"
            f"Total weight: {record.total_weight_kg:.3f}kg\n"
            f"Processing steps: {', '.join(record.preprocessing_steps)}\n"
            f"Supply chain: {record.supply_chain_type}\n"
            f"Transport: {transport_str}\n"
            f"Total transport distance: "
            f"{record.total_transport_distance_km:.0f}km\n"
            f"Packaging: {packaging_str}\n"
            f"Total packaging mass: {record.total_packaging_mass_kg:.4f}kg"
        )

    def get_dataset_quality_estimate(self) -> Optional[float]:
        """Mean of sampled scores, or None if no samples yet."""
        if not self.sampled_scores:
            return None
        return statistics.mean(self.sampled_scores)

    def get_quality_interpretation(self, score: float) -> str:
        """Map a 0.0-1.0 score to a quality label string."""
        if score >= 0.8:
            return "High quality"
        if score >= 0.6:
            return "Acceptable"
        if score >= 0.4:
            return "Marginal"
        return "Low quality"

    def get_sampling_summary(self) -> Dict[str, Any]:
        """Summary with counts, rates, score stats, and quality distribution."""
        summary: Dict[str, Any] = {
            "total_records_seen": self.total_records_seen,
            "total_records_sampled": self.total_records_sampled,
            "configured_sample_rate": self.config.reward_sample_rate,
            "actual_sample_rate": (
                self.total_records_sampled / self.total_records_seen
                if self.total_records_seen > 0
                else 0.0
            ),
        }

        if not self.sampled_scores:
            summary["score_statistics"] = None
            summary["quality_distribution"] = None
            summary["dataset_estimated_quality"] = None
            summary["quality_interpretation"] = None
            return summary

        mean_score = statistics.mean(self.sampled_scores)
        median_score = statistics.median(self.sampled_scores)
        min_score = min(self.sampled_scores)
        max_score = max(self.sampled_scores)

        stdev_score = (
            statistics.stdev(self.sampled_scores)
            if len(self.sampled_scores) >= 2
            else 0.0
        )

        summary["score_statistics"] = {
            "mean": round(mean_score, 4),
            "median": round(median_score, 4),
            "stdev": round(stdev_score, 4),
            "min": round(min_score, 4),
            "max": round(max_score, 4),
            "count": len(self.sampled_scores),
        }

        # Quality distribution buckets
        high = sum(1 for s in self.sampled_scores if s >= 0.8)
        acceptable = sum(
            1 for s in self.sampled_scores if 0.6 <= s < 0.8
        )
        marginal = sum(
            1 for s in self.sampled_scores if 0.4 <= s < 0.6
        )
        low = sum(1 for s in self.sampled_scores if s < 0.4)
        total = len(self.sampled_scores)

        summary["quality_distribution"] = {
            "high_quality": {"count": high, "pct": high / total},
            "acceptable": {"count": acceptable, "pct": acceptable / total},
            "marginal": {"count": marginal, "pct": marginal / total},
            "low_quality": {"count": low, "pct": low / total},
        }

        summary["dataset_estimated_quality"] = round(mean_score, 4)
        summary["quality_interpretation"] = (
            self.get_quality_interpretation(mean_score)
        )

        return summary
