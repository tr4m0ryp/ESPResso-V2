"""
Cross-Layer Coherence Validator for Layer 5: Stage 2 Validation (V2).

Evaluates cross-layer coherence for batches of up to 50 records via LLM.
Replaces the V1 SemanticValidator with batch-only evaluation using
CoherencePromptBuilder for prompt construction and response parsing.
"""

import logging
from typing import Dict, List

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.clients.api_client import Layer5Client
from data.data_generation.layer_5.core.coherence_prompt import CoherencePromptBuilder
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord,
    CrossLayerCoherenceResult,
)

logger = logging.getLogger(__name__)


class CoherenceValidator:
    """Evaluates cross-layer coherence for product record batches via LLM.

    All evaluation is batched (up to 50 records). There is no single-record
    method. This class never raises -- it always returns a result dict
    covering every input record, falling back to safe defaults on any error.
    """

    def __init__(self, config: Layer5Config, api_client: Layer5Client):
        self.config = config
        self.api_client = api_client
        self.prompt_builder = CoherencePromptBuilder()

    def validate_batch(
        self, records: List[CompleteProductRecord]
    ) -> Dict[str, CrossLayerCoherenceResult]:
        """Evaluate cross-layer coherence for a batch of up to 50 records.

        Steps:
            1. Build prompt via self.prompt_builder.build_batch_prompt(records)
            2. Token budget: self.config.max_tokens_instruct (8000)
            3. Call API for batch coherence evaluation
            4. Parse response via self.prompt_builder.parse_batch_response()
            5. Fill missing results with defaults (scores 0.7, recommendation "review")
            6. Log summary: count evaluated, mean coherence, parse failures

        Args:
            records: List of CompleteProductRecord instances (max 50).

        Returns:
            Dictionary mapping subcategory_id to CrossLayerCoherenceResult.
            Every input record is guaranteed to have an entry.
        """
        record_ids = [r.subcategory_id for r in records]

        if not records:
            return {}

        try:
            # 1. Build prompt
            prompt = self.prompt_builder.build_batch_prompt(records)

            # 2-3. Call API with configured token budget
            response = self.api_client.generate_batch_coherence_evaluation(
                prompt=prompt,
                temperature=self.config.temperature_instruct,
                max_tokens=self.config.max_tokens_instruct,
            )

            # API returned None (e.g. no keys configured)
            if response is None:
                logger.warning(
                    "Coherence API returned None for batch of %d records; "
                    "using defaults for all",
                    len(records),
                )
                return self._defaults_for_all(record_ids)

            # 4. Parse response
            results = self.prompt_builder.parse_batch_response(
                response, record_ids
            )

            # 5. Fill any missing entries with defaults
            missing_count = 0
            for rid in record_ids:
                if rid not in results:
                    missing_count += 1
                    results[rid] = self._default_result()

            if missing_count > 0:
                logger.warning(
                    "Filled %d/%d missing coherence results with defaults",
                    missing_count,
                    len(record_ids),
                )

            # 6. Log summary
            self._log_summary(results, record_ids)

            return results

        except Exception as e:
            logger.error(
                "Coherence validation failed for batch of %d records: %s",
                len(records),
                e,
            )
            return self._defaults_for_all(record_ids)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _defaults_for_all(
        self, record_ids: List[str]
    ) -> Dict[str, CrossLayerCoherenceResult]:
        """Return default results for every record in the batch."""
        return {rid: self._default_result() for rid in record_ids}

    @staticmethod
    def _default_result() -> CrossLayerCoherenceResult:
        """Create a safe default coherence result (scores 0.7, review)."""
        return CrossLayerCoherenceResult(
            lifecycle_coherence_score=0.7,
            cross_layer_contradiction_score=0.7,
            overall_coherence_score=0.7,
            contradictions_found=[],
            recommendation="review",
        )

    @staticmethod
    def _log_summary(
        results: Dict[str, CrossLayerCoherenceResult],
        record_ids: List[str],
    ) -> None:
        """Log a concise summary of the batch evaluation."""
        evaluated = len(results)
        if evaluated == 0:
            logger.info("Coherence batch: 0 records evaluated")
            return

        scores = [r.overall_coherence_score for r in results.values()]
        mean_coherence = sum(scores) / len(scores)
        parse_failures = evaluated - len(
            [rid for rid in record_ids if rid in results]
        )

        logger.info(
            "Coherence batch: %d evaluated, mean coherence=%.3f, "
            "parse failures=%d",
            evaluated,
            mean_coherence,
            max(parse_failures, 0),
        )
