"""
Cross-Layer Coherence Validator for Layer 5: Stage 2 Validation (V2).

Evaluates cross-layer coherence for batches of up to 50 records via LLM.
Replaces the V1 SemanticValidator with batch-only evaluation using
CoherencePromptBuilder for prompt construction and response parsing.
"""

import logging
import time
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
    ) -> List[CrossLayerCoherenceResult]:
        """Evaluate cross-layer coherence for a batch of up to 50 records.

        Returns a list of results in the SAME ORDER as input records.
        Uses position-based keys (record_1, record_2, ...) to avoid
        collisions when multiple records share the same subcategory_id.
        """
        if not records:
            return []

        n = len(records)
        # Position-based keys for the prompt/parse round-trip
        position_keys = [f"record_{i + 1}" for i in range(n)]
        max_retries = getattr(self.config, 'max_retries', 3)
        retry_delay = getattr(self.config, 'retry_delay', 2.0)

        for attempt in range(max_retries):
            try:
                prompt = self.prompt_builder.build_batch_prompt(records)
                response = self.api_client.generate_batch_coherence_evaluation(
                    prompt=prompt,
                    temperature=self.config.temperature_instruct,
                    max_tokens=self.config.max_tokens_instruct,
                )

                if response is None:
                    logger.warning(
                        "Coherence API returned None (attempt %d/%d) "
                        "for batch of %d records",
                        attempt + 1, max_retries, n,
                    )
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (2 ** attempt))
                        continue
                    return [self._default_result() for _ in range(n)]

                parsed = self.prompt_builder.parse_batch_response(
                    response, position_keys
                )

                # Build ordered list from parsed dict
                result_list = []
                missing = 0
                for key in position_keys:
                    r = parsed.get(key)
                    if r is None:
                        missing += 1
                        result_list.append(self._default_result())
                    else:
                        result_list.append(r)

                # All missing -> parse failure, retry
                if missing == n:
                    logger.warning(
                        "Parse returned no results (attempt %d/%d), retrying",
                        attempt + 1, max_retries,
                    )
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (2 ** attempt))
                        continue

                if missing > 0:
                    logger.warning(
                        "Filled %d/%d missing coherence results with defaults",
                        missing, n,
                    )

                self._log_list_summary(result_list)
                return result_list

            except Exception as e:
                logger.error(
                    "Coherence validation error (attempt %d/%d) for "
                    "batch of %d records: %s",
                    attempt + 1, max_retries, n, e,
                )
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (2 ** attempt))
                    continue
                return [self._default_result() for _ in range(n)]

        return [self._default_result() for _ in range(n)]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
    def _log_list_summary(
        results: List[CrossLayerCoherenceResult],
    ) -> None:
        """Log a concise summary of the batch evaluation."""
        if not results:
            logger.info("Coherence batch: 0 records evaluated")
            return
        scores = [r.overall_coherence_score for r in results]
        logger.info(
            "Coherence batch: %d evaluated, mean coherence=%.3f",
            len(results),
            sum(scores) / len(scores),
        )
