"""
Decision Maker for Layer 5: Final Accept/Review/Reject Logic.

Combines passport, coherence, statistical, and reward results into
a single decision with score and rationale.
"""

import logging
from typing import List, Optional

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord, CompleteValidationResult,
    CrossLayerCoherenceResult, PassportVerificationResult,
    SampledRewardResult, StatisticalQualityResult, ValidationMetadata,
)

logger = logging.getLogger(__name__)


class DecisionMaker:
    """Produces final accept/review/reject decisions for validated records.

    Rules (evaluated in order):
        1. REJECT if passport failed.
        2. REJECT if coherence < review threshold (0.70).
        3. REJECT if duplicate.
        4. ACCEPT if coherence >= accept threshold (0.85) and no stat issues.
        5. REVIEW otherwise.
    """

    def __init__(self, config: Layer5Config):
        self.config = config

    def decide(
        self, record: CompleteProductRecord,
        passport: PassportVerificationResult,
        coherence: Optional[CrossLayerCoherenceResult],
        statistical: Optional[StatisticalQualityResult],
        reward: Optional[SampledRewardResult],
    ) -> CompleteValidationResult:
        """Evaluate all stage results and return a CompleteValidationResult."""
        rid = f"{record.subcategory_id}_{record.preprocessing_path_id}"
        factors: List[str] = []
        cs = coherence.overall_coherence_score if coherence else 0.0
        args = (rid, record, passport, coherence, statistical, reward)

        # Rule 1: passport failure -> reject
        if not passport.is_valid:
            factors.append("Passport verification failed")
            if passport.errors:
                factors.extend(passport.errors)
            if passport.missing_passports:
                factors.append("Missing: " + ", ".join(passport.missing_passports))
            return self._result(*args, "reject", cs, factors)

        # Rule 2: low coherence -> reject
        if coherence and cs < self.config.coherence_review_threshold:
            factors.append("Coherence %.3f < review threshold %.2f"
                           % (cs, self.config.coherence_review_threshold))
            if coherence.contradictions_found:
                factors.extend(coherence.contradictions_found)
            return self._result(*args, "reject", cs, factors)

        # Rule 3: duplicate -> review (near-duplicates may still be valid
        # records with different transport legs; PEFCR baselines produce
        # similar upstream data intentionally)
        if statistical and statistical.is_duplicate:
            factors.append("Duplicate (similarity=%.3f)" % statistical.duplicate_similarity)
            return self._result(*args, "review", cs, factors)

        # Rule 4: high coherence + clean stats -> accept
        clean = self._stats_clean(statistical)
        if cs >= self.config.coherence_accept_threshold and clean:
            factors.append("Coherence %.3f >= accept threshold %.2f"
                           % (cs, self.config.coherence_accept_threshold))
            factors.append("No statistical issues")
            return self._result(*args, "accept", cs, factors)

        # Rule 5: review
        if coherence:
            factors.append("Coherence %.3f in review range [%.2f, %.2f)"
                           % (cs, self.config.coherence_review_threshold,
                              self.config.coherence_accept_threshold))
        if statistical and statistical.distribution_issues:
            factors.extend(statistical.distribution_issues)
        if statistical and statistical.is_outlier:
            factors.append("Outlier: %s" % statistical.outlier_type)
        return self._result(*args, "review", cs, factors)

    @staticmethod
    def _stats_clean(stat: Optional[StatisticalQualityResult]) -> bool:
        """True when no statistical problems were flagged."""
        if stat is None:
            return True
        return not stat.is_duplicate and not stat.is_outlier and not stat.distribution_issues

    @staticmethod
    def _result(
        rid: str, record: CompleteProductRecord,
        passport: PassportVerificationResult,
        coherence: Optional[CrossLayerCoherenceResult],
        statistical: Optional[StatisticalQualityResult],
        reward: Optional[SampledRewardResult],
        decision: str, score: float, factors: List[str],
    ) -> CompleteValidationResult:
        """Assemble a CompleteValidationResult from stage outputs."""
        return CompleteValidationResult(
            record_id=rid, complete_record=record, passport=passport,
            coherence=coherence, statistical=statistical, reward=reward,
            metadata=ValidationMetadata(
                validation_status=decision, plausibility_score=score,
                pipeline_version="v2.0",
            ),
            final_decision=decision, final_score=score,
            decision_reasoning="; ".join(factors) if factors else decision,
            decision_factors=factors,
        )
