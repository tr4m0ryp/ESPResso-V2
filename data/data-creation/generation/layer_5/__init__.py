"""
Layer 5: Controlling and Validation Layer

Quality gate for synthetic data generation pipeline - validates all records 
from Layers 1-4 for consistency, plausibility, and training suitability using
configurable LLM API.

Components:
- config/config.py: Configuration settings and validation thresholds
- models/models.py: Data models for complete records and validation results
- clients/api_client.py: API client for Layer 5
- core/passport_verifier.py: Stage 1 - Upstream passport hash verification
- core/coherence_validator.py: Stage 2 - Cross-layer coherence via LLM
- core/statistical_validator.py: Stage 3 - Distribution monitoring and deduplication
- core/sampled_reward_scorer.py: Stage 4 - Sampled quality scoring via LLM
- core/decision_maker.py: Stage 5 - Final accept/review/reject logic
- io/writer_incremental.py: Incremental output writing and report generation
- core/orchestrator.py: Main pipeline orchestration
- main.py: Command-line interface

Validation Stages (V2):
1. Passport Verification: Upstream layer hash checks
2. Cross-Layer Coherence: Inter-layer consistency via LLM (50-record batches)
3. Statistical Quality: Distribution monitoring, dedup, outlier detection
4. Sampled Reward Scoring: 1-5% sample quality estimation via LLM
5. Final Decision: Accept/review/reject with rationale

Usage:
    from data.data_generation.layer_5 import Layer5Orchestrator, Layer5Config
    
    config = Layer5Config()
    orchestrator = Layer5Orchestrator(config)
    result = orchestrator.run_pipeline()
"""

from data.data_generation.layer_5.config.config import Layer5Config
from data.data_generation.layer_5.core.orchestrator import Layer5Orchestrator
from data.data_generation.layer_5.models.models import (
    CompleteProductRecord, CompleteValidationResult, ValidationSummary, ValidationPipelineStats,
    CrossLayerCoherenceResult, StatisticalQualityResult, SampledRewardResult,
    PassportVerificationResult, ValidationMetadata,
)
from data.data_generation.layer_5.core.passport_verifier import PassportVerifier
from data.data_generation.layer_5.core.coherence_validator import CoherenceValidator
from data.data_generation.layer_5.core.statistical_validator import StatisticalValidator
from data.data_generation.layer_5.core.sampled_reward_scorer import SampledRewardScorer
from data.data_generation.layer_5.core.decision_maker import DecisionMaker

__all__ = [
    'Layer5Config',
    'Layer5Orchestrator',
    'CompleteProductRecord',
    'CompleteValidationResult',
    'ValidationSummary',
    'ValidationPipelineStats',
    'CrossLayerCoherenceResult',
    'StatisticalQualityResult',
    'SampledRewardResult',
    'PassportVerificationResult',
    'ValidationMetadata',
    'PassportVerifier',
    'CoherenceValidator',
    'StatisticalValidator',
    'SampledRewardScorer',
    'DecisionMaker',
]

__version__ = '1.0.0'
__description__ = 'Layer 5 Validation Layer for quality control'