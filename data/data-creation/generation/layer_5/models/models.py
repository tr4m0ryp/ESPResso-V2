"""
Data models for Layer 5: Controlling and Validation Layer (V2).

Defines the complete product record structure combining all layers
and validation result models for the V2 pipeline. V2 uses passport
verification, cross-layer coherence, statistical quality, and
sampled reward scoring.
"""

from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Any
from datetime import datetime

import logging

logger = logging.getLogger(__name__)


@dataclass
class CompleteProductRecord:
    """Complete product record combining all layers (22 fields)."""

    # Layer 1: Product Composition
    category_id: str
    category_name: str
    subcategory_id: str
    subcategory_name: str
    materials: List[str]
    material_weights_kg: List[float]
    material_percentages: List[float]
    total_weight_kg: float

    # Layer 2: Preprocessing Path
    preprocessing_path_id: str
    preprocessing_steps: List[str]

    # Layer 3: Transport Scenario
    transport_scenario_id: str
    total_transport_distance_km: float
    supply_chain_type: str
    transport_items: List[Dict[str, Any]]
    transport_modes: List[str]
    transport_distances_kg: List[float]
    transport_emissions_kg_co2e: List[float]

    # Layer 4: Packaging Configuration
    packaging_config_id: str
    packaging_items: List[Dict[str, Any]]
    packaging_categories: List[str]
    packaging_masses_kg: List[float]
    total_packaging_mass_kg: float

    # Passport hashes (set by upstream layers, verified by Layer 5)
    layer1_passport_hash: Optional[str] = None
    layer2_passport_hash: Optional[str] = None
    layer3_passport_hash: Optional[str] = None
    layer4_passport_hash: Optional[str] = None


@dataclass
class PassportVerificationResult:
    """Result of passport hash verification against upstream layer validators."""

    is_valid: bool
    layer1_hash_valid: bool = True
    layer2_hash_valid: bool = True
    layer3_hash_valid: bool = True
    layer4_hash_valid: bool = True
    missing_passports: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


@dataclass
class CrossLayerCoherenceResult:
    """Result of cross-layer coherence evaluation via LLM.

    Focused on inter-layer consistency rather than per-layer validity.
    """

    # 0-1: does material->processing->transport->packaging tell a
    # coherent story?
    lifecycle_coherence_score: float
    # 0-1: absence of contradictions between layers
    cross_layer_contradiction_score: float
    # 0-1: combined coherence assessment
    overall_coherence_score: float
    contradictions_found: List[str] = field(default_factory=list)
    recommendation: str = "review"  # "accept", "review", "reject"


@dataclass
class StatisticalQualityResult:
    """Result of statistical validation with cross-layer correlation checks."""

    is_duplicate: bool = False
    duplicate_similarity: float = 0.0
    is_outlier: bool = False
    outlier_type: Optional[str] = None

    # Distribution flags
    material_distribution_ok: bool = True
    category_distribution_ok: bool = True
    transport_distribution_ok: bool = True
    packaging_distribution_ok: bool = True

    # Cross-layer correlation flags
    weight_packaging_correlation_ok: bool = True
    material_transport_correlation_ok: bool = True

    distribution_issues: List[str] = field(default_factory=list)


@dataclass
class SampledRewardResult:
    """Result of sampled reward scoring. Only populated for sampled records."""

    was_sampled: bool = False
    reward_score: Optional[float] = None
    # "High quality", "Acceptable", "Marginal", "Low quality"
    quality_interpretation: Optional[str] = None
    # Estimated from sample distribution
    dataset_estimated_quality: Optional[float] = None


@dataclass
class ValidationMetadata:
    """Metadata added during the validation process."""

    validation_status: str  # "accepted", "review", "rejected"
    plausibility_score: float

    deterministic_flags: List[str] = field(default_factory=list)
    semantic_issues: List[str] = field(default_factory=list)

    pipeline_version: str = "v2.0"
    validation_timestamp: str = field(
        default_factory=lambda: datetime.now().isoformat()
    )
    record_hash: str = ""

    # Processing metadata
    processing_batch_id: str = ""
    validation_worker_id: str = ""
    processing_duration_seconds: float = 0.0


@dataclass
class CompleteValidationResult:
    """Complete validation result combining all V2 validation stages."""

    record_id: str
    complete_record: CompleteProductRecord
    passport: PassportVerificationResult
    coherence: Optional[CrossLayerCoherenceResult] = None
    statistical: Optional[StatisticalQualityResult] = None
    reward: Optional[SampledRewardResult] = None
    metadata: Optional[ValidationMetadata] = None

    # Final decision
    final_decision: str = "review"  # "accept", "review", "reject"
    final_score: float = 0.0

    # Decision rationale
    decision_reasoning: str = ""
    decision_factors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert the validation result to a plain dictionary."""
        result: Dict[str, Any] = {
            "record_id": self.record_id,
            "complete_record": asdict(self.complete_record),
            "passport": asdict(self.passport),
            "final_decision": self.final_decision,
            "final_score": self.final_score,
            "decision_reasoning": self.decision_reasoning,
            "decision_factors": self.decision_factors,
        }
        if self.coherence is not None:
            result["coherence"] = asdict(self.coherence)
        if self.statistical is not None:
            result["statistical"] = asdict(self.statistical)
        if self.reward is not None:
            result["reward"] = asdict(self.reward)
        if self.metadata is not None:
            result["metadata"] = asdict(self.metadata)
        return result

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CompleteValidationResult":
        """Reconstruct a CompleteValidationResult from a dictionary."""
        record_data = data.get("complete_record", {})
        complete_record = CompleteProductRecord(
            category_id=record_data.get("category_id", ""),
            category_name=record_data.get("category_name", ""),
            subcategory_id=record_data.get("subcategory_id", ""),
            subcategory_name=record_data.get("subcategory_name", ""),
            materials=record_data.get("materials", []),
            material_weights_kg=record_data.get(
                "material_weights_kg", []
            ),
            material_percentages=record_data.get(
                "material_percentages", []
            ),
            total_weight_kg=record_data.get("total_weight_kg", 0.0),
            preprocessing_path_id=record_data.get(
                "preprocessing_path_id", ""
            ),
            preprocessing_steps=record_data.get(
                "preprocessing_steps", []
            ),
            transport_scenario_id=record_data.get(
                "transport_scenario_id", ""
            ),
            total_transport_distance_km=record_data.get(
                "total_transport_distance_km", 0.0
            ),
            supply_chain_type=record_data.get("supply_chain_type", ""),
            transport_items=record_data.get("transport_items", []),
            transport_modes=record_data.get("transport_modes", []),
            transport_distances_kg=record_data.get(
                "transport_distances_kg", []
            ),
            transport_emissions_kg_co2e=record_data.get(
                "transport_emissions_kg_co2e", []
            ),
            packaging_config_id=record_data.get(
                "packaging_config_id", ""
            ),
            packaging_items=record_data.get("packaging_items", []),
            packaging_categories=record_data.get(
                "packaging_categories", []
            ),
            packaging_masses_kg=record_data.get(
                "packaging_masses_kg", []
            ),
            total_packaging_mass_kg=record_data.get(
                "total_packaging_mass_kg", 0.0
            ),
            layer1_passport_hash=record_data.get(
                "layer1_passport_hash"
            ),
            layer2_passport_hash=record_data.get(
                "layer2_passport_hash"
            ),
            layer3_passport_hash=record_data.get(
                "layer3_passport_hash"
            ),
            layer4_passport_hash=record_data.get(
                "layer4_passport_hash"
            ),
        )

        # Passport (required)
        pp_data = data.get("passport", {})
        passport = PassportVerificationResult(
            is_valid=pp_data.get("is_valid", False),
            layer1_hash_valid=pp_data.get("layer1_hash_valid", True),
            layer2_hash_valid=pp_data.get("layer2_hash_valid", True),
            layer3_hash_valid=pp_data.get("layer3_hash_valid", True),
            layer4_hash_valid=pp_data.get("layer4_hash_valid", True),
            missing_passports=pp_data.get("missing_passports", []),
            errors=pp_data.get("errors", []),
        )

        # Coherence (optional)
        coherence = None
        coh_data = data.get("coherence")
        if coh_data is not None:
            coherence = CrossLayerCoherenceResult(
                lifecycle_coherence_score=coh_data.get(
                    "lifecycle_coherence_score", 0.0
                ),
                cross_layer_contradiction_score=coh_data.get(
                    "cross_layer_contradiction_score", 0.0
                ),
                overall_coherence_score=coh_data.get(
                    "overall_coherence_score", 0.0
                ),
                contradictions_found=coh_data.get(
                    "contradictions_found", []
                ),
                recommendation=coh_data.get("recommendation", "review"),
            )

        # Statistical (optional)
        statistical = None
        stat_data = data.get("statistical")
        if stat_data is not None:
            statistical = StatisticalQualityResult(
                is_duplicate=stat_data.get("is_duplicate", False),
                duplicate_similarity=stat_data.get(
                    "duplicate_similarity", 0.0
                ),
                is_outlier=stat_data.get("is_outlier", False),
                outlier_type=stat_data.get("outlier_type"),
                material_distribution_ok=stat_data.get(
                    "material_distribution_ok", True
                ),
                category_distribution_ok=stat_data.get(
                    "category_distribution_ok", True
                ),
                transport_distribution_ok=stat_data.get(
                    "transport_distribution_ok", True
                ),
                packaging_distribution_ok=stat_data.get(
                    "packaging_distribution_ok", True
                ),
                weight_packaging_correlation_ok=stat_data.get(
                    "weight_packaging_correlation_ok", True
                ),
                material_transport_correlation_ok=stat_data.get(
                    "material_transport_correlation_ok", True
                ),
                distribution_issues=stat_data.get(
                    "distribution_issues", []
                ),
            )

        # Reward (optional)
        reward = None
        rwd_data = data.get("reward")
        if rwd_data is not None:
            reward = SampledRewardResult(
                was_sampled=rwd_data.get("was_sampled", False),
                reward_score=rwd_data.get("reward_score"),
                quality_interpretation=rwd_data.get(
                    "quality_interpretation"
                ),
                dataset_estimated_quality=rwd_data.get(
                    "dataset_estimated_quality"
                ),
            )

        # Metadata (optional)
        metadata = None
        meta_data = data.get("metadata")
        if meta_data is not None:
            metadata = ValidationMetadata(
                validation_status=meta_data.get(
                    "validation_status", "review"
                ),
                plausibility_score=meta_data.get(
                    "plausibility_score", 0.0
                ),
                deterministic_flags=meta_data.get(
                    "deterministic_flags", []
                ),
                semantic_issues=meta_data.get("semantic_issues", []),
                pipeline_version=meta_data.get(
                    "pipeline_version", "v2.0"
                ),
                validation_timestamp=meta_data.get(
                    "validation_timestamp",
                    datetime.now().isoformat(),
                ),
                record_hash=meta_data.get("record_hash", ""),
                processing_batch_id=meta_data.get(
                    "processing_batch_id", ""
                ),
                validation_worker_id=meta_data.get(
                    "validation_worker_id", ""
                ),
                processing_duration_seconds=meta_data.get(
                    "processing_duration_seconds", 0.0
                ),
            )

        return cls(
            record_id=data.get("record_id", ""),
            complete_record=complete_record,
            passport=passport,
            coherence=coherence,
            statistical=statistical,
            reward=reward,
            metadata=metadata,
            final_decision=data.get("final_decision", "review"),
            final_score=data.get("final_score", 0.0),
            decision_reasoning=data.get("decision_reasoning", ""),
            decision_factors=data.get("decision_factors", []),
        )


@dataclass
class ValidationSummary:
    """Summary of the validation process for reporting."""

    total_records_processed: int
    accepted_records: int
    review_queue_records: int
    rejected_records: int

    # Acceptance rates
    acceptance_rate: float
    review_rate: float
    rejection_rate: float

    # Score statistics
    average_plausibility_score: float

    # Sampled reward statistics
    sampled_reward_records: int = 0
    estimated_dataset_quality: float = 0.0

    # Passport and coherence failure counters
    passport_failures: int = 0
    cross_layer_issues: Dict[str, int] = field(default_factory=dict)

    # Processing statistics
    processing_duration_seconds: float = 0.0
    average_processing_time_per_record: float = 0.0

    # Error statistics
    deterministic_check_failures: Dict[str, int] = field(
        default_factory=dict
    )
    semantic_issues: Dict[str, int] = field(default_factory=dict)

    # Distribution statistics
    distribution_coverage: Dict[str, Any] = field(default_factory=dict)
    duplicates_removed: int = 0
    outliers_flagged: int = 0

    # Quality metrics
    quality_metrics: Dict[str, float] = field(default_factory=dict)


@dataclass
class ValidationPipelineStats:
    """Real-time statistics during validation pipeline execution."""

    records_processed: int = 0
    records_accepted: int = 0
    records_in_review: int = 0
    records_rejected: int = 0

    # Error counters
    deterministic_errors: int = 0
    passport_failures: int = 0
    coherence_errors: int = 0
    api_errors: int = 0
    sampled_records: int = 0

    # Timing
    start_time: Optional[str] = None
    end_time: Optional[str] = None

    # Current batch
    current_batch_id: str = ""
    current_batch_size: int = 0
    current_batch_processed: int = 0

    def get_acceptance_rate(self) -> float:
        """Calculate acceptance rate."""
        if self.records_processed == 0:
            return 0.0
        return self.records_accepted / self.records_processed

    def get_rejection_rate(self) -> float:
        """Calculate rejection rate."""
        if self.records_processed == 0:
            return 0.0
        return self.records_rejected / self.records_processed

    def get_review_rate(self) -> float:
        """Calculate review rate."""
        if self.records_processed == 0:
            return 0.0
        return self.records_in_review / self.records_processed
