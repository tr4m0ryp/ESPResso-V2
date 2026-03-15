# Task 01: Data Models

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product lifecycle
assessment. Layer 5 is being redesigned from a "catch everything" quality gate
to a focused cross-layer coherence checker. The old V1 models supported 5
per-layer deterministic checks, per-record semantic scoring, per-record reward
scoring, and statistical validation. The V2 models support passport verification,
cross-layer coherence (LLM), statistical quality, sampled reward scoring, and
a final decision stage.

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Use Python dataclasses with `from dataclasses import dataclass, field`
- Use standard library `typing` for type hints
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path: `from data.data_generation.layer_5.models.models import ...`
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_5/models/models.py` -- Current V1 models to replace
- `data/data_generation/layer_3/models/models.py` -- Pattern for ValidationResult, Layer3Record
- `data/data_generation/layer_4/models/models.py` -- Pattern for PackagingResult, Layer4Record

## The task

Rewrite `models/models.py` with V2 dataclasses. Remove all V1 models. Create the
`models/__init__.py` re-exporting all public models.

### CompleteProductRecord (keep, unchanged)

Same as V1. 22 fields covering layers 1-4. No changes needed.

### PassportVerificationResult (new)

Result of passport hash verification against upstream layer validators.

```python
@dataclass
class PassportVerificationResult:
    is_valid: bool
    layer1_hash_valid: bool = True
    layer2_hash_valid: bool = True
    layer3_hash_valid: bool = True
    layer4_hash_valid: bool = True
    missing_passports: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
```

### CrossLayerCoherenceResult (new, replaces SemanticValidationResult)

Result of cross-layer coherence evaluation via LLM. Focused on inter-layer
consistency, not per-layer validity.

```python
@dataclass
class CrossLayerCoherenceResult:
    lifecycle_coherence_score: float       # 0-1: does material->processing->transport->packaging tell a coherent story?
    cross_layer_contradiction_score: float # 0-1: absence of contradictions between layers
    overall_coherence_score: float         # 0-1: combined coherence assessment
    contradictions_found: List[str] = field(default_factory=list)
    recommendation: str = "review"        # "accept", "review", "reject"
```

### StatisticalQualityResult (replaces StatisticalValidationResult)

Result of statistical validation. Same structure as V1 but adds cross-layer
correlation fields.

```python
@dataclass
class StatisticalQualityResult:
    is_duplicate: bool = False
    duplicate_similarity: float = 0.0
    is_outlier: bool = False
    outlier_type: Optional[str] = None
    # Distribution flags
    material_distribution_ok: bool = True
    category_distribution_ok: bool = True
    transport_distribution_ok: bool = True
    packaging_distribution_ok: bool = True
    # Cross-layer correlation flags (new)
    weight_packaging_correlation_ok: bool = True
    material_transport_correlation_ok: bool = True
    distribution_issues: List[str] = field(default_factory=list)
```

### SampledRewardResult (new, replaces RewardValidationResult)

Result of sampled reward scoring. Only populated for sampled records.

```python
@dataclass
class SampledRewardResult:
    was_sampled: bool = False
    reward_score: Optional[float] = None
    quality_interpretation: Optional[str] = None  # "High quality", "Acceptable", "Marginal", "Low quality"
    dataset_estimated_quality: Optional[float] = None  # Estimated from sample distribution
```

### ValidationMetadata (keep, modify)

Update `pipeline_version` default to `"v2.0"`. Remove `reward_score` field
(moved to SampledRewardResult). Keep all other fields.

### CompleteValidationResult (rewrite)

```python
@dataclass
class CompleteValidationResult:
    record_id: str
    complete_record: CompleteProductRecord
    passport: PassportVerificationResult
    coherence: Optional[CrossLayerCoherenceResult] = None
    statistical: Optional[StatisticalQualityResult] = None
    reward: Optional[SampledRewardResult] = None
    metadata: Optional[ValidationMetadata] = None
    final_decision: str = "review"    # "accept", "review", "reject"
    final_score: float = 0.0
    decision_reasoning: str = ""
    decision_factors: List[str] = field(default_factory=list)
```

### ValidationSummary (keep, modify)

Remove `average_reward_score` field. Add:
- `sampled_reward_records: int = 0`
- `estimated_dataset_quality: float = 0.0`
- `passport_failures: int = 0`
- `cross_layer_issues: Dict[str, int] = field(default_factory=dict)`

### ValidationPipelineStats (keep, modify)

Remove `semantic_errors`. Add:
- `passport_failures: int = 0`
- `coherence_errors: int = 0`
- `sampled_records: int = 0`

### Remove

- `DeterministicValidationResult` -- no longer used (passport replaces it)
- `SemanticValidationResult` -- replaced by CrossLayerCoherenceResult
- `RewardValidationResult` -- replaced by SampledRewardResult
- `create_validation_result_from_dict()` -- rewrite as `CompleteValidationResult.from_dict()` classmethod

### Helper: CompleteValidationResult.from_dict()

Classmethod that reconstructs a CompleteValidationResult from a dictionary.
Same logic as the old `create_validation_result_from_dict()` but updated for
V2 model fields.

## Acceptance criteria

1. `from data.data_generation.layer_5.models import CompleteProductRecord, PassportVerificationResult, CrossLayerCoherenceResult, StatisticalQualityResult, SampledRewardResult, CompleteValidationResult` works
2. No V1 model classes exist (DeterministicValidationResult, SemanticValidationResult, RewardValidationResult)
3. `CompleteValidationResult.from_dict(result.to_dict())` round-trips correctly (add `to_dict()` method)
4. All dataclasses use `field(default_factory=list)` for mutable defaults
5. `pipeline_version` defaults to `"v2.0"`

## Files to create / modify

- `models/models.py` -- complete rewrite
- `models/__init__.py` -- update re-exports

## Reference

- V1 models: `layer_5/models/models.py` (current file, to be replaced)
- Layer 3 pattern: `layer_3/models/models.py`
