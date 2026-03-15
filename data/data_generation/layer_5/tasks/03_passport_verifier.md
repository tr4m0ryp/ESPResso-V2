# Task 03: Passport Verifier

## Codebase context

ESPResso-V2 Layer 5 V2 replaces redundant per-layer deterministic checks with
a lightweight passport verification system. Each upstream layer (1-4) stamps a
validation hash on its output records. Layer 5 verifies these hashes instead of
re-running the checks. If a passport is missing or invalid, the record is flagged
for the pipeline to decide how to handle it (reject or fall through to coherence).

## Design rules

- All code lives under `data/data_generation/layer_5/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere
- This is a new component -- no V1 code to remove

## Reference files to study

- `data/data_generation/layer_5/core/deterministic_validator.py` -- V1 pattern to
  understand the check structure being replaced
- `data/data_generation/layer_5/models/models.py` -- PassportVerificationResult
  dataclass (from task 01)
- `data/data_generation/layer_5/config/config.py` -- Layer5Config (from task 02)

## Dependencies

- Task 01 (PassportVerificationResult model)
- Task 02 (Layer5Config with passport_enabled flag)

## The task

Create `core/passport_verifier.py` with a `PassportVerifier` class.

### PassportVerifier class

```python
class PassportVerifier:
    def __init__(self, config: Layer5Config):
        self.config = config

    def verify(self, record: CompleteProductRecord) -> PassportVerificationResult:
        """Verify all upstream layer passports on a record.

        Checks for the presence and validity of validation hashes from
        layers 1-4. Each hash is a SHA-256 of the layer's key output fields,
        stored as a field on the record (e.g., layer1_passport_hash).

        If passport_enabled is False in config, returns a result with
        is_valid=True and all layer flags True (skip verification).

        Args:
            record: Complete product record with passport hash fields

        Returns:
            PassportVerificationResult with per-layer validity flags
        """
```

### Passport hash computation

Each layer's passport hash is computed by:
1. Extracting the layer's output fields (see below)
2. JSON-serializing them with `json.dumps(fields, sort_keys=True)`
3. Computing `hashlib.sha256(serialized.encode()).hexdigest()`

Layer field sets for hash computation:
- **Layer 1**: `materials`, `material_weights_kg`, `material_percentages`, `total_weight_kg`
- **Layer 2**: `preprocessing_path_id`, `preprocessing_steps`
- **Layer 3**: `transport_scenario_id`, `total_transport_distance_km`, `supply_chain_type`
- **Layer 4**: `packaging_config_id`, `packaging_categories`, `packaging_masses_kg`, `total_packaging_mass_kg`

### Passport field names on CompleteProductRecord

The record should have optional fields:
- `layer1_passport_hash: Optional[str] = None`
- `layer2_passport_hash: Optional[str] = None`
- `layer3_passport_hash: Optional[str] = None`
- `layer4_passport_hash: Optional[str] = None`

If a passport field is None (not present), the verifier should:
1. Add the layer name to `missing_passports` list
2. Set that layer's flag to False
3. Compute what the hash should be and log a warning

### Static helper

```python
@staticmethod
def compute_passport_hash(record: CompleteProductRecord, layer: int) -> str:
    """Compute the expected passport hash for a given layer.

    This can be used by upstream layers to stamp their output.

    Args:
        record: The product record
        layer: Layer number (1-4)

    Returns:
        SHA-256 hex digest of the layer's key fields
    """
```

### Batch method

```python
def verify_batch(self, records: List[CompleteProductRecord]) -> Dict[str, PassportVerificationResult]:
    """Verify passports for a batch of records.

    Args:
        records: List of records to verify

    Returns:
        Dict mapping record subcategory_id to PassportVerificationResult
    """
```

This simply iterates and calls `verify()` for each record. No LLM calls, no
threading needed -- this is pure CPU and should handle 10K+ records/second.

## Acceptance criteria

1. `PassportVerifier(config).verify(record_with_valid_passports).is_valid` returns True
2. `PassportVerifier(config).verify(record_missing_layer3_passport).layer3_hash_valid` returns False
3. `PassportVerifier(config).verify(record_missing_layer3_passport).missing_passports` contains "layer_3"
4. When `config.passport_enabled = False`, `verify()` returns all-True result
5. `compute_passport_hash()` produces deterministic output for the same input
6. `verify_batch()` returns results for all records

## Files to create

- `core/passport_verifier.py`

## Files to remove

- `core/deterministic_validator.py` -- replaced by passport verifier

## Reference

- V1 deterministic validator: `layer_5/core/deterministic_validator.py` (being removed)
- Hashlib pattern: standard library `hashlib.sha256`
