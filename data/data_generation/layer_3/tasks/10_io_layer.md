# Task 10: I/O Layer

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. The I/O layer handles reading Layer 2 input data
and writing Layer 3 output. V2 changes the output schema: 13 columns
instead of the V1 schema, transport_legs as a JSON string column, and
Parquet output in addition to CSV.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for data structures
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_3/io/output.py` -- Current V1 output writer
  with CSV headers, write_records(), checkpointing, progress tracking
- `data/data_generation/layer_3/io/layer2_reader.py` -- Current Layer 2
  reader (mostly unchanged for V2)
- `data/data_generation/layer_5/io/writer.py` or
  `data/data_generation/layer_5/io/writer_incremental.py` -- Pattern for
  Parquet output if available

## Dependencies

- Task 01 (Layer3Record model for serialization)

## The task

### Update io/output.py

Rewrite the OutputWriter for the V2 13-column schema.

**New CSV headers (13 columns):**
```python
HEADERS = [
    # Carried forward (11)
    "category_id", "category_name", "subcategory_id", "subcategory_name",
    "materials", "material_weights_kg", "material_percentages",
    "total_weight_kg", "preprocessing_path_id", "preprocessing_steps",
    "step_material_mapping",
    # Added by Layer 3 (2)
    "transport_legs", "total_distance_km",
]
```

**Remove V1 columns:**
- transport_scenario_id
- total_transport_distance_km
- supply_chain_type
- origin_region
- transport_modes
- reasoning
- reasoning_total_distance

**Key changes:**
- `write_records()` serializes `transport_legs` as a JSON string
  (the list of leg dicts)
- Other list/dict fields (materials, material_weights_kg, etc.) are
  serialized as JSON strings (same as V1)
- Add `write_parquet()` method using pandas for Parquet output
- Keep checkpointing, progress tracking, and summary stats
- Update `get_output_summary()` to report V2 metrics (avg legs per
  record, avg distance, location diversity)

**ProgressTracker:**
- Keep the existing ProgressTracker class mostly unchanged
- Remove references to V1 ValidationResult import

**Parquet output:**
```python
def write_parquet(self, records: List[Layer3Record],
                  output_path: Optional[Path] = None) -> bool:
    """Write records to Parquet format."""
    import pandas as pd
    # Convert records to dicts
    # Create DataFrame
    # Write to Parquet with snappy compression
```

### Update io/layer2_reader.py

Minimal changes needed:
- Keep the Layer2Record dataclass as-is (input schema unchanged)
- Keep Layer2DataReader class as-is
- Verify step_material_mapping is parsed correctly (Dict[str, List[str]])
- No V2-specific changes required

## Acceptance criteria

1. OutputWriter writes 13-column CSV (not V1's 17 columns)
2. transport_legs column contains valid JSON string of leg arrays
3. `write_parquet()` produces a readable Parquet file
4. Checkpointing works with V2 schema
5. `get_output_summary()` reports avg legs per record
6. Layer2DataReader still reads Layer 2 Parquet files correctly
7. No V1 column names remain in the output
