Rewrite the IO layer: `io/input_reader.py` for reading Layer 3 output and `io/writer.py`
for writing Layer 4 output.

## What to remove

- `io/packaging_data.py` -- V1 packaging materials database (17 materials). No longer
  needed with 3-category approach.
- `io/progress_tracker.py` -- merge progress tracking into writer or orchestrator inline.

## io/input_reader.py -- Layer3Reader class

Reads the Layer 3 Parquet output and yields records as dicts.

```python
class Layer3Reader:
    def __init__(self, config: Layer4Config):
        self.config = config
        self.input_path = config.layer3_output_path
```

### read_all() -> pd.DataFrame

Read the full Layer 3 Parquet file into a DataFrame. Log the number of records loaded.

### iter_records() -> Iterator[Dict[str, Any]]

Yield one record at a time as a dict. Uses `read_all().iterrows()` and converts each row
to a dict. This is the primary interface used by the orchestrator.

### get_record_count() -> int

Return the total number of records without loading the full DataFrame. Use
`pd.read_parquet(...).shape[0]` or pyarrow metadata if available.

### read_from_checkpoint(start_index: int) -> Iterator[Dict[str, Any]]

Same as `iter_records()` but skip the first `start_index` records. Used for resuming
from a checkpoint.

**V2 Layer 3 schema (13 columns):**

11 carried forward from L1/L2:
- category_id, category_name, subcategory_id, subcategory_name
- materials (JSON list), material_weights_kg (JSON list), material_percentages (JSON list)
- total_weight_kg
- preprocessing_path_id, preprocessing_steps (JSON list), step_material_mapping (JSON dict)

2 added by Layer 3:
- transport_legs (JSON string containing array of TransportLeg dicts)
- total_distance_km (float)

Note: transport_legs is a JSON-encoded string in the Parquet file. Parsing is deferred to
the prompt builder and model layer -- the reader passes it through as-is.

## io/writer.py -- OutputWriter class

Writes Layer 4 records to Parquet output.

```python
class OutputWriter:
    def __init__(self, config: Layer4Config):
        self.config = config
        self.output_path = config.output_path
        self.checkpoint_dir = config.checkpoint_dir
```

### V2 output schema (16 columns)

```python
HEADERS = [
    # Carried forward from L1/L2 (11)
    "category_id",
    "category_name",
    "subcategory_id",
    "subcategory_name",
    "materials",
    "material_weights_kg",
    "material_percentages",
    "total_weight_kg",
    "preprocessing_path_id",
    "preprocessing_steps",
    "step_material_mapping",
    # Carried forward from Layer 3 (2)
    "transport_legs",
    "total_distance_km",
    # Added by Layer 4 (3)
    "packaging_categories",
    "packaging_masses_kg",
    "packaging_reasoning",
]
```

### write_records(records: List[Layer4Record]) -> None

Write a list of Layer4Record objects to the output Parquet file.

1. Convert each record to a dict via `record.to_dict()`.
2. Build a DataFrame from the list of dicts.
3. Write to `self.output_path` using `pd.DataFrame.to_parquet()` with snappy compression.
4. Log the number of records written and the output file path.

### write_checkpoint(records: List[Layer4Record], checkpoint_index: int) -> None

Write an intermediate checkpoint file to `self.checkpoint_dir`.

Filename: `checkpoint_{checkpoint_index:06d}.parquet`

Same format as `write_records()` but to the checkpoint path. Also write a small JSON
metadata file alongside: `checkpoint_{checkpoint_index:06d}_meta.json` containing:
```json
{
    "checkpoint_index": 12000,
    "records_in_file": 5000,
    "timestamp": "2026-03-13T14:30:00"
}
```

### merge_checkpoints() -> None

Read all checkpoint Parquet files from `self.checkpoint_dir`, concatenate into a single
DataFrame, and write to `self.output_path`. Delete checkpoint files after successful merge.

This is called at the end of generation to produce the final output file.

### get_last_checkpoint_index() -> int

Scan checkpoint directory for the highest checkpoint index. Return 0 if no checkpoints
exist. Used for resume-from-checkpoint.

### get_output_summary() -> Dict[str, Any]

Read the final output Parquet and return summary statistics:
```python
{
    "total_records": int,
    "columns": List[str],
    "mean_paper_cardboard_kg": float,
    "mean_plastic_kg": float,
    "mean_other_kg": float,
    "mean_total_packaging_kg": float,
    "mean_packaging_ratio": float,  # mean(pkg_total / product_weight)
}
```

## Design rules

- Use pandas for Parquet I/O with snappy compression.
- JSON-encoded list/dict fields in Layer 3 input are passed through as strings. Do not
  re-parse and re-serialize them unnecessarily in the writer.
- Layer 4 output fields (`packaging_categories`, `packaging_masses_kg`) are serialized
  as JSON strings in the Parquet output for consistency with the upstream pattern.
- No emojis.

## Files to create/modify

- `io/input_reader.py` -- rewrite (currently exists but may need V2 schema updates)
- `io/writer.py` -- rewrite for V2 schema

## Files to remove

- `io/packaging_data.py`
- `io/progress_tracker.py`

## Reference

- Layer 3 IO: `layer_3/io/output.py` (OutputWriter, HEADERS, ProgressTracker)
- Layer 3 reader: `layer_3/io/layer2_reader.py` (Layer2DataReader)
- Design doc: `layer_4/DESIGN_V2.md` section 5 (Output Schema)
