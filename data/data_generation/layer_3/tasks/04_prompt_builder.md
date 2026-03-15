# Task 04: Prompt Builder

## Codebase context

ESPResso-V2 is a synthetic data generation pipeline for textile product
lifecycle assessment. Layer 3 uses Claude Sonnet to generate per-leg
transport scenarios. The prompt builder assembles the static system prompt
from text files (task 03) and builds per-record user prompts from Layer 2
input data. This replaces the V1 `prompts/prompts.py`.

## Design rules

- All code lives under `data/data_generation/layer_3/`
- Use Python dataclasses for configuration
- Logging via `logging.getLogger(__name__)`
- Imports use the full package path
- No emojis anywhere

## Reference files to study

- `data/data_generation/layer_3/prompts/prompts.py` -- Current V1 prompt
  builder to be replaced. Note the REALISM_DEFINITION constant and the
  build_full_context() method
- `data/data_generation/layer_3/LAYER3_DESIGN.md` -- Section 6 (Prompt
  Architecture) describes the system prompt structure
- `data/data_generation/layer_3/io/layer2_reader.py` -- Layer2Record
  dataclass showing the input fields available for user prompt

## Dependencies

- Task 03 (system prompt files must exist in `prompts/system/`)

## The task

Create `prompts/builder.py` with a `PromptBuilder` class. This replaces
the existing `prompts/prompts.py`.

### PromptBuilder class

```python
class PromptBuilder:
    def __init__(self, config: Layer3Config):
        self.config = config
        self._system_prompt: Optional[str] = None

    def get_system_prompt(self) -> str:
        """Load and cache the concatenated system prompt from text files."""
        # Load once, cache for reuse (static across all records)
        # Read all files in prompts/system/ sorted by filename
        # Concatenate with double newlines between files
        # Return cached string on subsequent calls

    def build_user_prompt(self, record: Layer2Record, seed: int = 0,
                          warehouse: str = "EU") -> str:
        """Build per-record user prompt."""
        # Include:
        # - Product: category_name, subcategory_name, total_weight_kg
        # - Materials: name, weight_kg, percentage for each
        # - step_material_mapping as formatted text
        # - Target warehouse (EU or US)
        # - Seed number for variety

    def build_correction_prompt(self, record: Layer2Record,
                                 failures: List[str],
                                 seed: int = 0,
                                 warehouse: str = "EU") -> str:
        """Build user prompt with correction feedback for two-pass."""
        # Same as build_user_prompt() but appends a CORRECTIONS block
        # listing the specific failures and asking for fixes
```

### Key design decisions

- The system prompt is loaded ONCE and cached (it's static across records)
- The user prompt is built per-record with product-specific data
- The correction prompt is for the two-pass regeneration flow
- Remove all V1 concepts: build_full_context(), build_product_context(),
  build_geographic_context(), REALISM_DEFINITION constant
- The builder does NOT call the API -- it only assembles prompt strings

### Delete or mark V1 file

The existing `prompts/prompts.py` should be kept temporarily for backward
compatibility but the new builder.py is the V2 replacement.

## Acceptance criteria

1. `PromptBuilder(config).get_system_prompt()` returns a non-empty string
2. Calling get_system_prompt() twice returns the same cached string
3. `build_user_prompt(record)` includes all material names and weights
4. `build_user_prompt(record)` includes the step_material_mapping
5. `build_user_prompt(record)` includes the warehouse and seed
6. `build_correction_prompt(record, failures)` includes the failure text
7. No V1 methods remain (build_full_context, build_product_context, etc.)
