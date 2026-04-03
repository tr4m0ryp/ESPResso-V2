"""
Layer 4 Packaging Configuration Generator (V2)

Generates realistic packaging material estimates for textile products
based on Layer 3 transport scenario data using an LLM API.

Components:
    config/config.py         -- Layer4Config
    models/models.py         -- PackagingResult, Layer4Record, ValidationResult
    prompts/builder.py       -- PromptBuilder
    clients/api_client.py    -- Layer4Client
    core/generator.py        -- PackagingGenerator
    core/validator.py        -- PackagingValidator
    core/orchestrator.py     -- Layer4Orchestrator
    io/input_reader.py       -- Layer3Reader
    io/writer.py             -- OutputWriter, HEADERS
"""

from data.data_generation.layer_4.config.config import Layer4Config
from data.data_generation.layer_4.models.models import (
    Layer4Record,
    PackagingResult,
    ValidationResult,
)

__all__ = [
    "Layer4Config",
    "Layer4Record",
    "PackagingResult",
    "ValidationResult",
]
