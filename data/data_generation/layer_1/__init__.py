"""
Layer 1: Product Composition Generator

This module generates realistic product compositions for fashion products
using configurable LLM API.
"""

from data.data_generation.layer_1.config.config import Layer1Config
from data.data_generation.layer_1.models.materials import MaterialDatabase, MaterialCategoryMapper
from data.data_generation.layer_1.models.taxonomy import TaxonomyLoader
from data.data_generation.layer_1.core.generator import ProductCompositionGenerator
from data.data_generation.layer_1.core.validator import CompositionValidator
from data.data_generation.layer_1.core.orchestrator import Layer1Orchestrator

__all__ = [
    'Layer1Config',
    'MaterialDatabase',
    'MaterialCategoryMapper',
    'TaxonomyLoader',
    'ProductCompositionGenerator',
    'CompositionValidator',
    'Layer1Orchestrator',
]
