"""
Layer 6: Carbon Footprint Calculation Layer

Final layer of the synthetic data generation pipeline. Processes validated
records from Layer 5 (or Layer 4 copy) and calculates complete carbon
footprints using deterministic formulas from the research paper.

Calculation Flow:
1. Raw Materials: CF_raw = sum(w_i * EF_i)
2. Transport: CF_transport = (W/1000) * D * (EF_weighted/1000)
3. Processing: CF_processing = sum(w_m * (combined_EF - raw_EF))
4. Packaging: CF_packaging = sum(m_i * EF_i)
5. Adjustments: CF_total = CF_modelled * 1.02

Output: ~100,000 complete records ready for ML model training

Components:
- config/config.py: Configuration dataclass with paths and parameters
- core/calculator.py: Main carbon footprint calculation logic
- core/transport_model.py: Multinomial logit model for transport modes
- core/orchestrator.py: Pipeline orchestration
- io/writer.py: Output writing utilities

Python Usage:
    python -m data_generation.scripts.run_layer_6

Legacy C Implementation (deprecated):
- layer6_calculation.h/c: Legacy C calculation logic
- main.c: Legacy C entry point
- Makefile: Legacy build system
"""

from data.data_generation.layer_6.config.config import Layer6Config
from data.data_generation.layer_6.core.orchestrator import Layer6Orchestrator
from data.data_generation.layer_6.core.calculator import CarbonFootprintCalculator
from data.data_generation.layer_6.core.transport_model import TransportModeModel

__version__ = '2.0.0'
__description__ = 'Layer 6 Carbon Footprint Calculation (Python implementation)'
__status__ = 'COMPLETE'

__all__ = [
    'Layer6Config',
    'Layer6Orchestrator',
    'CarbonFootprintCalculator',
    'TransportModeModel'
]
