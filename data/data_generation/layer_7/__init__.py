"""
Layer 7: Water Footprint Calculation Layer

Processes validated records from Layer 5 (joined with Layer 4 transport
legs) and calculates complete water footprints using AWARE-weighted
consumption factors from EcoInvent 3.12 and Agribalyse 3.2.

Calculation Flow:
1. Raw Materials: WF_raw = sum(w_i * WU_material_i * AWARE_agri_i)
2. Processing:    WF_proc = sum(w_m * WU_process_p * AWARE_nonagri_p)
3. Transport:     WF_transport = 0 (not applicable for water)
4. Packaging:     WF_pack = sum(m_j * WU_packaging_j) (no AWARE)
5. Total:         WF_total = WF_raw + WF_proc + WF_pack

Output unit: m3 world-equivalent (AWARE-weighted)

Components:
- config/config.py: Configuration dataclass with paths and parameters
- core/calculator.py: Main water footprint calculation logic
- core/components.py: Individual component calculators
- core/databases.py: Water reference database wrappers
- core/country_resolver.py: AWARE country extraction and matching
- core/orchestrator.py: Pipeline orchestration
- core/_processing.py: Input file handling and record processing
- enrichment/data_joiner.py: Layer 5 + Layer 4 join via pp-XXXXXX
- io/writer.py: CSV output writing

Python Usage:
    python -m data.data_generation.scripts.run_layer_7
"""

from data.data_generation.layer_7.config.config import Layer7Config
from data.data_generation.layer_7.core.orchestrator import (
    Layer7Orchestrator,
)

__version__ = '1.0.0'
__description__ = 'Layer 7 Water Footprint Calculation'
__status__ = 'ACTIVE'

__all__ = [
    'Layer7Config',
    'Layer7Orchestrator',
]
