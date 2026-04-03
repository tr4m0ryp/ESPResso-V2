"""Material family mappings for non-textile processing emission factors.

Data-only module (following _synonym_data.py pattern) that maps the 30
base materials without textile processing combinations to industrial
processing families with research-backed emission factors.

Primary data:
    MATERIAL_FAMILY_MAP -- Canonical material name to family identifier.
    FAMILY_PROCESSING_STEPS -- Per-family industrial steps with EFs and
        source references.
    FAMILY_APPLICABLE_EXISTING -- Which of the 32 existing textile steps
        also apply to each family.

Sources:
    Ecoinvent 3.9.1 (metal forming, cork, polymer processing)
    UNIDO Leather Carbon Footprint (tannery ~3.5 kgCO2e/kg)
    Arburg 2022 (injection moulding 1.07 kgCO2e/kg electric)
    IDFB White Paper 2019 (down/feather processing)
    Yulex rubber CO2e emissions data
"""

from typing import Dict, List, Set, Tuple

# Maps each of the 30 non-textile materials to a family identifier.
MATERIAL_FAMILY_MAP: Dict[str, str] = {
    # Metals
    'aluminium, primary, ingot': 'metal',
    'aluminium, wrought alloy': 'metal',
    'brass': 'metal',
    'copper, cathode': 'metal',
    'nickel, class 1': 'metal',
    'steel, chromium steel 18/8': 'metal',
    'steel, low-alloyed': 'metal',
    'zinc': 'metal',
    # Leather / hides
    'beef hides, at slaughterhouse (GB)': 'leather',
    'cowhide, from beef, at slaughterhouse': 'leather',
    'lamb hide, at slaughterhouse': 'leather',
    'veal hide, at slaughterhouse': 'leather',
    # Foams
    'polystyrene foam slab': 'foam',
    'polyurethane, flexible foam': 'foam',
    'polyurethane, flexible foam, flame retardant': 'foam',
    'polyurethane, rigid foam': 'foam',
    # Polymers
    'ethylene vinyl acetate copolymer': 'polymer',
    'polyethylene terephthalate, granulate, amorphous': 'polymer',
    'polyethylene terephthalate, granulate, bottle grade': 'polymer',
    'polylactic acid, granulate': 'polymer',
    # Rubber
    'latex': 'rubber',
    'seal, natural rubber based': 'rubber',
    'styrene butadiene rubber, emulsion polymerised': 'rubber',
    'styrene butadiene rubber, solution polymerised': 'rubber',
    'synthetic rubber': 'rubber',
    # Feathers
    'chicken feathers, at slaughterhouse': 'feathers',
    'duck feathers (fattened), at slaughterhouse': 'feathers',
    'duck feathers, at slaughterhouse': 'feathers',
    # Cork
    'cork slab': 'cork',
    'cork, raw': 'cork',
}

# Per-family industrial processing steps.
# Each tuple: (step_name, ef_kgCO2e_per_kg, source_short)
FAMILY_PROCESSING_STEPS: Dict[str, List[Tuple[str, float, str]]] = {
    'metal': [
        ('stamping', 0.45, 'Ecoinvent 3.9.1 metal forming'),
        ('machining', 0.80, 'Ecoinvent 3.9.1 metal working'),
        ('electroplating', 1.20, 'Ecoinvent 3.9.1 surface treatment'),
        ('casting', 0.60, 'Ecoinvent 3.9.1 metal casting'),
    ],
    'leather': [
        ('beamhouse', 0.80, 'UNIDO leather LCA'),
        ('chrome_tanning', 1.50, 'UNIDO leather LCA'),
        ('retanning_dyeing', 0.70, 'UNIDO leather LCA'),
        ('leather_finishing', 0.50, 'UNIDO leather LCA'),
    ],
    'foam': [
        ('foam_moulding', 1.50, 'Polymer processing LCA'),
        ('foam_cutting', 0.20, 'Polymer processing LCA'),
        ('foam_lamination', 0.80, 'Polymer processing LCA'),
    ],
    'polymer': [
        ('injection_moulding', 1.07, 'Arburg 2022 electric IM'),
        ('thermoforming', 1.20, 'Ecoinvent 3.9.1'),
        ('polymer_extrusion', 0.85, 'Ecoinvent 3.9.1'),
    ],
    'rubber': [
        ('vulcanisation', 1.50, 'Yulex/Ecoinvent'),
        ('rubber_moulding', 1.20, 'Yulex/Ecoinvent'),
        ('calendering_rubber', 0.60, 'Yulex/Ecoinvent'),
    ],
    'feathers': [
        ('washing_sanitising', 0.90, 'IDFB White Paper 2019'),
        ('sorting_grading', 0.15, 'IDFB White Paper 2019'),
        ('drying', 0.60, 'IDFB White Paper 2019'),
    ],
    'cork': [
        ('cork_boiling', 0.25, 'Ecoinvent 3.9.1 cork processing'),
        ('cork_pressing', 0.35, 'Ecoinvent 3.9.1 cork processing'),
        ('cork_grinding', 0.20, 'Ecoinvent 3.9.1 cork processing'),
    ],
}

# Existing textile steps that also apply to each non-textile family.
# Steps NOT in this set contribute zero processing for that family.
FAMILY_APPLICABLE_EXISTING: Dict[str, Set[str]] = {
    'metal': {
        'cutting', 'coating', 'finishing',
    },
    'leather': {
        'cutting', 'coating', 'finishing', 'printing',
        'waterproofing', 'flame retardant treatment',
        'softening',
    },
    'foam': {
        'cutting', 'coating', 'laminating', 'finishing',
        'flame retardant treatment',
    },
    'polymer': {
        'cutting', 'coating', 'finishing', 'extrusion',
        'heat setting', 'drawing',
    },
    'rubber': {
        'cutting', 'coating', 'finishing', 'calendering',
        'laminating',
    },
    'feathers': {
        'cutting', 'finishing', 'antimicrobial treatment',
    },
    'cork': {
        'cutting', 'coating', 'finishing', 'laminating',
    },
}
