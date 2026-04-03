"""
Material name corrector for Layer 1.

Maps common LLM-generated material name variants back to the exact names
in base_materials.parquet. This ensures Layer 1 output uses only valid
material names that Layer 2 can match against processing_steps.parquet
and material_processing_combinations.parquet.

Scientific accuracy notes (2026-03 review):
- Elastane/spandex/lycra are polyurethane-based fibres. No polyurethane fibre
  entry exists in the DB (only foams), so these MUST be flagged as uncorrectable
  rather than silently mapped to nylon 6-6 (a polyamide with ~72% higher CF).
- Acrylic (polyacrylonitrile) has no DB entry. Mapping to polyester is chemically
  wrong and introduces ~20-40% CF error. Flagged as uncorrectable.
- Cashmere (goat) and mohair (Angora goat) have drastically different emission
  profiles from sheep wool. No goat fibre entry exists in the DB, and sheep
  fleece (22.4 kgCO2e/kg) significantly underestimates cashmere (typically
  30-100+ kgCO2e/kg). Flagged as uncorrectable.
- Tencel/lyocell/modal -> cellulose fibre is scientifically correct (all are
  regenerated cellulose with similar emission profiles).
"""

import logging
import re
from typing import Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


# Materials that have no scientifically accurate proxy in base_materials.parquet.
# These are explicitly routed to the uncorrectable list instead of being silently
# mapped to a chemically different material.
#
# Elastane/spandex/lycra: polyurethane-based, DB only has PU foams (not fibres),
#   and the previous proxy (nylon 6-6, CF=9.5) overstates vs real elastane (~5-6).
# Acrylic: polyacrylonitrile, DB has no entry. Previous proxy (polyester, CF=6.98)
#   is chemically unrelated and introduces systematic bias.
# Cashmere/mohair: goat fibres with much higher CF than sheep wool. DB has no
#   goat fibre entry; using sheep fleece (CF=22.4) drastically underestimates.
UNCORRECTABLE_PROXY_MATERIALS: FrozenSet[str] = frozenset({
    # Elastane / Spandex / Lycra (polyurethane fibre -- no DB match)
    "fibre, elastane",
    "fibre, spandex",
    "fibre, Lycra",
    "fibre, lycra",
    "lycra",
    "spandex",
    "elastane",
    "fibre, elastane (assumed polyurethane)",
    "fibre, elastane (not listed, assumed from polyurethane)",
    "fibre, elastane (not listed, assumed polyurethane)",
    "fibre, elastane (not listed, assumed)",
    "fibre, elasthan (assumed nylon 6)",
    "fibre, elasthan (not listed, assumed nylon 6)",
    "fibre, elasthan (not listed, assumed similar to polyurethane)",
    "fibre, elasthan (polyurethane)",
    "fibre, recycled elastane",
    "elastic (nylon 6-6)",
    "elastane (not listed, assumed 2% for stretch)",
    "yarn, elastane",
    "yarn, elastane (not listed, assumed 1.50 kg CO2eq/kg)",
    "textile, elastane",
    "textile, spandex",
    # Acrylic (polyacrylonitrile -- no DB match)
    "fibre, acrylic",
    "acrylic",
    "yarn, acrylic",
    "textile, woven acrylic",
    # Cashmere (goat fibre -- no DB match; sheep fleece is not a valid proxy)
    "fibre, cashmere",
    "textile, cashmere",
    "yarn, cashmere",
    "yarn, cashmere (not listed, assumed similar to silk)",
    # Mohair (Angora goat fibre -- no DB match)
    "fibre, mohair",
})


# Deterministic mapping from common LLM variants to exact DB names.
# Built from empirical analysis of 296 mismatched names across 13,834 rows.
MATERIAL_NAME_CORRECTIONS: Dict[str, str] = {
    # --- Elastane / Spandex / Lycra ---
    # REMOVED: Previously mapped to nylon 6-6, but elastane is polyurethane-based.
    # No polyurethane fibre exists in the DB. These are now routed to
    # UNCORRECTABLE_PROXY_MATERIALS and will appear in the uncorrectable list.

    # --- Hemp variants ---
    "fibre, hemp": "cottonized fibre, hemp",
    "fibre, hemp (cottonized)": "cottonized fibre, hemp",
    "fibre, cottonized, hemp": "cottonized fibre, hemp",
    "hemp, cottonized fibre": "cottonized fibre, hemp",
    "hemp": "cottonized fibre, hemp",
    "hemp fibre": "cottonized fibre, hemp",
    "hemp fibre, raw": "hemp fibre, raw, at farm gate",

    # --- Organic cotton variants ---
    "fibre, organic cotton": "fibre, cotton, organic",
    "cotton, organic": "fibre, cotton, organic",
    "cotton, organic (system 2)": "fibre, cotton, organic",
    "cotton, organic, fibre": "fibre, cotton, organic",
    "organic cotton": "fibre, cotton, organic",

    # --- Cotton variants ---
    "cotton": "fibre, cotton",
    "cotton, fibre": "fibre, cotton",
    "fibre, cotton, recycled": "fibre, cotton",
    "fibre, recycled cotton": "fibre, cotton",
    "thread, cotton": "cotton string",

    # --- Nylon / Polyamide variants ---
    "fibre, nylon 6": "nylon 6",
    "fibre, nylon": "nylon 6",
    "nylon": "nylon 6",
    "polyamide": "nylon 6",
    "polyamide 6": "nylon 6",
    "fibre, polyamide": "nylon 6",
    "recycled nylon": "nylon 6",
    "fibre, recycled nylon": "nylon 6",
    "yarn, nylon": "nylon 6",
    "yarn, nylon 6": "nylon 6",
    "fibre, nylon 6-6": "nylon 6-6",
    "polyamide 6-6": "nylon 6-6",

    # --- Wool variants ---
    "fibre, wool, organic": "wool, organic, at farm gate",
    "fibre, wool, organic, at farm gate": "wool, organic, at farm gate",
    "fibre, organic, wool": "wool, organic, at farm gate",
    "fibre, organic wool": "wool, organic, at farm gate",
    "wool, organic": "wool, organic, at farm gate",
    "fibre, wool, conventional, at farm gate": "wool, conventional, at farm gate",
    "fibre, wool, conventional": "wool, conventional, at farm gate",
    "wool, conventional": "wool, conventional, at farm gate",
    "fibre, wool": "wool, conventional, at farm gate",
    "fibre, wool, organic (system 2), at farm gate": "wool, organic (system 2), at farm gate",
    "fibre, wool, organic (system 2)": "wool, organic (system 2), at farm gate",
    "fibre, organic, wool (system 2)": "wool, organic (system 2), at farm gate",
    "fibre, organic, wool, system 2": "wool, organic (system 2), at farm gate",
    "fibre, organic wool (system 2)": "wool, organic (system 2), at farm gate",
    "fibre, organic (system 2)": "wool, organic (system 2), at farm gate",
    "fibre, organic (system 2), at farm gate": "wool, organic (system 2), at farm gate",
    "fibre, wool, Roquefort dairy sheep, at farm gate": "wool, Roquefort dairy sheep, at farm gate",
    "fibre, wool, Roquefort dairy sheep": "wool, Roquefort dairy sheep, at farm gate",
    "fibre, merino wool": "sheep fleece in the grease",
    "merino wool": "sheep fleece in the grease",
    # REMOVED: "fibre, cashmere" and "fibre, mohair" were mapped to
    # "sheep fleece in the grease" but cashmere/mohair are goat fibres with
    # much higher emission factors. Now routed to UNCORRECTABLE_PROXY_MATERIALS.
    "fibre, organic, at farm gate": "wool, organic, at farm gate",
    "fibre, organic": "fibre, cotton, organic",
    # Typos: wood -> wool
    "wood, Roquefort dairy sheep": "wool, Roquefort dairy sheep, at farm gate",
    "wood, conventional, at farm gate": "wool, conventional, at farm gate",

    # --- Tencel / Lyocell / Modal -> cellulose fibre (regenerated cellulose) ---
    "fibre, Tencel": "cellulose fibre",
    "fibre, TENCEL": "cellulose fibre",
    "fibre, tencel": "cellulose fibre",
    "fibre, tencel (assumed viscose)": "cellulose fibre",
    "fibre, Tencel (lyocell)": "cellulose fibre",
    "Tencel": "cellulose fibre",
    "tencel": "cellulose fibre",
    "fibre, lyocell": "cellulose fibre",
    "fibre, lyocell (assumed viscose)": "cellulose fibre",
    "lyocell": "cellulose fibre",
    "fibre, modal": "cellulose fibre",
    "fibre, cellulose": "cellulose fibre",
    "acetate": "cellulose fibre",
    "fibre, acetate": "cellulose fibre",

    # --- Linen = Flax ---
    "fibre, linen": "fibre, flax",
    "fibre, organic linen": "fibre, flax",
    "linen": "fibre, flax",
    "flax, fibre": "fibre, flax",

    # --- Bamboo -> viscose (bamboo fibre is viscose-processed) ---
    "fibre, bamboo": "fibre, viscose",
    "fibre, bamboo viscose": "fibre, viscose",
    "bamboo fibre": "fibre, viscose",
    "bamboo viscose": "fibre, viscose",

    # --- Rayon = Viscose ---
    "fibre, rayon": "fibre, viscose",
    "rayon, viscose": "fibre, viscose",
    "viscose": "fibre, viscose",
    "viscose, fibre": "fibre, viscose",
    "fibre, elyocell (not listed, substituting with fibre, viscose)": "fibre, viscose",

    # --- Polyester variants ---
    "polyester": "fibre, polyester",
    "polyester fibre": "fibre, polyester",
    "polyester, fibre": "fibre, polyester",
    "fibre, recycled polyester": "fibre, polyester",
    "fibre, polyester, recycled": "fibre, polyester",
    "recycled polyester": "fibre, polyester",
    "polyester, recycled": "fibre, polyester",
    "polyester (not listed, assumed 20% for durability)": "fibre, polyester",
    "fibre, spandex (not listed, skipping, using fibre, polyester)": "fibre, polyester",
    # REMOVED: "fibre, acrylic" and "acrylic" were mapped to polyester, but
    # acrylic (polyacrylonitrile) is chemically distinct. Now in UNCORRECTABLE_PROXY_MATERIALS.
    "fibre, microfibre": "fibre, polyester",
    "microfibre": "fibre, polyester",
    "yarn, polyester": "fibre, polyester",
    # REMOVED: "yarn, acrylic" -> now in UNCORRECTABLE_PROXY_MATERIALS

    # --- PET granulate variants ---
    "polyethylene terephthalate, granulate": "polyethylene terephthalate, granulate, amorphous",
    "polyethylene terephthalate": "polyethylene terephthalate, granulate, amorphous",
    "polyester, granulate": "polyethylene terephthalate, granulate, amorphous",
    "polyester, granulate, amorphous": "polyethylene terephthalate, granulate, amorphous",
    "polyester, granulate, bottle grade": "polyethylene terephthalate, granulate, bottle grade",
    "polycarbonate": "polyethylene terephthalate, granulate, amorphous",
    "fibre, polycarbonate": "polyethylene terephthalate, granulate, amorphous",

    # --- Silk variants ---
    "fibre, silk": "fibre, silk, short",
    "silk": "fibre, silk, short",
    "silk fibre, short": "fibre, silk, short",
    "silk, short": "fibre, silk, short",
    "silk fabric": "textile, silk",
    "silk, textile": "textile, silk",
    "silk yarn": "yarn, silk",
    "fibre, chiffon": "textile, silk",

    # --- Leather / Hide variants ---
    "leather, cowhide, from beef, at slaughterhouse": "cowhide, from beef, at slaughterhouse",
    "leather, cowhide": "cowhide, from beef, at slaughterhouse",
    "leather": "cowhide, from beef, at slaughterhouse",
    "leather strap": "cowhide, from beef, at slaughterhouse",
    "leather (not listed, assumed similar to textile, woven cotton)": "cowhide, from beef, at slaughterhouse",
    "leather (not listed, assumed similar to wool for CO2eq)": "cowhide, from beef, at slaughterhouse",
    "leather (not listed, assumed similar to wool for strap)": "cowhide, from beef, at slaughterhouse",
    "leather, beef hides, at slaughterhouse (GB)": "beef hides, at slaughterhouse (GB)",
    "leather, beef hides": "beef hides, at slaughterhouse (GB)",
    "leather, lamb hide, at slaughterhouse": "lamb hide, at slaughterhouse",
    "leather, lamb hide": "lamb hide, at slaughterhouse",
    "leather, veal hide, at slaughterhouse": "veal hide, at slaughterhouse",
    "leather, veal hide": "veal hide, at slaughterhouse",
    "leather, sheep fleece in the grease": "sheep fleece in the grease",
    "leather, wool, conventional, at farm gate": "wool, conventional, at farm gate",

    # --- Down / Feather variants ---
    "down feathers (duck)": "duck feathers, at slaughterhouse",
    "down feathers (chicken)": "chicken feathers, at slaughterhouse",
    "down feathers (goose)": "duck feathers (fattened), at slaughterhouse",
    "down feathers (not listed, assuming chicken feathers)": "chicken feathers, at slaughterhouse",
    "down insulation (assumed from chicken feathers)": "chicken feathers, at slaughterhouse",
    "down insulation (assumed from duck feathers)": "duck feathers, at slaughterhouse",
    "down insulation (assumed from duck feathers, fattened)": "duck feathers (fattened), at slaughterhouse",
    "down insulation (chicken feathers, at slaughterhouse)": "chicken feathers, at slaughterhouse",
    "down insulation (duck feathers, at slaughterhouse)": "duck feathers, at slaughterhouse",
    "down insulation (implicit in subcategory)": "duck feathers, at slaughterhouse",
    "down insulation (not listed, assumed from chicken feathers)": "chicken feathers, at slaughterhouse",
    "down insulation (not listed, assumed)": "chicken feathers, at slaughterhouse",
    "down, chicken feathers, at slaughterhouse": "chicken feathers, at slaughterhouse",
    "down, duck feathers": "duck feathers, at slaughterhouse",
    "down, duck feathers, at slaughterhouse": "duck feathers, at slaughterhouse",
    "duck feathers": "duck feathers, at slaughterhouse",
    "chicken feathers": "chicken feathers, at slaughterhouse",

    # --- Stainless steel variants ---
    "stainless steel, chromium steel 18/8": "steel, chromium steel 18/8",
    "stainless steel, 18/8": "steel, chromium steel 18/8",
    "stainless steel": "steel, chromium steel 18/8",
    "stainless steel, low-alloyed": "steel, low-alloyed",

    # --- Rubber variants ---
    "natural rubber": "latex",
    "natural rubber based": "seal, natural rubber based",
    "rubber": "synthetic rubber",
    "rubber, synthetic": "synthetic rubber",

    # --- Foam variants ---
    "flexible polyurethane foam": "polyurethane, flexible foam",
    "polyurethane, flame retardant": "polyurethane, flexible foam, flame retardant",
    "plastic, polyurethane": "polyurethane, flexible foam",

    # --- Polymer granulate variants ---
    "polylactic acid": "polylactic acid, granulate",
    "polypropylene": "polypropylene, granulate",

    # --- Coconut variants ---
    "fibre, coconut": "coconut fibre, at storehouse",
    "fibre, coconut fibre": "coconut fibre, at storehouse",
    "fibre, coconut fibre, at storehouse": "coconut fibre, at storehouse",
    "fibre, coconut, at storehouse": "coconut fibre, at storehouse",
    "coconut, fibre": "coconut fibre, at storehouse",

    # --- Textile variants ---
    "textile, organic cotton": "textile, knit cotton",
    "textile, organic cotton twill": "textile, woven cotton",
    "textile, organic denim": "textile, woven cotton",
    "textile, cotton": "textile, knit cotton",
    "textile, cotton blend": "textile, knit cotton",
    "textile, cotton sateen": "textile, woven cotton",
    "textile, cotton twill": "textile, woven cotton",
    "textile, cotton voile": "textile, woven cotton",
    "textile, cotton, knit": "textile, knit cotton",
    "textile, cotton, organic": "textile, knit cotton",
    "textile, knitted cotton": "textile, knit cotton",
    "textile, knit blend": "textile, knit cotton",
    "textile, denim": "textile, woven cotton",
    "textile, denim cotton": "textile, woven cotton",
    "textile, indigo denim": "textile, woven cotton",
    "textile, lightweight denim": "textile, woven cotton",
    "textile, stretch denim": "textile, woven cotton",
    "textile, vintage denim": "textile, woven cotton",
    "textile, recycled denim": "textile, woven cotton",
    "textile, recycled cotton": "textile, knit cotton",
    "textile, velvet": "textile, woven cotton",
    "textile, tweed": "textile, woven cotton",
    "textile, lace": "textile, woven cotton",
    "textile, oxford cloth": "textile, woven cotton",
    "textile, chambray": "textile, woven cotton",
    "textile, recycled polyester": "textile, nonwoven polyester",
    "textile, polyester": "textile, nonwoven polyester",
    "textile, knit polyester": "textile, nonwoven polyester",
    "textile, nylon": "textile, nonwoven polyester",
    "textile, knit nylon": "textile, nonwoven polyester",
    "textile, viscose": "textile, nonwoven polyester",
    "textile, rayon": "textile, nonwoven polyester",
    "textile, rayon challis": "textile, nonwoven polyester",
    "textile, lyocell": "textile, nonwoven polyester",
    "textile, Tencel": "textile, nonwoven polyester",
    "textile, bamboo": "textile, nonwoven polyester",
    # REMOVED: "textile, woven acrylic" -> now in UNCORRECTABLE_PROXY_MATERIALS
    "textile, hemp": "textile, jute",
    "textile, hemp blend": "textile, jute",
    "textile, flax": "textile, jute",
    "textile, woven coconut fibre": "textile, jute",
    "textile, woven wool": "textile, kenaf",
    "textile, linen": "textile, kenaf",
    "textile, linen blend": "textile, kenaf",
    "textile, organic linen": "textile, kenaf",
    "textile, silk blend": "textile, silk",
    "textile, silk habotai": "textile, silk",
    # REMOVED: "textile, cashmere" -> now in UNCORRECTABLE_PROXY_MATERIALS
    "textile, merino wool": "textile, silk",
    "textile, chiffon": "textile, silk",
    "textile, brocade": "textile, silk",

    # --- Yarn variants ---
    "yarn, organic cotton": "yarn, cotton",
    "yarn, wool": "yarn, silk",
    "yarn, wool, conventional": "yarn, silk",
    "yarn, wool, conventional, at farm gate": "yarn, silk",
    "yarn, merino wool": "yarn, silk",
    # REMOVED: "yarn, cashmere" variants -> now in UNCORRECTABLE_PROXY_MATERIALS
    "yarn, flax": "yarn, jute",
    "yarn, linen": "yarn, jute",
    "jute, yarn": "yarn, jute",
    "kenaf, yarn": "yarn, kenaf",

    # --- Miscellaneous ---
    "fibre, metallic": "steel, low-alloyed",
    "fibre, TR-90": "nylon 6",
    "fibre, corn": "polylactic acid, granulate",
    "fibre, milk": "cellulose fibre",
    "fibre, soy": "cellulose fibre",
    "recycled aluminium": "aluminium, wrought alloy",
    "silicone": "synthetic rubber",
    "titanium": "steel, chromium steel 18/8",
    "titanium alloy": "steel, chromium steel 18/8",
    "ylene vinyl acetate copolymer": "ethylene vinyl acetate copolymer",
}

# Regex pattern for "prefix: material_name" format (e.g., "canopy: fibre, cotton")
_PREFIX_PATTERN = re.compile(
    r'^(?:canopy|frame|handle|ribs|sunbrella|fabric|water-resistant|waterproof):\s*(.+)$'
)


def correct_material_name(
    raw_name: str,
    valid_names: Set[str],
) -> Tuple[str, bool]:
    """
    Correct a material name to match the reference database.

    Returns (corrected_name, was_corrected).
    If the name is already valid, returns it unchanged.
    If the name is in UNCORRECTABLE_PROXY_MATERIALS, returns it unchanged
    with was_corrected=False so it lands in the uncorrectable list.
    If the name cannot be corrected, returns the original name unchanged
    with was_corrected=False.
    """
    stripped = raw_name.strip()

    # Already valid
    if stripped in valid_names:
        return stripped, False

    # Explicitly uncorrectable: materials with no scientifically accurate
    # proxy in the DB. Return unchanged so they appear in uncorrectable output.
    if stripped in UNCORRECTABLE_PROXY_MATERIALS:
        logger.warning(
            "Material '%s' has no scientifically accurate proxy in the "
            "reference database and will be flagged as uncorrectable.",
            stripped,
        )
        return stripped, False

    # Check direct correction map
    if stripped in MATERIAL_NAME_CORRECTIONS:
        corrected = MATERIAL_NAME_CORRECTIONS[stripped]
        return corrected, True

    # Check for "prefix: material" pattern -- strip the prefix
    prefix_match = _PREFIX_PATTERN.match(stripped)
    if prefix_match:
        inner = prefix_match.group(1).strip()
        if inner in valid_names:
            return inner, True
        if inner in UNCORRECTABLE_PROXY_MATERIALS:
            logger.warning(
                "Material '%s' (from prefix pattern '%s') has no "
                "scientifically accurate proxy in the reference database.",
                inner, stripped,
            )
            return stripped, False
        if inner in MATERIAL_NAME_CORRECTIONS:
            return MATERIAL_NAME_CORRECTIONS[inner], True

    return stripped, False


def correct_material_list(
    materials: List[str],
    valid_names: Set[str],
) -> Tuple[List[str], List[str], List[str]]:
    """
    Correct a list of material names.

    Returns:
        (corrected_list, corrections_log, uncorrectable)
        - corrected_list: all names after correction
        - corrections_log: human-readable log of what was changed
        - uncorrectable: names that could not be mapped
    """
    corrected = []
    log = []
    uncorrectable = []

    for name in materials:
        fixed, was_corrected = correct_material_name(name, valid_names)
        corrected.append(fixed)
        if was_corrected:
            log.append(f"'{name}' -> '{fixed}'")
        elif fixed not in valid_names:
            uncorrectable.append(name)

    return corrected, log, uncorrectable
