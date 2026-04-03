"""Material name alias resolution for Layer 6 carbon footprint calculation.

Maps LLM-generated material names from Layer 1 to canonical reference
names in the base_materials database. The training dataset contains 284
unique material names, but only 67 match reference entries exactly. This
module provides resolution covering the remaining 217 names (148 with
no match at all, plus 69 that previously relied on fragile substring
matching).

Primary functions:
    resolve_material_name -- Resolve any material name to its canonical
        reference form via SYNONYM_MAP and prefix stripping.
    validate_aliases -- Verify all alias targets exist in the reference
        database.

Dependencies:
    _synonym_data module for the SYNONYM_MAP dictionary.
"""

import logging
import re
from typing import List, Set

from data.data_generation.layer_6.core._synonym_data import (
    SYNONYM_MAP
)

logger = logging.getLogger(__name__)

# Pre-compiled pattern for component prefixes
_PREFIX_RE = re.compile(
    r'^(?:canopy|frame|handle|ribs|fabric|sunbrella|'
    r'water-resistant|waterproof):\s*',
    re.IGNORECASE
)

# Pre-compiled pattern for parenthetical annotations
_PAREN_RE = re.compile(r'\s*\([^)]*\)\s*$')


def resolve_material_name(name: str) -> str:
    """Resolve a material name to its canonical reference form.

    Resolution strategy (in order):
    1. Direct lookup in SYNONYM_MAP.
    2. Strip component prefix (e.g., "canopy: fibre, cotton") and
       retry SYNONYM_MAP.
    3. Strip trailing parenthetical annotation and retry.
    4. Return original name unchanged (it may already be canonical).

    Args:
        name: Raw material name from the dataset.

    Returns:
        Canonical reference name, or the original if no alias found.
    """
    if not name:
        return name

    stripped = name.strip()

    # Direct lookup
    if stripped in SYNONYM_MAP:
        return SYNONYM_MAP[stripped]

    # Strip component prefix
    deprefixed = _PREFIX_RE.sub('', stripped)
    if deprefixed != stripped and deprefixed in SYNONYM_MAP:
        return SYNONYM_MAP[deprefixed]

    # Strip parenthetical annotation
    deparen = _PAREN_RE.sub('', stripped).strip()
    if deparen != stripped and deparen in SYNONYM_MAP:
        return SYNONYM_MAP[deparen]

    return stripped


def validate_aliases(ref_names: Set[str]) -> List[str]:
    """Validate that all alias targets exist in the reference set.

    Args:
        ref_names: Set of canonical material names from the reference
            database.

    Returns:
        List of error messages for invalid alias targets.
    """
    errors = []
    targets = set(SYNONYM_MAP.values())

    for target in sorted(targets):
        if target not in ref_names:
            errors.append(
                f"Alias target not in reference: {repr(target)}"
            )

    if errors:
        for err in errors:
            logger.error(err)
    else:
        logger.info(
            "All %d alias targets validated against %d references",
            len(targets), len(ref_names)
        )

    return errors
