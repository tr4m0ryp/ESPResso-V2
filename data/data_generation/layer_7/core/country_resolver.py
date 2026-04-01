"""AWARE country extraction and matching for Layer 7.

Implements D6 country matching logic: extracts the origin country
from transport_legs location strings, resolves known aliases, and
looks up the AWARE characterization factor.

Multi-part locations (e.g., "Shanghai, China") take the last
comma-separated part. A static alias map handles known mismatches
between data sources and AWARE country names.

Primary functions:
    load_country_aliases -- Load aliases from CSV file.
    extract_country -- Extract country from location string.
    extract_material_origins -- Get origin per material name.
    extract_step_locations -- Get factory location per (material, step).

Dependencies:
    json for parsing transport_legs JSON strings.
"""

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Static alias dict for 10 known mismatches (D6 spec)
COUNTRY_ALIASES: Dict[str, str] = {
    'Turkey': 'Turkiye',
    'USA': 'United States of America',
    'UK': 'United Kingdom',
    'England': 'United Kingdom',
    'Scotland': 'United Kingdom',
    'Czech Republic': 'Czechia',
    'Kitwe': 'Zambia',
    'Lusaka': 'Zambia',
    'Tashkent': 'Uzbekistan',
    'Fergana Valley': 'Uzbekistan',
}


def load_country_aliases(aliases_path: str) -> Dict[str, str]:
    """Load country aliases from CSV file and merge with static map.

    The CSV has columns: alias, canonical_name. These are merged
    with the built-in COUNTRY_ALIASES dict.

    Args:
        aliases_path: Path to aware_country_aliases.csv.

    Returns:
        Merged alias dictionary.
    """
    import csv
    from pathlib import Path

    aliases = dict(COUNTRY_ALIASES)

    if not Path(aliases_path).exists():
        logger.warning(
            "Country aliases file not found: %s. "
            "Using built-in aliases only.",
            aliases_path
        )
        return aliases

    try:
        with open(aliases_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                alias = row.get('alias', '').strip()
                canonical = row.get('canonical_name', '').strip()
                if alias and canonical:
                    aliases[alias] = canonical
        logger.info(
            "Loaded %d country aliases (including built-in)",
            len(aliases)
        )
    except Exception as e:
        logger.warning(
            "Failed to load country aliases: %s. "
            "Using built-in aliases only.",
            e
        )

    return aliases


def extract_country(
    location_string: str,
    aliases: Optional[Dict[str, str]] = None,
) -> str:
    """Extract country from a location string.

    For multi-part locations (comma-separated), takes the last part.
    Applies alias resolution for known mismatches.

    Args:
        location_string: Raw location (e.g., "Shanghai, China").
        aliases: Country alias dictionary. Uses COUNTRY_ALIASES if None.

    Returns:
        Resolved country name.
    """
    if not location_string or not isinstance(location_string, str):
        return ''

    if aliases is None:
        aliases = COUNTRY_ALIASES

    parts = [p.strip() for p in location_string.split(',')]
    country = parts[-1]
    return aliases.get(country, country)


def _parse_transport_legs(
    transport_legs_raw: Any,
) -> List[Dict[str, Any]]:
    """Parse transport_legs from JSON string or list.

    Args:
        transport_legs_raw: Raw transport_legs value.

    Returns:
        List of leg dictionaries.
    """
    if isinstance(transport_legs_raw, list):
        return transport_legs_raw

    if not transport_legs_raw:
        return []

    try:
        parsed = json.loads(str(transport_legs_raw))
        if isinstance(parsed, list):
            return parsed
    except (json.JSONDecodeError, TypeError):
        pass

    return []


def extract_material_origins(
    transport_legs_raw: Any,
    materials: List[str],
    aliases: Optional[Dict[str, str]] = None,
) -> Dict[str, dict]:
    """Extract origin country/coords per material from transport_legs.

    Groups legs by their 'material' field, then takes the FIRST
    leg's from_location as that material's origin.

    Args:
        transport_legs_raw: Raw transport_legs (JSON string or list).
        materials: List of material names (for fallback ordering).
        aliases: Country alias dictionary.

    Returns:
        Dict mapping material name to
        {"country": str, "lat": float, "lon": float}.
    """
    legs = _parse_transport_legs(transport_legs_raw)
    if aliases is None:
        aliases = COUNTRY_ALIASES

    origins: Dict[str, dict] = {}

    # Group by material, keep insertion order (first leg wins)
    for leg in legs:
        mat = leg.get('material', '')
        if not mat or mat in origins:
            continue
        from_loc = leg.get('from_location', '')
        country = extract_country(from_loc, aliases)
        origins[mat] = {
            'country': country,
            'lat': leg.get('from_lat', 0.0),
            'lon': leg.get('from_lon', 0.0),
        }

    return origins


def extract_step_locations(
    transport_legs_raw: Any,
    aliases: Optional[Dict[str, str]] = None,
) -> Dict[Tuple[str, str], dict]:
    """Extract factory location per (material, step) from transport_legs.

    For each unique (material, from_step) pair, takes the first
    occurrence's from_location as the factory location for that step.

    Args:
        transport_legs_raw: Raw transport_legs (JSON string or list).
        aliases: Country alias dictionary.

    Returns:
        Dict mapping (material_name, step_name) to
        {"country": str, "lat": float, "lon": float}.
    """
    legs = _parse_transport_legs(transport_legs_raw)
    if aliases is None:
        aliases = COUNTRY_ALIASES

    locations: Dict[Tuple[str, str], dict] = {}

    for leg in legs:
        mat = leg.get('material', '')
        step = leg.get('from_step', '')
        key = (mat, step)
        if not mat or not step or key in locations:
            continue
        from_loc = leg.get('from_location', '')
        country = extract_country(from_loc, aliases)
        locations[key] = {
            'country': country,
            'lat': leg.get('from_lat', 0.0),
            'lon': leg.get('from_lon', 0.0),
        }

    return locations
