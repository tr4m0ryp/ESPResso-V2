"""
EcoInvent Derby DB dump parsers and water consumption search logic.

Parses pre-dumped ij output files for fast in-memory matching.
Only processes with Water elementary flow data are considered.
"""

import re
import subprocess
from collections import defaultdict
from pathlib import Path

DUMP_DIR = Path("/tmp")
DERBY_CP = ":".join([
    "/tmp/derby/db-derby-10.17.1.0-lib/lib/derby.jar",
    "/tmp/derby/db-derby-10.17.1.0-lib/lib/derbyshared.jar",
    "/tmp/derby/db-derby-10.17.1.0-lib/lib/derbytools.jar",
])
DB_URL = "jdbc:derby:/tmp/ecoinvent_db/ecoinvent_db"


def ensure_dumps():
    """Create Derby dump files if missing."""
    dumps = {
        "ecoinvent_processes_dump.txt": """
SELECT p.ID, p.NAME, l.NAME AS LOCATION, p.F_QUANTITATIVE_REFERENCE
FROM TBL_PROCESSES p
LEFT JOIN TBL_LOCATIONS l ON p.F_LOCATION = l.ID ORDER BY p.ID;""",
        "ecoinvent_water_exchanges.txt": """
SELECT e.F_OWNER, e.IS_INPUT, e.RESULTING_AMOUNT_VALUE, u.NAME AS UNIT,
       e.INTERNAL_ID FROM TBL_EXCHANGES e
JOIN TBL_FLOWS f ON e.F_FLOW = f.ID
LEFT JOIN TBL_UNITS u ON e.F_UNIT = u.ID
WHERE f.FLOW_TYPE = 'ELEMENTARY_FLOW' AND f.NAME = 'Water'
ORDER BY e.F_OWNER;""",
        "ecoinvent_ref_flows.txt": """
SELECT p.ID AS PID, e.RESULTING_AMOUNT_VALUE AS REF, u.NAME AS UNIT
FROM TBL_PROCESSES p
JOIN TBL_EXCHANGES e ON e.F_OWNER = p.ID AND e.ID = p.F_QUANTITATIVE_REFERENCE
LEFT JOIN TBL_UNITS u ON e.F_UNIT = u.ID ORDER BY p.ID;""",
    }
    for fname, sql in dumps.items():
        path = DUMP_DIR / fname
        if path.exists() and path.stat().st_size > 1000:
            continue
        print(f"  Dumping {fname}...")
        full = f"connect '{DB_URL}';\n{sql}\nexit;\n"
        r = subprocess.run(
            ["java", "-cp", DERBY_CP, "org.apache.derby.tools.ij"],
            input=full, capture_output=True, text=True, timeout=120)
        path.write_text(r.stdout)


def parse_processes(path: Path) -> list[dict]:
    """Parse fixed-width ij process dump."""
    results = []
    col_positions = []
    with open(path) as f:
        in_data = False
        for line in f:
            s = line.rstrip()
            if "|" in s and "ID" in s and "NAME" in s:
                idx = s.find("ID")
                header = s[idx:]
                col_positions = [0]
                for i, c in enumerate(header):
                    if c == "|":
                        col_positions.append(i + 1)
                continue
            if s.startswith("---"):
                in_data = True
                continue
            if not in_data:
                continue
            if "row" in s.lower() and "selected" in s.lower():
                break
            if len(col_positions) < 4:
                continue
            try:
                pid = int(s[col_positions[0]:col_positions[1]-1].strip())
                name = s[col_positions[1]:col_positions[2]-1].strip()
                loc = s[col_positions[2]:col_positions[3]-1].strip()
                ref = int(s[col_positions[3]:].strip())
            except (ValueError, IndexError):
                continue
            results.append({"id": pid, "name": name,
                            "location": loc, "ref_exchange": ref})
    return results


def parse_water_exchanges(path: Path) -> dict[int, list[dict]]:
    """Parse water exchanges grouped by process (F_OWNER)."""
    by_owner = defaultdict(list)
    with open(path) as f:
        in_data = False
        for line in f:
            s = line.rstrip()
            if s.startswith("---"):
                in_data = True
                continue
            if not in_data:
                continue
            if "row" in s.lower() and "selected" in s.lower():
                break
            parts = s.split("|")
            if len(parts) >= 5:
                try:
                    owner = int(parts[0].strip())
                    is_input = int(parts[1].strip())
                    amount = float(parts[2].strip())
                    by_owner[owner].append(
                        {"is_input": is_input, "amount": amount})
                except (ValueError, IndexError):
                    continue
    return dict(by_owner)


def parse_ref_flows(path: Path) -> dict[int, float]:
    """Parse reference flows: process_id -> amount."""
    refs = {}
    with open(path) as f:
        in_data = False
        for line in f:
            s = line.rstrip()
            if s.startswith("---"):
                in_data = True
                continue
            if not in_data:
                continue
            if "row" in s.lower() and "selected" in s.lower():
                break
            parts = s.split("|")
            if len(parts) >= 3:
                try:
                    refs[int(parts[0].strip())] = float(parts[1].strip())
                except (ValueError, IndexError):
                    continue
    return refs


def like_to_regex(pattern: str) -> re.Pattern | None:
    """Convert SQL LIKE pattern to compiled regex."""
    placeholder = "\x00"
    safe = pattern.lower().replace("%", placeholder)
    escaped = re.escape(safe)
    regex_str = "^" + escaped.replace(
        re.escape(placeholder), ".*") + "$"
    try:
        return re.compile(regex_str, re.DOTALL)
    except re.error:
        return None


def compute_net_water(
    pid: int, water_map: dict, ref_map: dict,
) -> float | None:
    """Net water consumption in m3, normalized by reference flow."""
    exch = water_map.get(pid)
    if not exch:
        return None
    out = sum(e["amount"] for e in exch if e["is_input"] == 0)
    inp = sum(e["amount"] for e in exch if e["is_input"] == 1)
    net = out - inp
    ref = ref_map.get(pid, 1.0)
    if ref != 0:
        net /= ref
    if net < 0:
        net = abs(net)
    return net if net > 0 else None


def find_best_match(
    patterns: list[str],
    procs_with_water: list[dict],
    water_map: dict,
    ref_map: dict,
) -> dict | None:
    """Find best matching process with valid water data."""
    for pattern in patterns:
        regex = like_to_regex(pattern)
        if not regex:
            continue
        matches = [p for p in procs_with_water
                   if regex.match(p["name"].lower())]
        if not matches:
            continue
        matches.sort(key=lambda p: (
            p["location"].lower() not in (
                "global", "glo", "rest of world"),
            p["name"].lower(),
        ))
        for proc in matches[:10]:
            water = compute_net_water(
                proc["id"], water_map, ref_map)
            if water is not None and water > 0:
                return {
                    "process_id": proc["id"],
                    "process_name": proc["name"],
                    "location": proc["location"],
                    "water_m3_per_kg": round(water, 6),
                }
    return None


def load_ecoinvent_data():
    """Load and return all EcoInvent data from dumps."""
    ensure_dumps()
    all_procs = parse_processes(
        DUMP_DIR / "ecoinvent_processes_dump.txt")
    water_map = parse_water_exchanges(
        DUMP_DIR / "ecoinvent_water_exchanges.txt")
    ref_map = parse_ref_flows(
        DUMP_DIR / "ecoinvent_ref_flows.txt")
    water_procs = [p for p in all_procs if p["id"] in water_map]
    return all_procs, water_procs, water_map, ref_map
