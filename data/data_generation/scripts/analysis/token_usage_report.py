#!/usr/bin/env python3
"""CLI tool for viewing accumulated token usage across pipeline layers.

Usage:
    python scripts/token_usage_report.py              # Summary
    python scripts/token_usage_report.py --per-layer   # Per-layer breakdown
    python scripts/token_usage_report.py --sessions    # Session history
    python scripts/token_usage_report.py --json        # Raw JSON
"""

import argparse
import json
import sys
from pathlib import Path

# Resolve token_usage.json relative to this script
_SCRIPT_DIR = Path(__file__).resolve().parent
_USAGE_PATH = _SCRIPT_DIR.parent / "token_usage.json"


def _load(path: Path) -> dict:
    if not path.exists():
        print(f"No token usage file found at {path}", file=sys.stderr)
        sys.exit(1)
    return json.loads(path.read_text())


def _fmt(n: int) -> str:
    """Format an integer with thousands separators."""
    return f"{n:,}"


def print_summary(data: dict) -> None:
    at = data.get("all_time", {})
    print("-- All-time token usage --")
    print(f"  API calls:         {_fmt(at.get('api_calls', 0))}")
    print(f"  Prompt tokens:     {_fmt(at.get('prompt_tokens', 0))}")
    print(f"  Completion tokens: {_fmt(at.get('completion_tokens', 0))}")
    print(f"  Total tokens:      {_fmt(at.get('total_tokens', 0))}")


def print_per_layer(data: dict) -> None:
    layers = data.get("per_layer", {})
    if not layers:
        print("No per-layer data recorded yet.")
        return
    header = f"{'Layer':<12} {'Calls':>10} {'Prompt':>14} {'Completion':>14} {'Total':>14}"
    print(header)
    print("-" * len(header))
    for name in sorted(layers):
        c = layers[name]
        print(
            f"{name:<12} "
            f"{_fmt(c.get('api_calls', 0)):>10} "
            f"{_fmt(c.get('prompt_tokens', 0)):>14} "
            f"{_fmt(c.get('completion_tokens', 0)):>14} "
            f"{_fmt(c.get('total_tokens', 0)):>14}"
        )


def print_sessions(data: dict) -> None:
    sessions = data.get("sessions", [])
    if not sessions:
        print("No sessions recorded yet.")
        return
    header = (
        f"{'Session':<30} {'Layer':<10} {'Started':<20} "
        f"{'Calls':>8} {'Total tokens':>14}"
    )
    print(header)
    print("-" * len(header))
    for s in sessions:
        print(
            f"{s.get('session_id', '?'):<30} "
            f"{s.get('layer', '?'):<10} "
            f"{s.get('started_at', '?'):<20} "
            f"{_fmt(s.get('api_calls', 0)):>8} "
            f"{_fmt(s.get('total_tokens', 0)):>14}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Token usage report")
    parser.add_argument("--per-layer", action="store_true",
                        help="Show per-layer breakdown")
    parser.add_argument("--sessions", action="store_true",
                        help="Show session history")
    parser.add_argument("--json", action="store_true",
                        help="Print raw JSON")
    parser.add_argument("--file", type=Path, default=_USAGE_PATH,
                        help="Path to token_usage.json")
    args = parser.parse_args()

    data = _load(args.file)

    if args.json:
        print(json.dumps(data, indent=2))
        return

    print_summary(data)

    if args.per_layer:
        print()
        print_per_layer(data)

    if args.sessions:
        print()
        print_sessions(data)

    if not args.per_layer and not args.sessions:
        print()
        print_per_layer(data)


if __name__ == "__main__":
    main()
