#!/usr/bin/env python3
"""Run Layer 6 enrichment via Gemini CLI (Pro-account rate limits).

Spawns parallel `gemini -p` processes to extract transport distances.
Uses the optimized prompt (single-mode legs pre-computed).

Prerequisites:
    npm install -g @google/gemini-cli
    gemini          # run once interactively to authenticate + verify

Usage:
    python3 -m data.data_generation.scripts.run_layer6_gemini_cli [options]

Options:
    --workers N, -w N       Parallel workers (default: 3)
    --batch-size N, -b N    Records per LLM call (default: 100)
    --save-interval N       Save checkpoint every N batches (default: 50)
"""

import argparse
import json
import logging
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

project_root = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from data.data_generation.layer_6.enrichment.data_joiner import (
    join_transport_legs,
)
from data.data_generation.layer_6.enrichment.prompt_builder import (
    build_batch_prompt, get_system_prompt,
)
from data.data_generation.layer_6.enrichment.validator import (
    validate_extraction,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

TOLERANCE = 0.02
CKPT_DIR = 'data/datasets/pre-model/generated/layer_6/checkpoints/enrichment'
LAYER5 = (
    'data/datasets/pre-model/generated/'
    'layer_5/layer_5_validated_dataset.csv'
)
LAYER4 = (
    'data/datasets/pre-model/generated/'
    'layer_4/layer_4_complete_dataset.parquet'
)
MODE_COLS = ['road_km', 'sea_km', 'rail_km', 'air_km', 'inland_waterway_km']

stats_lock = threading.Lock()
stats = {'passed': 0, 'failed': 0, 'errors': 0, 'completed': 0}
results_lock = threading.Lock()
all_results = []


def _inc(key, n=1):
    with stats_lock:
        stats[key] += n


def _extract_json_array(text):
    """Parse a JSON array from possibly noisy LLM text output."""
    if not text:
        return None
    text = re.sub(r"<think(?:ing)?>[\s\S]*?</think(?:ing)?>", "", text)
    fence = re.search(r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", text)
    if fence:
        try:
            return json.loads(fence.group(1))
        except (json.JSONDecodeError, ValueError):
            pass
    s = text.find('[')
    e = text.rfind(']')
    if s >= 0 and e > s:
        try:
            return json.loads(text[s:e + 1])
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def process_batch(batch_records, batch_idx, total):
    """Run one batch through gemini CLI."""
    system = get_system_prompt()
    user = build_batch_prompt(batch_records)
    prompt = (
        "CRITICAL: Output ONLY a raw JSON array. "
        "No text, no explanation, no code blocks, no markdown. "
        "Just [ ... ].\n\n" + system + '\n\n' + user
    )

    try:
        proc = subprocess.run(
            ['gemini', '-p', '', '-o', 'text'],
            input=prompt,
            capture_output=True,
            text=True,
            timeout=600,
        )
        if proc.returncode != 0:
            stderr = proc.stderr[:300] if proc.stderr else '(no stderr)'
            logger.warning(
                "Batch %d/%d: exit %d: %s",
                batch_idx, total, proc.returncode, stderr,
            )
            _inc('errors')
            return

        result_text = proc.stdout
        extractions = _extract_json_array(result_text)
        if not extractions:
            logger.warning(
                "Batch %d/%d: no JSON array in output (len=%d)",
                batch_idx, total, len(result_text),
            )
            _inc('errors')
            return

        extraction_map = {str(ex.get('id', '')): ex for ex in extractions}
        batch_results = []

        for rec in batch_records:
            rid = str(rec.get('record_id', ''))
            ext = extraction_map.get(rid)
            if not ext:
                _inc('failed')
                continue

            total_km = float(rec.get('total_distance_km', 0))
            vr = validate_extraction(ext, total_km, TOLERANCE)

            row = {'record_id': rid, 'is_valid': vr.is_valid}
            for m in MODE_COLS:
                row[m] = ext.get(m, 0.0)
            batch_results.append(row)

            _inc('passed' if vr.is_valid else 'failed')

        with results_lock:
            all_results.extend(batch_results)

    except subprocess.TimeoutExpired:
        logger.warning("Batch %d/%d: timed out (600s)", batch_idx, total)
        _inc('errors')
    except Exception as exc:
        logger.warning("Batch %d/%d: %s", batch_idx, total, exc)
        _inc('errors')

    _inc('completed')
    with stats_lock:
        c = stats['completed']
    if c % 10 == 0:
        logger.info(
            "Progress: %d/%d batches | %d passed, %d failed, %d errors",
            c, total, stats['passed'], stats['failed'], stats['errors'],
        )


def save_checkpoint(label=""):
    """Write current results to a checkpoint CSV."""
    with results_lock:
        if not all_results:
            return
        snapshot = list(all_results)

    out_df = pd.DataFrame(snapshot)
    ckpt_path = Path(CKPT_DIR)
    ckpt_path.mkdir(parents=True, exist_ok=True)
    existing = sorted(ckpt_path.glob('enrichment_batch_*.csv'))
    next_num = len(existing) + 1
    out_file = ckpt_path / f'enrichment_batch_{next_num:04d}.csv'
    out_df.to_csv(out_file, index=False)
    logger.info(
        "Checkpoint%s: %s (%d rows, %d valid)",
        f" ({label})" if label else "",
        out_file, len(out_df),
        out_df['is_valid'].sum(),
    )


def main():
    parser = argparse.ArgumentParser(
        description='Layer 6 enrichment via Gemini CLI'
    )
    parser.add_argument('--workers', '-w', type=int, default=3)
    parser.add_argument('--batch-size', '-b', type=int, default=100)
    parser.add_argument('--save-interval', type=int, default=50,
                        help='Save checkpoint every N batches')
    parser.add_argument('--skip-preflight', action='store_true')
    args = parser.parse_args()

    if not args.skip_preflight:
        logger.info("Preflight: testing gemini CLI...")
        try:
            proc = subprocess.run(
                ['gemini', '-p', 'Reply with exactly: OK', '-o', 'text'],
                capture_output=True, text=True, timeout=30,
            )
            if proc.returncode != 0 or 'OK' not in proc.stdout:
                logger.error("Preflight failed: %s", proc.stderr[:300])
                return 1
            logger.info("Preflight OK")
        except Exception as exc:
            logger.error("Preflight failed: %s", exc)
            return 1

    logger.info("Loading data...")
    df = join_transport_legs(LAYER5, LAYER4)

    completed_ids = set()
    ckpt_path = Path(CKPT_DIR)
    if ckpt_path.exists():
        for f in sorted(ckpt_path.glob('*.csv')):
            cdf = pd.read_csv(f)
            valid_ids = set(
                cdf[cdf['is_valid'] == True]['record_id'].astype(str)
            )
            completed_ids.update(valid_ids)
    logger.info("Already valid: %d / %d", len(completed_ids), len(df))

    remaining = df[~df['record_id'].astype(str).isin(completed_ids)]
    bs = args.batch_size
    num_batches = (len(remaining) + bs - 1) // bs
    logger.info(
        "Remaining: %d records in %d batches (size %d)",
        len(remaining), num_batches, bs,
    )
    logger.info("Estimated requests: %d (budget: ~1000/day)", num_batches)

    if len(remaining) == 0:
        logger.info("Nothing to do -- all records already valid")
        return 0

    records = remaining.to_dict('records')
    batches = [records[i:i + bs] for i in range(0, len(records), bs)]

    logger.info(
        "Launching %d workers for %d batches", args.workers, len(batches)
    )
    start = time.time()

    completed_counter = {'n': 0}
    counter_lock = threading.Lock()

    def on_batch_done(future):
        try:
            future.result()
        except Exception:
            pass
        with counter_lock:
            completed_counter['n'] += 1
            if (completed_counter['n'] % args.save_interval == 0
                    and completed_counter['n'] < len(batches)):
                save_checkpoint(
                    label=f"batch {completed_counter['n']}/{len(batches)}"
                )

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = []
        for idx, batch in enumerate(batches, 1):
            f = executor.submit(process_batch, batch, idx, len(batches))
            f.add_done_callback(on_batch_done)
            futures.append(f)
        for f in futures:
            try:
                f.result()
            except Exception:
                pass

    elapsed = time.time() - start
    logger.info(
        "Done in %.1fs: %d passed, %d failed, %d errors",
        elapsed, stats['passed'], stats['failed'], stats['errors'],
    )

    save_checkpoint(label="final")


if __name__ == '__main__':
    sys.exit(main() or 0)
