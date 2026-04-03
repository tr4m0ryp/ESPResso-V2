#!/usr/bin/env python3
"""Run Layer 6 enrichment via Claude Code CLI for OAuth-based access.

Spawns parallel `claude -p` processes to extract transport distances.
Uses the optimized prompt (single-mode legs pre-computed).

Usage:
    python3 -m data.data_generation.scripts.run_layer6_claude_cli [--workers N]
"""

import argparse
import json
import logging
import os
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

BATCH_SIZE = 100
TOLERANCE = 0.02
CKPT_DIR = 'data/datasets/pre-model/generated/layer_6/checkpoints/enrichment'
LAYER5 = 'data/datasets/pre-model/generated/layer_5/layer_5_validated_dataset.csv'
LAYER4 = 'data/datasets/pre-model/generated/layer_4/layer_4_complete_dataset.parquet'

stats_lock = threading.Lock()
stats = {'passed': 0, 'failed': 0, 'errors': 0, 'completed': 0}
results_lock = threading.Lock()
all_results = []


def process_batch(batch_records, batch_idx, total):
    """Run one batch through claude CLI."""
    prompt = get_system_prompt() + '\n\n' + build_batch_prompt(batch_records)
    prompt = (
        "CRITICAL: Output ONLY a raw JSON array. No text, no explanation, "
        "no code blocks, no markdown. Just [ ... ].\n\n" + prompt
    )

    try:
        proc = subprocess.run(
            ['claude', '-p', '--model', 'claude-sonnet-4-6',
             '--max-turns', '1', '--output-format', 'json'],
            input=prompt, capture_output=True, text=True, timeout=600,
        )
        if proc.returncode != 0:
            logger.warning("Batch %d: CLI error: %s", batch_idx, proc.stderr[:200])
            with stats_lock:
                stats['errors'] += 1
            return

        data = json.loads(proc.stdout)
        result_text = data.get('result', '')

        s = result_text.find('[')
        e = result_text.rfind(']')
        if s < 0 or e <= s:
            logger.warning("Batch %d: no JSON array in output", batch_idx)
            with stats_lock:
                stats['errors'] += 1
            return

        extractions = json.loads(result_text[s:e + 1])

        extraction_map = {str(ex.get('id', '')): ex for ex in extractions}
        batch_results = []

        for rec in batch_records:
            rid = str(rec.get('record_id', ''))
            ext = extraction_map.get(rid)
            if not ext:
                with stats_lock:
                    stats['failed'] += 1
                continue

            total_km = float(rec.get('total_distance_km', 0))
            vr = validate_extraction(ext, total_km, TOLERANCE)

            modes = ['road_km', 'sea_km', 'rail_km', 'air_km',
                     'inland_waterway_km']
            row = {'record_id': rid, 'is_valid': vr.is_valid}
            for m in modes:
                row[m] = ext.get(m, 0.0)
            batch_results.append(row)

            with stats_lock:
                if vr.is_valid:
                    stats['passed'] += 1
                else:
                    stats['failed'] += 1

        with results_lock:
            all_results.extend(batch_results)

    except Exception as exc:
        logger.warning("Batch %d failed: %s", batch_idx, exc)
        with stats_lock:
            stats['errors'] += 1

    with stats_lock:
        stats['completed'] += 1
        if stats['completed'] % 10 == 0:
            logger.info(
                "Progress: %d/%d batches, %d passed, %d failed, %d errors",
                stats['completed'], total,
                stats['passed'], stats['failed'], stats['errors'],
            )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--workers', '-w', type=int, default=5)
    args = parser.parse_args()

    logger.info("Loading data...")
    df = join_transport_legs(LAYER5, LAYER4)

    # Load completed IDs from checkpoints
    completed_ids = set()
    ckpt_path = Path(CKPT_DIR)
    if ckpt_path.exists():
        for f in sorted(ckpt_path.glob('*.csv')):
            cdf = pd.read_csv(f)
            valid_ids = set(cdf[cdf['is_valid'] == True]['record_id'])
            completed_ids.update(valid_ids)
    logger.info("Already valid: %d", len(completed_ids))

    remaining = df[~df['record_id'].isin(completed_ids)]
    logger.info("Remaining: %d records in %d batches",
                len(remaining), (len(remaining) + BATCH_SIZE - 1) // BATCH_SIZE)

    records = remaining.to_dict('records')
    batches = [records[i:i + BATCH_SIZE]
               for i in range(0, len(records), BATCH_SIZE)]

    logger.info("Launching %d workers for %d batches", args.workers, len(batches))
    start = time.time()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_batch, batch, idx, len(batches)): idx
            for idx, batch in enumerate(batches, 1)
        }
        for future in as_completed(futures):
            future.result()

    elapsed = time.time() - start
    logger.info("Done in %.1fs: %d passed, %d failed, %d errors",
                elapsed, stats['passed'], stats['failed'], stats['errors'])

    # Save results as new checkpoint
    if all_results:
        out_df = pd.DataFrame(all_results)
        ckpt_path.mkdir(parents=True, exist_ok=True)
        existing = sorted(ckpt_path.glob('enrichment_batch_*.csv'))
        next_num = len(existing) + 1
        out_file = ckpt_path / f'enrichment_batch_{next_num:04d}.csv'
        out_df.to_csv(out_file, index=False)
        logger.info("Saved checkpoint: %s (%d rows)", out_file, len(out_df))


if __name__ == '__main__':
    sys.exit(main() or 0)
