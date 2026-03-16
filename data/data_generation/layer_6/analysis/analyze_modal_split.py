"""Step 2: Analyze modal split by distance band.

Bins transport legs by distance, computes actual modal split per band,
and compares against the current multinomial logit model predictions.
"""

import math
from pathlib import Path

import pandas as pd

from data.data_generation.layer_6.config.config import (
    TRANSPORT_MODE_PARAMS,
    TRANSPORT_EMISSION_FACTORS,
)
from data.data_generation.layer_6.core.transport_model import (
    TransportModeModel,
)


INPUT_PATH = Path(
    'data/data_generation/layer_6/analysis/output/transport_legs_flat.csv'
)
OUTPUT_DIR = Path(
    'data/data_generation/layer_6/analysis/output'
)

MODES = ['road', 'rail', 'sea', 'air', 'inland_waterway']

DISTANCE_BANDS = [
    (0, 100, '0-100'),
    (100, 500, '100-500'),
    (500, 2000, '500-2000'),
    (2000, 5000, '2000-5000'),
    (5000, 10000, '5000-10000'),
    (10000, 30000, '10000+'),
]


def compute_observed_split(legs_df: pd.DataFrame) -> pd.DataFrame:
    """Compute actual modal split per distance band."""
    rows = []
    for lo, hi, label in DISTANCE_BANDS:
        band = legs_df[
            (legs_df['leg_distance_km'] >= lo)
            & (legs_df['leg_distance_km'] < hi)
        ]
        n = len(band)
        if n == 0:
            continue

        midpoint = (lo + hi) / 2.0
        if hi >= 30000:
            midpoint = 15000.0

        row = {
            'band': label,
            'midpoint_km': midpoint,
            'n_legs': n,
        }
        for mode in MODES:
            count = len(band[band['primary_mode'] == mode])
            row[f'obs_{mode}'] = count / n
        rows.append(row)

    return pd.DataFrame(rows)


def compute_predicted_split(bands_df: pd.DataFrame) -> pd.DataFrame:
    """Run current model at each band midpoint."""
    model = TransportModeModel()

    for _, row in bands_df.iterrows():
        d = row['midpoint_km']
        probs = model.calculate_mode_probabilities(d)
        for mode in MODES:
            bands_df.loc[
                bands_df['band'] == row['band'],
                f'pred_{mode}'
            ] = probs.get(mode, 0.0)

    return bands_df


def format_comparison(df: pd.DataFrame) -> str:
    """Format a readable comparison table."""
    lines = []
    header = f'{"Band":>12s} {"N":>6s}'
    for m in MODES:
        short = m[:5].upper()
        header += f'  {short + "_obs":>9s} {short + "_pred":>9s} {"diff":>7s}'
    lines.append(header)
    lines.append('-' * len(header))

    for _, row in df.iterrows():
        line = f'{row["band"]:>12s} {row["n_legs"]:6.0f}'
        for m in MODES:
            obs = row.get(f'obs_{m}', 0.0)
            pred = row.get(f'pred_{m}', 0.0)
            diff = obs - pred
            line += f'  {obs:9.3f} {pred:9.3f} {diff:+7.3f}'
        lines.append(line)

    return '\n'.join(lines)


def compute_error_summary(df: pd.DataFrame) -> str:
    """Compute per-mode mean absolute error across bands."""
    lines = ['Per-mode mean absolute error (MAE):']
    total_mae = 0.0
    for m in MODES:
        obs_col = f'obs_{m}'
        pred_col = f'pred_{m}'
        if obs_col in df.columns and pred_col in df.columns:
            mae = (df[obs_col] - df[pred_col]).abs().mean()
            total_mae += mae
            lines.append(f'  {m:20s}: {mae:.4f}')
    lines.append(f'  {"TOTAL":20s}: {total_mae:.4f}')
    return '\n'.join(lines)


def main():
    print(f'Reading extracted legs from {INPUT_PATH}...')
    legs_df = pd.read_csv(INPUT_PATH)
    print(f'  Total legs: {len(legs_df)}')

    print('\nComputing observed modal split by distance band...')
    bands_df = compute_observed_split(legs_df)

    print('Computing predicted modal split (current model)...')
    bands_df = compute_predicted_split(bands_df)

    print('\n=== OBSERVED vs PREDICTED MODAL SPLIT ===\n')
    print(format_comparison(bands_df))

    print()
    print(compute_error_summary(bands_df))

    # Save results
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / 'modal_split_comparison.csv'
    bands_df.to_csv(out_path, index=False)
    print(f'\nSaved comparison to {out_path}')


if __name__ == '__main__':
    main()
