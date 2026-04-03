"""Step 5: Compare transport footprint under old vs new parameters.

Loads a sample of Layer 4 records, computes transport emissions
with both old and new (fitted) parameters, and reports the impact.
"""

import json
from pathlib import Path

import numpy as np
import pandas as pd

from data.data_generation.layer_6.config.config import (
    TRANSPORT_EMISSION_FACTORS,
)
from data.data_generation.layer_6.core.transport_model import (
    TransportModeModel,
)

# Original hardcoded parameters (before fitting)
OLD_TRANSPORT_MODE_PARAMS = {
    'road': {'alpha': 2.5, 'beta': -0.0003, 'd_ref': 500},
    'rail': {'alpha': 1.8, 'beta': -0.00015, 'd_ref': 1500},
    'inland_waterway': {'alpha': 0.5, 'beta': -0.0002, 'd_ref': 800},
    'sea': {'alpha': 1.0, 'beta': 0.00008, 'd_ref': 5000},
    'air': {'alpha': -2.0, 'beta': 0.00025, 'd_ref': 8000},
}


INPUT_PATH = Path(
    'data/datasets/pre-model/generated/'
    'layer_4/layer_4_complete_dataset.parquet'
)
FITTED_PARAMS_PATH = Path(
    'data/data_generation/layer_6/analysis/output/fitted_parameters.csv'
)
OUTPUT_DIR = Path(
    'data/data_generation/layer_6/analysis/output'
)


def load_fitted_params() -> dict:
    """Load fitted parameters from CSV."""
    df = pd.read_csv(FITTED_PARAMS_PATH)
    params = {}
    for _, row in df.iterrows():
        params[row['mode']] = {
            'alpha': row['alpha'],
            'beta': row['beta'],
            'd_ref': row['d_ref'],
        }
    return params


def compute_footprints(
    records_df: pd.DataFrame,
    old_model: TransportModeModel,
    new_model: TransportModeModel,
) -> pd.DataFrame:
    """Compute transport footprint for each record under both models."""
    rows = []
    for _, record in records_df.iterrows():
        weight = record.get('total_weight_kg', 1.0)
        distance = record.get('total_distance_km', 0.0)

        if distance <= 0 or weight <= 0:
            continue

        old_result = old_model.calculate_transport_footprint(weight, distance)
        new_result = new_model.calculate_transport_footprint(weight, distance)

        old_fp = old_result['footprint_kg_co2e']
        new_fp = new_result['footprint_kg_co2e']

        if old_fp > 0:
            pct_change = ((new_fp - old_fp) / old_fp) * 100
        else:
            pct_change = 0.0

        rows.append({
            'total_distance_km': distance,
            'total_weight_kg': weight,
            'old_footprint_kg_co2e': old_fp,
            'new_footprint_kg_co2e': new_fp,
            'abs_change_kg_co2e': new_fp - old_fp,
            'pct_change': pct_change,
            'old_weighted_ef': old_result['weighted_ef_g_co2e_tkm'],
            'new_weighted_ef': new_result['weighted_ef_g_co2e_tkm'],
        })

    return pd.DataFrame(rows)


def print_summary(comparison_df: pd.DataFrame):
    """Print summary statistics of the comparison."""
    n = len(comparison_df)
    print(f'\n=== FOOTPRINT COMPARISON: OLD vs NEW PARAMETERS ===')
    print(f'Records analyzed: {n}\n')

    print('Percent change in transport footprint:')
    pct = comparison_df['pct_change']
    print(f'  Mean:   {pct.mean():+.2f}%')
    print(f'  Median: {pct.median():+.2f}%')
    print(f'  Std:    {pct.std():.2f}%')
    print(f'  Min:    {pct.min():+.2f}%')
    print(f'  Max:    {pct.max():+.2f}%')

    print('\nAbsolute change in transport footprint (kgCO2e):')
    abs_c = comparison_df['abs_change_kg_co2e']
    print(f'  Mean:   {abs_c.mean():+.4f}')
    print(f'  Median: {abs_c.median():+.4f}')

    print('\nWeighted emission factor (g CO2e/tkm):')
    print(f'  Old mean: {comparison_df["old_weighted_ef"].mean():.2f}')
    print(f'  New mean: {comparison_df["new_weighted_ef"].mean():.2f}')

    # Breakdown by distance range
    bands = [
        (0, 100, '0-100 km'),
        (100, 500, '100-500 km'),
        (500, 2000, '500-2000 km'),
        (2000, 5000, '2000-5000 km'),
        (5000, 10000, '5000-10000 km'),
        (10000, 30000, '10000+ km'),
    ]
    print('\nBy distance range:')
    print(f'  {"Range":>15s} {"N":>6s} {"Mean %chg":>10s} '
          f'{"Old EF":>8s} {"New EF":>8s}')
    print(f'  {"-"*15} {"-"*6} {"-"*10} {"-"*8} {"-"*8}')

    for lo, hi, label in bands:
        band = comparison_df[
            (comparison_df['total_distance_km'] >= lo)
            & (comparison_df['total_distance_km'] < hi)
        ]
        if band.empty:
            continue
        print(
            f'  {label:>15s} {len(band):6d} '
            f'{band["pct_change"].mean():+10.2f} '
            f'{band["old_weighted_ef"].mean():8.2f} '
            f'{band["new_weighted_ef"].mean():8.2f}'
        )

    # Direction counts
    increased = len(comparison_df[comparison_df['pct_change'] > 1.0])
    decreased = len(comparison_df[comparison_df['pct_change'] < -1.0])
    unchanged = n - increased - decreased
    print(f'\nDirection of change (>1% threshold):')
    print(f'  Increased: {increased} ({100*increased/n:.1f}%)')
    print(f'  Decreased: {decreased} ({100*decreased/n:.1f}%)')
    print(f'  Unchanged: {unchanged} ({100*unchanged/n:.1f}%)')


def main():
    print(f'Reading Layer 4 data from {INPUT_PATH}...')
    df = pd.read_parquet(INPUT_PATH)
    print(f'  Records: {len(df)}')

    print(f'Loading fitted parameters from {FITTED_PARAMS_PATH}...')
    new_params = load_fitted_params()

    old_model = TransportModeModel(
        mode_params=OLD_TRANSPORT_MODE_PARAMS,
        emission_factors=TRANSPORT_EMISSION_FACTORS,
    )
    new_model = TransportModeModel(
        mode_params=new_params,
        emission_factors=TRANSPORT_EMISSION_FACTORS,
    )

    print('Computing footprints...')
    comparison_df = compute_footprints(df, old_model, new_model)

    print_summary(comparison_df)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUTPUT_DIR / 'footprint_comparison.csv'
    comparison_df.to_csv(out_path, index=False)
    print(f'\nSaved comparison to {out_path}')


if __name__ == '__main__':
    main()
