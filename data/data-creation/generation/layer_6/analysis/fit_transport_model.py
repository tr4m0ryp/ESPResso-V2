"""Step 3: Fit multinomial logit parameters to observed modal split.

Uses scipy.optimize to find alpha, beta, d_ref for each transport mode
that minimize the squared error between predicted and observed mode
frequencies across distance bands.
"""

import math
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import minimize


INPUT_PATH = Path(
    'data/data_generation/layer_6/analysis/output/modal_split_comparison.csv'
)
OUTPUT_DIR = Path(
    'data/data_generation/layer_6/analysis/output'
)

MODES = ['road', 'rail', 'sea', 'air', 'inland_waterway']


def multinomial_logit_probs(
    distances: np.ndarray,
    params_flat: np.ndarray,
) -> np.ndarray:
    """Compute mode probabilities for an array of distances.

    Args:
        distances: (N,) array of distance midpoints
        params_flat: (15,) array = [alpha, beta, d_ref] * 5 modes

    Returns:
        (N, 5) array of probabilities
    """
    n = len(distances)
    n_modes = len(MODES)
    utilities = np.zeros((n, n_modes))

    for i, mode in enumerate(MODES):
        alpha = params_flat[i * 3]
        beta = params_flat[i * 3 + 1]
        d_ref = params_flat[i * 3 + 2]
        utilities[:, i] = alpha + beta * (distances - d_ref)

    # Softmax with numerical stability
    max_u = utilities.max(axis=1, keepdims=True)
    exp_u = np.exp(utilities - max_u)
    probs = exp_u / exp_u.sum(axis=1, keepdims=True)
    return probs


def loss_function(
    params_flat: np.ndarray,
    distances: np.ndarray,
    observed: np.ndarray,
    weights: np.ndarray,
) -> float:
    """Sum of weighted squared errors between predicted and observed.

    Args:
        params_flat: (15,) parameter vector
        distances: (N,) distance midpoints
        observed: (N, 5) observed mode frequencies
        weights: (N,) per-band weight (leg count)

    Returns:
        Weighted sum of squared errors
    """
    predicted = multinomial_logit_probs(distances, params_flat)
    errors = (predicted - observed) ** 2
    # Weight by number of legs in each band
    weighted = errors * weights[:, np.newaxis]
    return weighted.sum()


def fit_parameters(comparison_df: pd.DataFrame) -> dict:
    """Fit multinomial logit parameters to observed data."""
    distances = comparison_df['midpoint_km'].values.astype(float)
    weights = comparison_df['n_legs'].values.astype(float)
    # Normalize weights so total = 1
    weights = weights / weights.sum()

    observed = np.zeros((len(distances), len(MODES)))
    for i, mode in enumerate(MODES):
        col = f'obs_{mode}'
        observed[:, i] = comparison_df[col].values.astype(float)

    # Initial guess from current parameters
    from data.data_generation.layer_6.config.config import (
        TRANSPORT_MODE_PARAMS,
    )
    x0 = []
    for mode in MODES:
        p = TRANSPORT_MODE_PARAMS[mode]
        x0.extend([p['alpha'], p['beta'], p['d_ref']])
    x0 = np.array(x0, dtype=float)

    # Bounds: alpha in [-10, 10], beta in [-0.01, 0.01], d_ref in [10, 25000]
    bounds = []
    for _ in MODES:
        bounds.append((-10.0, 10.0))      # alpha
        bounds.append((-0.01, 0.01))       # beta
        bounds.append((10.0, 25000.0))     # d_ref

    result = minimize(
        loss_function,
        x0,
        args=(distances, observed, weights),
        method='L-BFGS-B',
        bounds=bounds,
        options={'maxiter': 10000, 'ftol': 1e-12},
    )

    print(f'Optimization converged: {result.success}')
    print(f'Final loss: {result.fun:.8f}')
    if not result.success:
        print(f'Message: {result.message}')

    # Extract fitted parameters
    fitted = {}
    for i, mode in enumerate(MODES):
        fitted[mode] = {
            'alpha': round(result.x[i * 3], 6),
            'beta': round(result.x[i * 3 + 1], 8),
            'd_ref': round(result.x[i * 3 + 2], 1),
        }

    return fitted


def validate_fit(
    fitted: dict,
    comparison_df: pd.DataFrame,
) -> pd.DataFrame:
    """Compare fitted model predictions against observed data."""
    distances = comparison_df['midpoint_km'].values.astype(float)

    params_flat = []
    for mode in MODES:
        p = fitted[mode]
        params_flat.extend([p['alpha'], p['beta'], p['d_ref']])
    params_flat = np.array(params_flat)

    predicted = multinomial_logit_probs(distances, params_flat)

    result_df = comparison_df[['band', 'midpoint_km', 'n_legs']].copy()
    for i, mode in enumerate(MODES):
        result_df[f'obs_{mode}'] = comparison_df[f'obs_{mode}']
        result_df[f'fitted_{mode}'] = predicted[:, i]
        result_df[f'diff_{mode}'] = (
            comparison_df[f'obs_{mode}'].values - predicted[:, i]
        )

    return result_df


def format_params_for_config(fitted: dict) -> str:
    """Format fitted parameters as Python dict literal for config.py."""
    lines = [
        "TRANSPORT_MODE_PARAMS: Dict[str, Dict[str, float]] = {",
    ]
    entries = []
    for mode in MODES:
        p = fitted[mode]
        if mode == 'inland_waterway':
            entries.append(
                f"    '{mode}': {{\n"
                f"        'alpha': {p['alpha']}, "
                f"'beta': {p['beta']}, "
                f"'d_ref': {p['d_ref']}\n"
                f"    }}"
            )
        else:
            entries.append(
                f"    '{mode}': {{"
                f"'alpha': {p['alpha']}, "
                f"'beta': {p['beta']}, "
                f"'d_ref': {p['d_ref']}"
                f"}}"
            )
    lines.append(',\n'.join(entries))
    lines.append("}")
    return '\n'.join(lines)


def main():
    print(f'Reading modal split comparison from {INPUT_PATH}...')
    comparison_df = pd.read_csv(INPUT_PATH)
    print(f'  Distance bands: {len(comparison_df)}')

    print('\nFitting multinomial logit parameters...')
    fitted = fit_parameters(comparison_df)

    print('\n=== FITTED PARAMETERS ===\n')
    for mode in MODES:
        p = fitted[mode]
        print(
            f"  {mode:20s}: alpha={p['alpha']:8.4f}  "
            f"beta={p['beta']:12.8f}  d_ref={p['d_ref']:8.1f}"
        )

    print('\n=== VALIDATION: FITTED vs OBSERVED ===\n')
    val_df = validate_fit(fitted, comparison_df)
    for _, row in val_df.iterrows():
        print(f'  Band {row["band"]:>10s} (n={row["n_legs"]:.0f}):')
        for mode in MODES:
            obs = row[f'obs_{mode}']
            fit = row[f'fitted_{mode}']
            diff = row[f'diff_{mode}']
            print(
                f'    {mode:20s}: obs={obs:.3f}  '
                f'fitted={fit:.3f}  diff={diff:+.3f}'
            )

    # Compute improvement
    old_mae = 0.0
    new_mae = 0.0
    for mode in MODES:
        if f'pred_{mode}' in comparison_df.columns:
            old_mae += (
                comparison_df[f'obs_{mode}']
                - comparison_df[f'pred_{mode}']
            ).abs().mean()
        new_mae += val_df[f'diff_{mode}'].abs().mean()

    print(f'\nOld model total MAE: {old_mae:.4f}')
    print(f'New model total MAE: {new_mae:.4f}')
    if old_mae > 0:
        improvement = (1.0 - new_mae / old_mae) * 100
        print(f'Improvement: {improvement:.1f}%')

    print('\n=== CONFIG SNIPPET ===\n')
    print(format_params_for_config(fitted))

    # Save fitted parameters
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    val_df.to_csv(OUTPUT_DIR / 'fitted_validation.csv', index=False)

    params_df = pd.DataFrame([
        {'mode': mode, **params}
        for mode, params in fitted.items()
    ])
    params_df.to_csv(OUTPUT_DIR / 'fitted_parameters.csv', index=False)

    with open(OUTPUT_DIR / 'config_snippet.py', 'w') as f:
        f.write(format_params_for_config(fitted))
        f.write('\n')

    print(f'\nSaved fitted parameters to {OUTPUT_DIR}/')


if __name__ == '__main__':
    main()
