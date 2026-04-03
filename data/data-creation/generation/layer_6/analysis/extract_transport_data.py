"""Step 1: Extract transport leg data from Layer 4 parquet.

Reads the Layer 4 dataset, parses the transport_legs JSON column,
and produces a flat CSV of individual transport legs with distance,
mode, material, and location information.
"""

import json
from pathlib import Path

import pandas as pd


INPUT_PATH = Path(
    'data/datasets/pre-model/generated/'
    'layer_4/layer_4_complete_dataset.parquet'
)
OUTPUT_DIR = Path(
    'data/data_generation/layer_6/analysis/output'
)
OUTPUT_PATH = OUTPUT_DIR / 'transport_legs_flat.csv'

# Mode dominance order for mixed-mode legs (longer-haul modes rank higher)
MODE_RANK = {
    'sea': 5,
    'air': 4,
    'rail': 3,
    'inland_waterway': 2,
    'road': 1,
}


def infer_primary_mode(modes: list, distance_km: float) -> str:
    """Determine the dominant transport mode for a multi-mode leg.

    For single-mode legs, returns that mode directly.
    For multi-mode legs (e.g. ["road", "sea", "road"]):
      - At distances >= 500 km, pick the highest-ranked mode
        (sea > air > rail > inland_waterway > road)
      - At distances < 500 km, default to road unless a higher mode
        appears more than once
    """
    if not modes:
        return 'road'
    if len(modes) == 1:
        return modes[0]

    # Count occurrences and find highest-ranked mode
    from collections import Counter
    counts = Counter(modes)
    highest = max(modes, key=lambda m: MODE_RANK.get(m, 0))

    if distance_km >= 500:
        return highest

    # Short distance: only pick non-road if it appears more than once
    if counts.get(highest, 0) > 1 or highest == 'road':
        return highest
    return 'road'


def extract_legs(df: pd.DataFrame) -> pd.DataFrame:
    """Parse transport_legs JSON and flatten into individual rows."""
    rows = []

    for _, record in df.iterrows():
        raw_legs = record.get('transport_legs', '[]')
        if isinstance(raw_legs, str):
            try:
                legs = json.loads(raw_legs)
            except (json.JSONDecodeError, TypeError):
                continue
        elif isinstance(raw_legs, list):
            legs = raw_legs
        else:
            continue

        for leg in legs:
            if not isinstance(leg, dict):
                continue

            distance = leg.get('distance_km', 0)
            modes = leg.get('transport_modes', [])
            if isinstance(modes, str):
                modes = [modes]

            primary = infer_primary_mode(modes, distance)
            mode_combo = '+'.join(modes) if len(modes) > 1 else modes[0] if modes else 'unknown'

            rows.append({
                'leg_distance_km': distance,
                'primary_mode': primary,
                'mode_combo': mode_combo,
                'num_modes': len(modes),
                'modes_raw': json.dumps(modes),
                'material': leg.get('material', ''),
                'from_location': leg.get('from_location', ''),
                'to_location': leg.get('to_location', ''),
            })

    return pd.DataFrame(rows)


def main():
    print(f'Reading Layer 4 data from {INPUT_PATH}...')
    df = pd.read_parquet(INPUT_PATH)
    print(f'  Records: {len(df)}')

    print('Extracting transport legs...')
    legs_df = extract_legs(df)
    print(f'  Total legs extracted: {len(legs_df)}')

    if legs_df.empty:
        print('No transport legs found. Exiting.')
        return

    # Summary stats
    print(f'\nPrimary mode distribution:')
    mode_counts = legs_df['primary_mode'].value_counts()
    for mode, count in mode_counts.items():
        pct = 100.0 * count / len(legs_df)
        print(f'  {mode:20s}: {count:6d} ({pct:5.1f}%)')

    print(f'\nDistance stats (km):')
    print(f'  Mean:   {legs_df["leg_distance_km"].mean():.0f}')
    print(f'  Median: {legs_df["leg_distance_km"].median():.0f}')
    print(f'  Min:    {legs_df["leg_distance_km"].min():.0f}')
    print(f'  Max:    {legs_df["leg_distance_km"].max():.0f}')

    multi_mode = legs_df[legs_df['num_modes'] > 1]
    print(f'\nMulti-mode legs: {len(multi_mode)} '
          f'({100.0 * len(multi_mode) / len(legs_df):.1f}%)')
    if not multi_mode.empty:
        print('  Top combos:')
        for combo, cnt in multi_mode['mode_combo'].value_counts().head(10).items():
            print(f'    {combo}: {cnt}')

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    legs_df.to_csv(OUTPUT_PATH, index=False)
    print(f'\nSaved {len(legs_df)} legs to {OUTPUT_PATH}')


if __name__ == '__main__':
    main()
