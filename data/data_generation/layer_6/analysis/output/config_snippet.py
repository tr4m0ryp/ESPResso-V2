TRANSPORT_MODE_PARAMS: Dict[str, Dict[str, float]] = {
    'road': {'alpha': 9.124443, 'beta': -0.01, 'd_ref': 500.0},
    'rail': {'alpha': -0.008712, 'beta': -0.00310896, 'd_ref': 1500.0},
    'sea': {'alpha': 1.291277, 'beta': 0.00369985, 'd_ref': 5000.0},
    'air': {'alpha': -2.077025, 'beta': 0.00483928, 'd_ref': 8000.0},
    'inland_waterway': {
        'alpha': 0.106072, 'beta': -0.00325982, 'd_ref': 800.0
    }
}
