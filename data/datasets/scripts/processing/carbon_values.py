#!/usr/bin/env python3
"""
Add carbon footprint values to fashion materials based on literature values
and ecoinvent typical values for material production.

Note: Since the ecoinvent database in Unit process format doesn't contain
pre-calculated LCA results, we use literature values from published LCA studies
that comply with ISO 14040/14044 standards.
"""

import csv
from pathlib import Path

# Carbon footprint values in kg CO2-eq per kg material
# Sources: Ecoinvent reports, Higg MSI, various peer-reviewed LCA studies
MATERIAL_CARBON_FOOTPRINTS = {
    # Natural fibers
    'cotton': 5.89,  # Conventional cotton fiber production
    'cotton, organic': 3.46,  # Organic cotton (lower due to no synthetic fertilizers)
    'wool': 10.4,  # Wool fiber production
    'silk': 22.0,  # Silk fiber production
    'jute': 2.16,  # Jute fiber
    'flax': 2.29,  # Flax fiber
    'hemp': 1.92,  # Hemp fiber
    'linen': 2.1,  # Linen (from flax)

    # Synthetic fibers
    'polyester': 6.98,  # PET polyester fiber
    'nylon 6': 7.6,  # Nylon 6 production
    'nylon 6-6': 8.62,  # Nylon 6-6 production
    'acrylic': 8.5,  # Acrylic fiber
    'elastane': 11.0,  # Elastane/spandex
    'viscose': 5.48,  # Viscose rayon fiber

    # Semi-synthetic
    'rayon': 5.5,  # Rayon fiber
    'lyocell': 3.0,  # Lyocell (Tencel)
    'modal': 4.2,  # Modal fiber

    # Processed materials
    'yarn': 1.2,  # Additional processing for yarn (add to fiber value)
    'fabric, woven': 0.8,  # Weaving process per kg
    'fabric, knit': 0.6,  # Knitting process per kg
    'dyeing': 3.5,  # Dyeing and finishing per kg
    'textile': 2.0,  # Generic textile processing

    # Other materials
    'leather': 17.0,  # Leather production
    'rubber': 3.2,  # Rubber
    'down': 12.5,  # Down feathers

    # Glass and mineral fibers
    'glass fiber': 3.18,  # Glass fiber
    'glass fibre': 3.18,
    'carbon fiber': 24.0,  # Carbon fiber
    'carbon fibre': 24.0,
    'stone wool': 1.35,  # Rock wool/stone wool
    'glass wool': 1.28,  # Glass wool insulation
    'mineral wool': 1.3,  # Generic mineral wool

    # Resins and plastics
    'polyester resin': 3.4,  # Unsaturated polyester resin
    'epoxy': 6.0,  # Epoxy resin

    # Seeds and oils
    'cottonseed': 0.5,  # Cotton seeds
    'cottonseed oil': 1.8,  # Cottonseed oil

    # Construction materials with fibers
    'fibreboard': 0.72,  # Wood fiberboard
    'fibre cement': 0.82,  # Fiber cement
    'fiber cement': 0.82,
}

def estimate_carbon_footprint(material_name):
    """
    Estimate carbon footprint based on material name keywords.
    Returns (value, confidence, notes)
    """
    name_lower = material_name.lower()

    # Check for exact matches or keyword matches
    matched_values = []

    for keyword, value in MATERIAL_CARBON_FOOTPRINTS.items():
        if keyword in name_lower:
            matched_values.append((value, keyword))

    if not matched_values:
        # Check for generic categories
        if 'waste' in name_lower or 'recycl' in name_lower:
            return (0.0, 'low', 'Waste/recycled material - minimal footprint')
        elif 'water' in name_lower or 'wastewater' in name_lower:
            return (0.0, 'low', 'Wastewater - not a material')
        elif 'finishing' in name_lower or 'bleach' in name_lower or 'merceri' in name_lower:
            return (3.5, 'medium', 'Estimated from finishing processes')
        elif 'weaving' in name_lower or 'spinning' in name_lower or 'knit' in name_lower:
            return (0.8, 'medium', 'Estimated from textile processing')
        else:
            return (None, 'unknown', 'No data available')

    # If multiple matches, sum them (e.g., "dyed cotton fabric" = cotton + dyeing + fabric)
    if len(matched_values) == 1:
        return (matched_values[0][0], 'high', f'Based on {matched_values[0][1]}')
    else:
        total = sum(v[0] for v in matched_values)
        keywords = ', '.join(v[1] for v in matched_values)
        return (total, 'medium', f'Composite: {keywords}')

def main():
    input_csv = Path(__file__).parent / 'dataset' / 'fashion_materials_carbon_footprint.csv'
    output_csv = Path(__file__).parent / 'dataset' / 'fashion_materials_carbon_footprint_complete.csv'

    materials = []

    # Read existing CSV
    print("Reading existing materials...")
    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            materials.append(row)

    print(f"Found {len(materials)} materials")

    # Add carbon footprint values
    print("Estimating carbon footprint values...")
    for material in materials:
        value, confidence, notes = estimate_carbon_footprint(material['Material Name'])
        material['Carbon_Footprint_kg_CO2eq_per_kg'] = value if value is not None else ''
        material['Data_Confidence'] = confidence
        material['Notes'] = notes

    # Write updated CSV
    print(f"Writing complete dataset to {output_csv}...")
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'Material Name',
            'Type',
            'Reference ID',
            'Description',
            'Carbon_Footprint_kg_CO2eq_per_kg',
            'Data_Confidence',
            'Notes'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(materials)

    # Statistics
    with_values = [m for m in materials if m['Carbon_Footprint_kg_CO2eq_per_kg'] != '']
    high_conf = [m for m in materials if m['Data_Confidence'] == 'high']

    print(f"\nStatistics:")
    print(f"  Total materials: {len(materials)}")
    print(f"  With carbon footprint values: {len(with_values)} ({len(with_values)/len(materials)*100:.1f}%)")
    print(f"  High confidence: {len(high_conf)} ({len(high_conf)/len(materials)*100:.1f}%)")

    # Show top materials by carbon footprint
    print(f"\nTop 10 materials by carbon footprint:")
    materials_sorted = sorted(
        [m for m in materials if m['Carbon_Footprint_kg_CO2eq_per_kg'] != ''],
        key=lambda x: float(x['Carbon_Footprint_kg_CO2eq_per_kg']),
        reverse=True
    )
    for i, mat in enumerate(materials_sorted[:10], 1):
        print(f"  {i}. {mat['Material Name']}: {mat['Carbon_Footprint_kg_CO2eq_per_kg']} kg CO2-eq/kg")

    print(f"\n Complete dataset saved to: {output_csv}")

if __name__ == "__main__":
    main()
