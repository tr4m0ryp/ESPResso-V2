#!/usr/bin/env python3
"""
Create a clean dataset containing only high confidence materials.
"""

import csv
from pathlib import Path

def main():
    input_csv = Path(__file__).parent / 'dataset' / 'fashion_materials_carbon_footprint.csv'
    output_csv = Path(__file__).parent / 'dataset' / 'fashion_materials_high_confidence.csv'

    print("Reading materials dataset...")
    materials = []

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row['Data_Confidence'] == 'high':
                materials.append(row)

    print(f"Found {len(materials)} high confidence materials")

    # Write high confidence materials to new CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'Material Name',
            'Type',
            'Reference ID',
            'Description',
            'Carbon_Footprint_kg_CO2eq_per_kg',
            'Notes'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for mat in materials:
            writer.writerow({
                'Material Name': mat['Material Name'],
                'Type': mat['Type'],
                'Reference ID': mat['Reference ID'],
                'Description': mat['Description'],
                'Carbon_Footprint_kg_CO2eq_per_kg': mat['Carbon_Footprint_kg_CO2eq_per_kg'],
                'Notes': mat['Notes']
            })

    # Statistics by material type
    flows = [m for m in materials if m['Type'] == 'flow']
    processes = [m for m in materials if m['Type'] == 'process']

    # Get unique primary materials
    primary_materials = {}
    for mat in materials:
        name = mat['Material Name'].lower()
        # Extract primary material keywords
        for keyword in ['cotton', 'polyester', 'nylon', 'wool', 'silk', 'linen', 'hemp',
                       'jute', 'flax', 'viscose', 'glass fiber', 'glass fibre',
                       'carbon fiber', 'carbon fibre', 'stone wool', 'glass wool']:
            if keyword in name and 'waste' not in name and 'wastewater' not in name:
                if keyword not in primary_materials:
                    primary_materials[keyword] = []
                primary_materials[keyword].append(mat)
                break

    print(f"\nHigh Confidence Dataset Statistics:")
    print(f"  Total materials: {len(materials)}")
    print(f"  Flows: {len(flows)}")
    print(f"  Processes: {len(processes)}")
    print(f"  Primary material categories: {len(primary_materials)}")

    print(f"\nPrimary Materials Found:")
    for keyword, mats in sorted(primary_materials.items()):
        print(f"  {keyword.title()}: {len(mats)} entries")

    print(f"\nTop 10 materials by carbon footprint:")
    materials_sorted = sorted(
        materials,
        key=lambda x: float(x['Carbon_Footprint_kg_CO2eq_per_kg']) if x['Carbon_Footprint_kg_CO2eq_per_kg'] else 0,
        reverse=True
    )
    for i, mat in enumerate(materials_sorted[:10], 1):
        print(f"  {i}. {mat['Material Name']}: {mat['Carbon_Footprint_kg_CO2eq_per_kg']} kg CO2-eq/kg")

    print(f"\n High confidence dataset saved to: {output_csv}")
    print(f"  File size: {output_csv.stat().st_size / 1024:.1f} KB")

if __name__ == "__main__":
    main()
