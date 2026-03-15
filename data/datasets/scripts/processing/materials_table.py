#!/usr/bin/env python3
"""
Create a simplified table with just the essential primary materials.
"""

import csv
from pathlib import Path

# Essential materials - most commonly used in fashion
ESSENTIAL_MATERIALS = [
    'fibre, cotton',
    'fibre, polyester',
    'nylon 6',
    'nylon 6-6',
    'fibre, viscose',
    'fibre, flax',
    'fibre, jute',
    'decorticated fibre, hemp',
    'fibre, silk, short',
    'glass fibre',
    'stone wool',
]

def main():
    input_csv = Path(__file__).parent / 'dataset' / 'fashion_materials_high_confidence.csv'
    output_csv = Path(__file__).parent / 'dataset' / 'essential_materials_simplified.csv'

    print("Reading high confidence materials...")
    all_materials = []

    with open(input_csv, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            all_materials.append(row)

    # Filter for essential materials
    essential = []
    for mat_name in ESSENTIAL_MATERIALS:
        for mat in all_materials:
            if mat['Material Name'].lower() == mat_name.lower():
                essential.append(mat)
                break

    print(f"Found {len(essential)} essential materials")

    # Write simplified CSV
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        fieldnames = [
            'Material',
            'Carbon_Footprint_kg_CO2eq_per_kg',
            'Type',
            'Reference_ID'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for mat in essential:
            writer.writerow({
                'Material': mat['Material Name'],
                'Carbon_Footprint_kg_CO2eq_per_kg': mat['Carbon_Footprint_kg_CO2eq_per_kg'],
                'Type': 'Natural Fiber' if any(x in mat['Material Name'].lower()
                    for x in ['cotton', 'flax', 'jute', 'hemp', 'silk']) else
                    'Synthetic Fiber' if any(x in mat['Material Name'].lower()
                    for x in ['polyester', 'nylon', 'viscose']) else 'Technical Fiber',
                'Reference_ID': mat['Reference ID']
            })

    print(f"\n Essential materials table saved to: {output_csv}")

    # Print table
    print("\nEssential Fashion Materials (High Confidence):")
    print("=" * 80)
    print(f"{'Material':<30} {'Type':<20} {'kg CO2-eq/kg':>15}")
    print("-" * 80)

    for mat in essential:
        mat_type = 'Natural' if any(x in mat['Material Name'].lower()
            for x in ['cotton', 'flax', 'jute', 'hemp', 'silk']) else \
            'Synthetic' if any(x in mat['Material Name'].lower()
            for x in ['polyester', 'nylon', 'viscose']) else 'Technical'

        cf = mat['Carbon_Footprint_kg_CO2eq_per_kg']
        print(f"{mat['Material Name']:<30} {mat_type:<20} {cf:>15}")

    print("=" * 80)

if __name__ == "__main__":
    main()
