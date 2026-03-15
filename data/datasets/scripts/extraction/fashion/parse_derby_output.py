#!/usr/bin/env python3
"""
Parse the Derby ij output and create a clean CSV file with fashion materials.
"""

import csv
import re
from pathlib import Path

def parse_derby_output(input_file):
    """Parse the Derby ij output file."""

    materials = []

    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by the queries - look for the flows section and processes section
    flows_section_match = re.search(
        r'-- Query for fashion materials in flows.*?SELECT name, ref_id, description.*?ij>',
        content,
        re.DOTALL
    )

    processes_section_match = re.search(
        r'-- Query for fashion materials in processes.*?SELECT name, ref_id, description.*?ij>',
        content,
        re.DOTALL
    )

    # Parse flows
    if flows_section_match:
        flows_text = flows_section_match.group(0)
        # Extract rows - each row has NAME | REF_ID | DESCRIPTION format
        # Skip header lines
        lines = flows_text.split('\n')

        capture = False
        current_entry = {'name': '', 'ref_id': '', 'description': '', 'type': 'flow'}

        for line in lines:
            # Skip separator lines and headers
            if '----' in line or 'NAME' in line and 'REF_ID' in line:
                capture = True
                continue

            if not capture:
                continue

            # Stop at next query
            if 'ij>' in line or 'rows selected' in line:
                if current_entry['name']:
                    materials.append(current_entry.copy())
                break

            # Split by pipe but handle multi-line descriptions
            if '|' in line:
                parts = line.split('|')
                if len(parts) >= 3:
                    name = parts[0].strip()
                    ref_id = parts[1].strip()
                    description = parts[2].strip()

                    # If name is not empty, it's a new entry
                    if name and len(name) > 0 and not name.startswith('--'):
                        # Save previous entry if exists
                        if current_entry['name']:
                            materials.append(current_entry.copy())

                        # Start new entry
                        current_entry = {
                            'name': name,
                            'ref_id': ref_id,
                            'description': description,
                            'type': 'flow'
                        }
                    elif description:
                        # Continue previous description
                        current_entry['description'] += ' ' + description

    # Parse processes
    if processes_section_match:
        processes_text = processes_section_match.group(0)
        lines = processes_text.split('\n')

        capture = False
        current_entry = {'name': '', 'ref_id': '', 'description': '', 'type': 'process'}

        for line in lines:
            if '----' in line or 'NAME' in line and 'REF_ID' in line:
                capture = True
                continue

            if not capture:
                continue

            if 'ij>' in line or 'disconnect' in line or 'rows selected' in line:
                if current_entry['name']:
                    materials.append(current_entry.copy())
                break

            if '|' in line:
                parts = line.split('|')
                if len(parts) >= 3:
                    name = parts[0].strip()
                    ref_id = parts[1].strip()
                    description = parts[2].strip()

                    if name and len(name) > 0 and not name.startswith('--'):
                        if current_entry['name']:
                            materials.append(current_entry.copy())

                        current_entry = {
                            'name': name,
                            'ref_id': ref_id,
                            'description': description,
                            'type': 'process'
                        }
                    elif description:
                        current_entry['description'] += ' ' + description

    return materials

def main():
    input_file = Path(__file__).parent / "fashion_materials_output.txt"
    output_file = Path(__file__).parent / "fashion_materials_carbon_footprint.csv"

    print("Parsing Derby output file...")
    materials = parse_derby_output(input_file)

    print(f"Found {len(materials)} fashion material entries")

    # Remove duplicates based on ref_id
    unique_materials = {}
    for mat in materials:
        if mat['ref_id'] and mat['ref_id'] not in unique_materials:
            unique_materials[mat['ref_id']] = mat

    materials = list(unique_materials.values())
    print(f"After removing duplicates: {len(materials)} unique materials")

    # Write to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Material Name', 'Type', 'Reference ID', 'Description'])

        for mat in sorted(materials, key=lambda x: (x['type'], x['name'])):
            # Clean up description - remove EcoSpold prefix and truncate
            desc = mat['description']
            desc = re.sub(r'EcoSpold 2 \w+ exchange, ID = [a-f0-9-]+\s*', '', desc)
            desc = re.sub(r'Information:\s*', '', desc)
            desc = desc.replace('&', '').strip()

            # Truncate long descriptions
            if len(desc) > 300:
                desc = desc[:297] + '...'

            writer.writerow([
                mat['name'],
                mat['type'],
                mat['ref_id'],
                desc
            ])

    print(f"\nCSV file created: {output_file}")

    # Print summary
    flows = [m for m in materials if m['type'] == 'flow']
    processes = [m for m in materials if m['type'] == 'process']

    print(f"\nSummary:")
    print(f"  Flows: {len(flows)}")
    print(f"  Processes: {len(processes)}")

    print(f"\nSample materials:")
    for mat in materials[:10]:
        print(f"  [{mat['type']}] {mat['name']}")

if __name__ == "__main__":
    main()
