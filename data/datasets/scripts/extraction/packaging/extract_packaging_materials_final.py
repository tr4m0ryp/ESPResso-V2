#!/usr/bin/env python3
"""
Final script to extract packaging materials from the fashion materials output
and get their emission factors from the exchanges data.
"""

import re
import pandas as pd
import csv

def extract_packaging_from_fashion_output():
    """Extract packaging materials from the fashion materials output file"""
    
    packaging_materials = []
    
    # Read the fashion materials output
    with open('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/fashion_materials_output.txt', 'r') as f:
        content = f.read()
    
    # Split into lines and process
    lines = content.split('\n')
    current_section = None
    
    for line in lines:
        # Skip header and separator lines
        if 'NAME' in line or '---' in line or line.strip() == '':
            continue
            
        # Look for data lines with packaging terms
        if '|' in line and not line.startswith('--') and not line.startswith('ij>'):
            parts = line.split('|')
            if len(parts) >= 3:
                name = parts[0].strip()
                ref_id = parts[1].strip()
                description = parts[2].strip() if len(parts) > 2 else ''
                
                # Check if this is a packaging material
                packaging_terms = [
                    'packaging', 'container', 'bottle', 'carton', 'box', 'bag', 
                    'wrap', 'film', 'foil', 'pallet', 'crate', 'can', 'jar',
                    'cardboard', 'kraft', 'paperboard', 'corrugated', 'disposable',
                    'single use', 'food packaging'
                ]
                
                if any(term in name.lower() for term in packaging_terms):
                    packaging_materials.append({
                        'name': name,
                        'ref_id': ref_id,
                        'description': description,
                        'category': 'packaging'
                    })
    
    return packaging_materials

def search_for_packaging_materials():
    """Search for additional packaging materials using broader terms"""
    
    packaging_materials = []
    
    # Read the fashion materials output
    with open('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/fashion_materials_output.txt', 'r') as f:
        content = f.read()
    
    # Look for basic packaging materials
    basic_materials = [
        'polyethylene', 'polypropylene', 'polystyrene', 'pvc', 'pet',
        'paper', 'cardboard', 'glass', 'aluminium', 'steel', 'tin', 'wood'
    ]
    
    lines = content.split('\n')
    
    for line in lines:
        # Skip header and separator lines
        if 'NAME' in line or '---' in line or line.strip() == '':
            continue
            
        # Look for data lines with basic material terms
        if '|' in line and not line.startswith('--') and not line.startswith('ij>'):
            parts = line.split('|')
            if len(parts) >= 3:
                name = parts[0].strip()
                ref_id = parts[1].strip()
                description = parts[2].strip() if len(parts) > 2 else ''
                
                # Check if this is a basic packaging material
                for material in basic_materials:
                    if material in name.lower() and 'production' not in name.lower():
                        # Additional check for packaging context
                        if any(term in name.lower() for term in ['film', 'sheet', 'container', 'bottle', 'wrap', 'bag', 'box']):
                            packaging_materials.append({
                                'name': name,
                                'ref_id': ref_id,
                                'description': description,
                                'category': material.title()
                            })
                            break
    
    return packaging_materials

def extract_emission_factors():
    """Extract emission factors from the exchanges section"""
    
    emission_factors = []
    
    # Read the fashion materials output
    with open('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/fashion_materials_output.txt', 'r') as f:
        content = f.read()
    
    # Find the exchanges section
    exchanges_pattern = r'-- Query for fashion materials in exchanges.*?$(.*?)(?=disconnect)'
    exchanges_match = re.search(exchanges_pattern, content, re.DOTALL | re.MULTILINE)
    
    if exchanges_match:
        exchanges_section = exchanges_match.group(1)
        lines = exchanges_section.split('\n')
        
        for line in lines:
            # Skip header and separator lines
            if 'FLOW_NAME' in line or '---' in line or line.strip() == '':
                continue
                
            # Look for data lines
            if '|' in line and not line.startswith('--') and not line.startswith('ij>'):
                parts = line.split('|')
                if len(parts) >= 7:  # We need at least flow_name, flow_ref_id, process_name, process_ref_id, exchange_value, exchange_unit, is_input
                    flow_name = parts[0].strip()
                    flow_ref_id = parts[1].strip()
                    process_name = parts[2].strip()
                    process_ref_id = parts[3].strip()
                    exchange_value = parts[4].strip()
                    exchange_unit = parts[5].strip()
                    is_input = parts[6].strip()
                    
                    # Check if this is a packaging-related exchange
                    packaging_terms = [
                        'packaging', 'container', 'bottle', 'carton', 'box', 'bag', 
                        'wrap', 'film', 'foil', 'pallet', 'crate', 'can', 'jar',
                        'cardboard', 'kraft', 'paperboard', 'corrugated', 'disposable',
                        'single use', 'food packaging', 'polyethylene', 'polypropylene',
                        'polystyrene', 'pvc', 'pet', 'paper', 'glass', 'aluminium'
                    ]
                    
                    if any(term in flow_name.lower() for term in packaging_terms):
                        try:
                            value = float(exchange_value) if exchange_value else 0.0
                            emission_factors.append({
                                'flow_name': flow_name,
                                'flow_ref_id': flow_ref_id,
                                'process_name': process_name,
                                'process_ref_id': process_ref_id,
                                'exchange_value': value,
                                'exchange_unit': exchange_unit,
                                'is_input': is_input
                            })
                        except ValueError:
                            continue
    
    return emission_factors

def create_packaging_database():
    """Create a comprehensive packaging materials database"""
    
    print("Extracting packaging materials from fashion data...")
    
    # Extract packaging materials
    packaging_materials = extract_packaging_from_fashion_output()
    basic_materials = search_for_packaging_materials()
    emission_factors = extract_emission_factors()
    
    # Combine all materials
    all_materials = packaging_materials + basic_materials
    
    # Remove duplicates based on ref_id
    unique_materials = {}
    for material in all_materials:
        if material['ref_id'] not in unique_materials:
            unique_materials[material['ref_id']] = material
    
    final_materials = list(unique_materials.values())
    
    print(f"Found {len(final_materials)} unique packaging materials")
    print(f"Found {len(emission_factors)} emission factor records")
    
    # Save materials to CSV
    if final_materials:
        with open('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials_extracted.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['name', 'ref_id', 'description', 'category']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(final_materials)
        
        print(f"Packaging materials saved to packaging_materials_extracted.csv")
    
    # Save emission factors to CSV
    if emission_factors:
        with open('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_emission_factors.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['flow_name', 'flow_ref_id', 'process_name', 'process_ref_id', 'exchange_value', 'exchange_unit', 'is_input']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(emission_factors)
        
        print(f"Emission factors saved to packaging_emission_factors.csv")
    
    # Create a summary report
    with open('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_extraction_report.txt', 'w') as f:
        f.write("PACKAGING MATERIALS EXTRACTION REPORT\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Total packaging materials found: {len(final_materials)}\n")
        f.write(f"Total emission factor records: {len(emission_factors)}\n\n")
        
        if final_materials:
            f.write("Sample packaging materials:\n")
            f.write("-" * 30 + "\n")
            for i, material in enumerate(final_materials[:15]):
                f.write(f"{i+1}. {material['name']}\n")
            
            f.write(f"\n... and {len(final_materials) - 15} more materials\n")
        
        if emission_factors:
            f.write("\nSample emission factors:\n")
            f.write("-" * 30 + "\n")
            for i, ef in enumerate(emission_factors[:10]):
                f.write(f"{i+1}. {ef['flow_name']}: {ef['exchange_value']} {ef['exchange_unit']}\n")
    
    return final_materials, emission_factors

if __name__ == "__main__":
    materials, factors = create_packaging_database()
    print("Extraction complete!")