#!/usr/bin/env python3
"""
Script to extract packaging materials and their emission factors from the ecoinvent database.
This script connects to the Derby database and extracts information about packaging materials.
"""

import os
import sys
import csv
import subprocess
import tempfile

def create_packaging_query():
    """Create SQL query to extract packaging materials from ecoinvent"""
    query = """
SELECT 
    f.NAME as flow_name,
    f.REF_ID as flow_id,
    f.CATEGORY as category,
    f.SUBCATEGORY as subcategory,
    f.LOCATION as location,
    f.UNIT as unit,
    f.SYNONYMS as synonyms,
    p.NAME as process_name,
    p.REF_ID as process_id,
    e.VALUE as emission_factor,
    e.UNIT as ef_unit,
    e.IS_INPUT as is_input
FROM FLOWS f
LEFT JOIN EXCHANGES e ON f.ID = e.FLOW_ID
LEFT JOIN PROCESSES p ON e.PROCESS_ID = p.ID
WHERE (
    LOWER(f.NAME) LIKE '%packag%'
    OR LOWER(f.CATEGORY) LIKE '%packag%'
    OR LOWER(f.SUBCATEGORY) LIKE '%packag%'
    OR LOWER(f.SYNONYMS) LIKE '%packag%'
    OR LOWER(p.NAME) LIKE '%packag%'
    OR LOWER(f.NAME) LIKE '%cardboard%'
    OR LOWER(f.NAME) LIKE '%plastic%'
    OR LOWER(f.NAME) LIKE '%paper%'
    OR LOWER(f.NAME) LIKE '%glass%'
    OR LOWER(f.NAME) LIKE '%metal%'
    OR LOWER(f.NAME) LIKE '%aluminium%'
    OR LOWER(f.NAME) LIKE '%steel%'
    OR LOWER(f.NAME) LIKE '%tin%'
    OR LOWER(f.NAME) LIKE '%wood%'
    OR LOWER(f.NAME) LIKE '%bag%'
    OR LOWER(f.NAME) LIKE '%film%'
    OR LOWER(f.NAME) LIKE '%bottle%'
    OR LOWER(f.NAME) LIKE '%container%'
    OR LOWER(f.NAME) LIKE '%box%'
    OR LOWER(f.NAME) LIKE '%carton%'
)
AND e.VALUE IS NOT NULL
ORDER BY f.CATEGORY, f.NAME;
"""
    return query

def run_derby_query(query, db_path):
    """Run SQL query using Derby ij tool"""
    try:
        # Create temporary file for the query
        with tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False) as f:
            f.write(query)
            query_file = f.name
        
        # Run Derby ij command
        ij_command = [
            'java', '-cp', 'derbytools.jar:derby.jar',
            'org.apache.derby.tools.ij',
            query_file
        ]
        
        # Set working directory to where the database files are
        result = subprocess.run(
            ij_command,
            cwd=db_path,
            capture_output=True,
            text=True,
            timeout=300
        )
        
        # Clean up temporary file
        os.unlink(query_file)
        
        if result.returncode != 0:
            print(f"Error running Derby query: {result.stderr}")
            return None
            
        return result.stdout
        
    except Exception as e:
        print(f"Exception running Derby query: {e}")
        return None

def parse_derby_output(output_text):
    """Parse the output from Derby ij into structured data"""
    lines = output_text.strip().split('\n')
    data = []
    
    # Find the start of the actual data (skip header lines)
    data_start = False
    for line in lines:
        line = line.strip()
        if not line:
            continue
            
        # Look for the header separator (row of dashes)
        if 'FLOW_NAME' in line:
            data_start = True
            continue
            
        if data_start and line.startswith('---'):
            continue
            
        if data_start and '|' in line:
            # Split by pipe and clean up
            fields = [field.strip() for field in line.split('|')]
            if len(fields) >= 10:  # Ensure we have enough fields
                data.append({
                    'flow_name': fields[0],
                    'flow_id': fields[1],
                    'category': fields[2],
                    'subcategory': fields[3],
                    'location': fields[4],
                    'unit': fields[5],
                    'synonyms': fields[6],
                    'process_name': fields[7],
                    'process_id': fields[8],
                    'emission_factor': fields[9],
                    'ef_unit': fields[10] if len(fields) > 10 else '',
                    'is_input': fields[11] if len(fields) > 11 else ''
                })
    
    return data

def save_to_csv(data, filename):
    """Save the extracted data to CSV file"""
    if not data:
        print("No data to save")
        return
        
    fieldnames = ['flow_name', 'flow_id', 'category', 'subcategory', 'location', 
                  'unit', 'synonyms', 'process_name', 'process_id', 'emission_factor', 
                  'ef_unit', 'is_input']
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Data saved to {filename}")

def main():
    """Main function to extract packaging materials data"""
    print("Extracting packaging materials from ecoinvent database...")
    
    # Paths
    db_path = "/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation"
    output_file = "/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials_raw.csv"
    
    # Create and run query
    query = create_packaging_query()
    print("Running Derby query...")
    
    output = run_derby_query(query, db_path)
    if not output:
        print("Failed to get data from database")
        return
    
    print("Parsing output...")
    data = parse_derby_output(output)
    
    if data:
        print(f"Extracted {len(data)} records")
        save_to_csv(data, output_file)
    else:
        print("No data extracted")
        # Save raw output for debugging
        with open("/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_raw_output.txt", "w") as f:
            f.write(output)
        print("Raw output saved to packaging_raw_output.txt")

if __name__ == "__main__":
    main()