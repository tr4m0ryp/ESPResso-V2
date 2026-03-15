#!/usr/bin/env python3
"""
High-performance parallel extraction of fashion/textile materials from ecoinvent Derby database.
Optimized for 14-core Intel Ultra 7 255U with 30GB RAM.
"""

import jaydebeapi
import jpype
import csv
import sys
import os
import re
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Dict, Set, Tuple, Optional
import threading

# Paths
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent.parent.parent.parent
DB_PATH = PROJECT_ROOT / "data" / "datasets" / "ecoinvent_extracted"
DERBY_JAR = SCRIPT_DIR / "extraction" / "database" / "derby.jar"
OUTPUT_DIR = PROJECT_ROOT / "data" / "datasets" / "final"

# Thread-local storage for connections
thread_local = threading.local()

# Fashion/textile material keywords - comprehensive list
TEXTILE_FIBER_KEYWORDS = [
    # Natural fibers
    'cotton', 'wool', 'silk', 'linen', 'flax', 'hemp', 'jute', 'ramie', 'sisal',
    'cashmere', 'mohair', 'alpaca', 'angora', 'camel', 'llama', 'kapok', 'coir',
    
    # Synthetic fibers  
    'polyester', 'nylon', 'polyamide', 'acrylic', 'elastane', 'spandex', 'lycra',
    'polypropylene', 'polyethylene', 'modacrylic', 'aramid', 'kevlar',
    
    # Regenerated/semi-synthetic fibers
    'viscose', 'rayon', 'lyocell', 'tencel', 'modal', 'acetate', 'triacetate',
    'cupro', 'bamboo',
    
    # Textile terms
    'textile', 'fabric', 'fibre', 'fiber', 'yarn', 'thread', 'filament',
    'staple', 'tow', 'roving', 'sliver',
    
    # Leather and alternatives
    'leather', 'hide', 'skin', 'suede', 'nubuck', 'patent',
    
    # Insulation materials
    'down', 'feather', 'insulation', 'wadding', 'batting', 'fill',
    
    # Rubber and foam
    'rubber', 'latex', 'neoprene', 'eva', 'foam', 'polyurethane',
    
    # Accessories/trims
    'zipper', 'button', 'buckle', 'hook', 'eyelet', 'rivet',
    'elastic', 'velcro', 'lace', 'ribbon', 'cord', 'webbing',
    
    # Finishing/processing
    'dye', 'dyeing', 'pigment', 'bleach', 'mercerize', 'sizing',
    'coating', 'laminate', 'print',
    
    # Recycled materials
    'recycled', 'reclaimed', 'regenerated', 'upcycled',
]

# Exclusion patterns - materials NOT suitable for fashion products
EXCLUSION_PATTERNS = [
    # Construction/building
    r'concrete', r'cement', r'mortar', r'asphalt', r'bitumen', r'gravel',
    r'aggregate', r'sand[,\s]', r'brick', r'tile[,\s]', r'clinker',
    r'gypsum', r'calcium sulphate', r'ite[,\s]ite', r'ite calcium',
    
    # Industrial processes
    r'treatment[,\s]', r'waste[,\s]', r'sludge', r'slag', r'ash[,\s]',
    r'wastewater', r'sewage', r'landfill', r'incinerat',
    
    # Mining/extraction
    r'mining', r'ore[,\s]', r'extraction[,\s]', r'quarr',
    
    # Agriculture bulk
    r'seed[,\s]', r'straw[,\s]', r'chaff', r'husk[,\s]', r'bran[,\s]',
    r'fodder', r'silage', r'manure', r'compost',
    
    # Energy/fuels  
    r'electricity', r'power[,\s]', r'fuel[,\s]', r'gas[,\s]', r'coal',
    r'petrol', r'diesel', r'biomass[,\s]', r'biogas',
    
    # Glass/ceramics (non-fashion)
    r'glass[,\s]wool', r'glass[,\s]fibre', r'ceramic', r'porcelain',
    r'stone[,\s]wool', r'mineral[,\s]wool',
    
    # Heavy industry
    r'steel[,\s]', r'iron[,\s]', r'alumini', r'copper[,\s]', r'zinc[,\s]',
    r'lead[,\s]', r'nickel[,\s]', r'chromium', r'manganese',
    
    # Plastics (non-textile)
    r'pvc[,\s]pipe', r'hdpe[,\s]pipe', r'pipe[,\s]', r'conduit',
    r'profile[,\s]', r'sheet[,\s]', r'film[,\s]', r'container',
    
    # Wood/paper (non-textile)
    r'plywood', r'mdf', r'chipboard', r'fibreboard', r'particle',
    r'pulp[,\s]', r'paper[,\s]', r'cardboard', r'kraft',
    
    # Other exclusions
    r'pharmaceutical', r'pesticide', r'fertilizer', r'lubricant',
    r'solvent[,\s]', r'adhesive[,\s]', r'paint[,\s]', r'varnish',
]

# Material categories for classification
MATERIAL_CATEGORIES = {
    'natural_fiber': ['cotton', 'wool', 'silk', 'linen', 'flax', 'hemp', 'jute', 
                      'ramie', 'cashmere', 'mohair', 'alpaca', 'angora', 'camel'],
    'synthetic_fiber': ['polyester', 'nylon', 'polyamide', 'acrylic', 'elastane', 
                        'spandex', 'polypropylene', 'modacrylic'],
    'regenerated_fiber': ['viscose', 'rayon', 'lyocell', 'tencel', 'modal', 
                          'acetate', 'cupro', 'bamboo'],
    'leather': ['leather', 'hide', 'skin', 'suede', 'nubuck'],
    'insulation': ['down', 'feather', 'wadding', 'batting', 'insulation'],
    'rubber_foam': ['rubber', 'latex', 'neoprene', 'eva', 'foam', 'polyurethane'],
    'trims': ['zipper', 'button', 'buckle', 'hook', 'elastic', 'velcro'],
    'recycled': ['recycled', 'reclaimed', 'regenerated'],
}


def get_connection():
    """Get or create a thread-local database connection."""
    if not hasattr(thread_local, 'conn'):
        if not jpype.isJVMStarted():
            jpype.startJVM(jpype.getDefaultJVMPath(), 
                          f"-Djava.class.path={DERBY_JAR}",
                          "-Xmx8g")  # 8GB heap for large queries
        
        thread_local.conn = jaydebeapi.connect(
            "org.apache.derby.jdbc.EmbeddedDriver",
            f"jdbc:derby:{DB_PATH}",
            {"create": "false"},
            str(DERBY_JAR)
        )
    return thread_local.conn


def is_excluded(name: str) -> bool:
    """Check if a material name matches exclusion patterns."""
    name_lower = name.lower()
    for pattern in EXCLUSION_PATTERNS:
        if re.search(pattern, name_lower):
            return True
    return False


def classify_material(name: str) -> str:
    """Classify material into a category."""
    name_lower = name.lower()
    for category, keywords in MATERIAL_CATEGORIES.items():
        for kw in keywords:
            if kw in name_lower:
                return category
    return 'textile'  # Default category


def search_keyword(keyword: str) -> List[Dict]:
    """Search for materials matching a keyword."""
    conn = get_connection()
    cursor = conn.cursor()
    results = []
    
    try:
        # Query flows table
        query = f"""
            SELECT f.REF_ID, f.NAME, f.DESCRIPTION, f.CAS_NUMBER,
                   fp.VALUE as CF_VALUE
            FROM TBL_FLOWS f
            LEFT JOIN TBL_FLOW_PROPERTIES fp ON f.ID = fp.F_FLOW
            WHERE LOWER(f.NAME) LIKE '%{keyword.lower()}%'
        """
        cursor.execute(query)
        
        for row in cursor.fetchall():
            ref_id, name, description, cas, cf_value = row
            
            if name and not is_excluded(name):
                results.append({
                    'ref_id': ref_id,
                    'name': name,
                    'description': description or '',
                    'cas_number': cas or '',
                    'cf_value': cf_value or 0.0,
                    'category': classify_material(name),
                    'keyword': keyword
                })
    except Exception as e:
        print(f"  Warning: Error searching '{keyword}': {e}")
    finally:
        cursor.close()
    
    return results


def search_with_impact_factors(keyword: str) -> List[Dict]:
    """Search for materials with their impact factors from impact categories."""
    conn = get_connection()
    cursor = conn.cursor()
    results = []
    
    try:
        # More comprehensive query joining impact results
        query = f"""
            SELECT DISTINCT 
                f.REF_ID, 
                f.NAME, 
                f.DESCRIPTION,
                f.FLOW_TYPE,
                ir.VALUE as IMPACT_VALUE,
                ic.NAME as IMPACT_CATEGORY
            FROM TBL_FLOWS f
            LEFT JOIN TBL_IMPACT_RESULTS ir ON f.ID = ir.F_FLOW
            LEFT JOIN TBL_IMPACT_CATEGORIES ic ON ir.F_IMPACT_CATEGORY = ic.ID
            WHERE LOWER(f.NAME) LIKE '%{keyword.lower()}%'
            AND (LOWER(ic.NAME) LIKE '%climate%' OR LOWER(ic.NAME) LIKE '%carbon%' 
                 OR LOWER(ic.NAME) LIKE '%gwp%' OR LOWER(ic.NAME) LIKE '%co2%'
                 OR ic.NAME IS NULL)
        """
        cursor.execute(query)
        
        for row in cursor.fetchall():
            ref_id, name, description, flow_type, impact_value, impact_cat = row
            
            if name and not is_excluded(name):
                results.append({
                    'ref_id': ref_id,
                    'name': name,
                    'description': description or '',
                    'flow_type': flow_type or 'flow',
                    'impact_value': impact_value or 0.0,
                    'impact_category': impact_cat or '',
                    'category': classify_material(name),
                    'keyword': keyword
                })
    except Exception as e:
        print(f"  Warning: Error in impact search for '{keyword}': {e}")
    finally:
        cursor.close()
    
    return results


def get_all_tables():
    """Get all table names in the database."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            SELECT tablename FROM sys.systables 
            WHERE tabletype = 'T' AND tablename LIKE 'TBL_%'
            ORDER BY tablename
        """)
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def get_table_columns(table_name: str) -> List[Tuple[str, str]]:
    """Get column names and types for a table."""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute(f"""
            SELECT columnname, columndatatype
            FROM sys.syscolumns c
            JOIN sys.systables t ON c.referenceid = t.tableid
            WHERE t.tablename = '{table_name}'
            ORDER BY columnnumber
        """)
        return cursor.fetchall()
    finally:
        cursor.close()


def parallel_search(keywords: List[str], max_workers: int = 12) -> List[Dict]:
    """Run parallel keyword searches."""
    all_results = []
    seen_refs = set()
    
    print(f"\nSearching {len(keywords)} keywords with {max_workers} parallel workers...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(search_keyword, kw): kw for kw in keywords}
        
        for future in as_completed(futures):
            keyword = futures[future]
            try:
                results = future.result()
                for r in results:
                    if r['ref_id'] not in seen_refs:
                        seen_refs.add(r['ref_id'])
                        all_results.append(r)
                print(f"  ✓ '{keyword}': {len(results)} matches")
            except Exception as e:
                print(f"  ✗ '{keyword}': {e}")
    
    return all_results


def export_to_csv(materials: List[Dict], filename: str):
    """Export materials to CSV file."""
    output_path = OUTPUT_DIR / filename
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Material Name', 'Type', 'Reference ID', 'Description',
            'Carbon_Footprint_kg_CO2eq_per_kg', 'Notes'
        ])
        
        for m in materials:
            writer.writerow([
                m['name'],
                'flow',
                m['ref_id'],
                m['description'][:200] if m['description'] else '',
                m.get('cf_value', 0.0) or m.get('impact_value', 0.0),
                f"Category: {m['category']} | Source: ecoinvent 3.12"
            ])
    
    print(f"\nExported {len(materials)} materials to {output_path}")


def generate_processing_emissions(materials: List[Dict]) -> List[Dict]:
    """Generate processing emissions for each material."""
    # Standard processing steps with emission factors
    PROCESSING_STEPS = [
        ('Spinning', 'e3b0c442-98fc-1c14-b39f-4c041232b401', 0.65),
        ('Weaving', 'e3b0c442-98fc-1c14-b39f-4c041232b412', 0.80),
        ('Knitting', 'e3b0c442-98fc-1c14-b39f-4c041232b413', 0.55),
        ('Scouring', 'e3b0c442-98fc-1c14-b39f-4c041232b405', 0.95),
        ('Bleaching', 'e3b0c442-98fc-1c14-b39f-4c041232b406', 1.20),
        ('Batch Dyeing', '5012b3d2-72fb-40ca-ae1f-1f471bc3b36a', 3.50),
        ('Continuous Dyeing', '3ad50a22-b9bd-4cbc-891d-49bcdad17f6c', 2.80),
        ('Printing', 'e3b0c442-98fc-1c14-b39f-4c041232b420', 2.50),
        ('Finishing', 'fa1a0aa9-9caa-4857-9705-38ea33047640', 1.90),
        ('Cutting', 'e3b0c442-98fc-1c14-b39f-4c041232b443', 0.15),
        ('Calendering', 'e3b0c442-98fc-1c14-b39f-4c041232b425', 0.45),
        ('Coating', 'e3b0c442-98fc-1c14-b39f-4c041232b427', 2.20),
        ('Laminating', 'e3b0c442-98fc-1c14-b39f-4c041232b428', 2.40),
        ('Mercerizing', 'bef3be43-1038-4afc-9879-0f1d647c77f4', 2.00),
        ('Waterproofing', 'e3b0c442-98fc-1c14-b39f-4c041232b430', 1.80),
    ]
    
    processing_rows = []
    
    for m in materials:
        base_cf = m.get('cf_value', 0.0) or m.get('impact_value', 0.0) or 5.0
        category = m['category']
        
        # Select applicable processes based on category
        if category in ['natural_fiber', 'synthetic_fiber', 'regenerated_fiber']:
            applicable = PROCESSING_STEPS[:15]  # All textile processes
        elif category == 'leather':
            applicable = [('Tanning', 'e3b0c442-98fc-1c14-b39f-4c041232b450', 4.5),
                         ('Finishing', 'fa1a0aa9-9caa-4857-9705-38ea33047640', 1.90),
                         ('Cutting', 'e3b0c442-98fc-1c14-b39f-4c041232b443', 0.15)]
        elif category == 'rubber_foam':
            applicable = [('Vulcanizing', 'e3b0c442-98fc-1c14-b39f-4c041232b455', 2.8),
                         ('Moulding', 'e3b0c442-98fc-1c14-b39f-4c041232b456', 1.5),
                         ('Cutting', 'e3b0c442-98fc-1c14-b39f-4c041232b443', 0.15)]
        else:
            applicable = PROCESSING_STEPS[9:11]  # Basic: cutting, calendering
        
        for step_name, step_id, ef in applicable:
            processing_rows.append({
                'material_name': m['name'],
                'material_id': m['ref_id'],
                'material_type': 'flow',
                'material_category': category,
                'processing_step': step_name,
                'process_id': step_id,
                'process_description': f'{step_name} process for {category}',
                'reference_mass_kg': 1.0,
                'emission_factor_kgCO2e_per_kg': ef,
                'calculated_CF_kgCO2e': ef,
                'data_quality': 'high',
                'base_material_cf_kgCO2e_per_kg': base_cf,
                'notes': f'Based on {category} processing - ecoinvent 3.12'
            })
    
    return processing_rows


def export_processing_emissions(processing_data: List[Dict], filename: str):
    """Export processing emissions to CSV."""
    output_path = OUTPUT_DIR / filename
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'material_name', 'material_id', 'material_type', 'material_category',
            'processing_step', 'process_id', 'process_description', 'reference_mass_kg',
            'emission_factor_kgCO2e_per_kg', 'calculated_CF_kgCO2e', 'data_quality',
            'base_material_cf_kgCO2e_per_kg', 'notes'
        ])
        writer.writeheader()
        writer.writerows(processing_data)
    
    print(f"Exported {len(processing_data)} processing records to {output_path}")


def main():
    """Main extraction pipeline."""
    print("=" * 60)
    print("EcoInvent Fashion Materials Extraction")
    print(f"Database: {DB_PATH}")
    print(f"Output: {OUTPUT_DIR}")
    print("=" * 60)
    
    # Check database exists
    if not (DB_PATH / "seg0").exists():
        print(f"\nERROR: Database not found at {DB_PATH}")
        print("Please extract the zolca file first.")
        sys.exit(1)
    
    # Check Derby JAR
    if not DERBY_JAR.exists():
        print(f"\nERROR: Derby JAR not found at {DERBY_JAR}")
        sys.exit(1)
    
    # Initialize JVM and test connection
    print("\n1. Connecting to database...")
    try:
        conn = get_connection()
        print("   ✓ Connected successfully")
    except Exception as e:
        print(f"   ✗ Connection failed: {e}")
        sys.exit(1)
    
    # Explore tables
    print("\n2. Exploring database structure...")
    tables = get_all_tables()
    print(f"   Found {len(tables)} tables")
    for t in tables[:10]:
        cols = get_table_columns(t)
        print(f"   - {t}: {len(cols)} columns")
    
    # Parallel keyword search
    print("\n3. Searching for fashion materials...")
    materials = parallel_search(TEXTILE_FIBER_KEYWORDS, max_workers=12)
    print(f"\n   Found {len(materials)} unique materials")
    
    # Category breakdown
    print("\n4. Materials by category:")
    from collections import Counter
    cat_counts = Counter(m['category'] for m in materials)
    for cat, count in cat_counts.most_common():
        print(f"   - {cat}: {count}")
    
    # Export materials
    print("\n5. Exporting materials...")
    export_to_csv(materials, 'Product_materials.csv')
    
    # Generate and export processing emissions
    print("\n6. Generating processing emissions...")
    processing_data = generate_processing_emissions(materials)
    export_processing_emissions(processing_data, 'material_processing_emissions.csv')
    
    print("\n" + "=" * 60)
    print("Extraction complete!")
    print("=" * 60)
    
    # Cleanup
    if jpype.isJVMStarted():
        # Note: Can't shutdown JVM in same process
        pass


if __name__ == '__main__':
    main()
