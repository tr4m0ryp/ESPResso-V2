#!/usr/bin/env python3
"""
Script to analyze packaging materials and calculate emission factors.
This script processes the raw data and creates a categorized list of packaging materials.
"""

import pandas as pd
import numpy as np
import re
from collections import defaultdict

def categorize_packaging_material(material_name, category, subcategory, synonyms):
    """Categorize packaging materials based on their names and categories"""
    name_lower = str(material_name).lower()
    category_lower = str(category).lower()
    subcategory_lower = str(subcategory).lower()
    synonyms_lower = str(synonyms).lower()
    
    # Define packaging material categories
    if any(word in name_lower for word in ['plastic', 'polyethylene', 'polypropylene', 'polystyrene', 'pvc', 'pet']):
        return 'Plastic'
    elif any(word in name_lower for word in ['paper', 'cardboard', 'carton', 'kraft']):
        return 'Paper/Cardboard'
    elif any(word in name_lower for word in ['glass', 'bottle']):
        return 'Glass'
    elif any(word in name_lower for word in ['aluminium', 'aluminum', 'steel', 'tin', 'metal']):
        return 'Metal'
    elif any(word in name_lower for word in ['wood', 'timber', 'pallet']):
        return 'Wood'
    elif any(word in name_lower for word in ['bag', 'film', 'wrap', 'foil']):
        # Check for material type in broader context
        if 'plastic' in category_lower or 'plastic' in subcategory_lower:
            return 'Plastic'
        elif 'paper' in category_lower or 'paper' in subcategory_lower:
            return 'Paper/Cardboard'
        elif 'alumin' in category_lower or 'alumin' in subcategory_lower:
            return 'Metal'
        else:
            return 'Flexible packaging (unspecified)'
    elif any(word in name_lower for word in ['container', 'box', 'packag']):
        # Generic packaging - try to determine material from category
        if 'plastic' in category_lower:
            return 'Plastic'
        elif 'paper' in category_lower or 'cardboard' in category_lower:
            return 'Paper/Cardboard'
        elif 'glass' in category_lower:
            return 'Glass'
        elif 'metal' in category_lower:
            return 'Metal'
        else:
            return 'General packaging (unspecified)'
    else:
        return 'Other/Unspecified'

def extract_emission_factor(value_str):
    """Extract numeric emission factor from string"""
    if pd.isna(value_str) or value_str == '':
        return None
    
    try:
        # Remove common units and extract number
        cleaned = str(value_str).strip()
        # Extract first number found
        numbers = re.findall(r'[-+]?[0-9]*\.?[0-9]+(?:[eE][-+]?[0-9]+)?', cleaned)
        if numbers:
            return float(numbers[0])
    except:
        pass
    return None

def analyze_packaging_materials(input_file):
    """Analyze packaging materials from the raw data"""
    print("Loading packaging materials data...")
    
    try:
        df = pd.read_csv(input_file)
        print(f"Loaded {len(df)} records")
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    # Clean and categorize data
    print("Categorizing packaging materials...")
    df['material_category'] = df.apply(
        lambda row: categorize_packaging_material(
            row['name'], row.get('category', ''), row.get('description', ''), ''
        ), axis=1
    )
    
    # Extract numeric emission factors
    print("Processing emission factors...")
    df['ef_numeric'] = df['emission_factor'].apply(extract_emission_factor)
    
    # Filter out records with invalid emission factors
    valid_df = df[df['ef_numeric'].notna()].copy()
    print(f"Found {len(valid_df)} records with valid emission factors")
    
    # Group by material category and calculate statistics
    print("Calculating emission factor statistics by material category...")
    category_stats = valid_df.groupby('material_category').agg({
        'ef_numeric': ['count', 'mean', 'median', 'std', 'min', 'max'],
        'flow_name': 'count'
    }).round(6)
    
    # Flatten column names
    category_stats.columns = ['_'.join(col).strip() for col in category_stats.columns.values]
    category_stats = category_stats.reset_index()
    
    # Create summary of unique materials by category
    material_summary = []
    for category in valid_df['material_category'].unique():
        cat_data = valid_df[valid_df['material_category'] == category]
        unique_materials = cat_data['flow_name'].unique()
        
        material_summary.append({
            'material_category': category,
            'count': len(cat_data),
            'unique_materials': len(unique_materials),
            'avg_emission_factor': cat_data['ef_numeric'].mean(),
            'median_emission_factor': cat_data['ef_numeric'].median(),
            'min_emission_factor': cat_data['ef_numeric'].min(),
            'max_emission_factor': cat_data['ef_numeric'].max(),
            'example_materials': '; '.join(unique_materials[:5])  # First 5 examples
        })
    
    summary_df = pd.DataFrame(material_summary)
    
    return valid_df, category_stats, summary_df

def create_packaging_database(analysis_df, summary_df, output_prefix):
    """Create a comprehensive packaging materials database"""
    
    # Save detailed analysis
    analysis_file = f"{output_prefix}_detailed.csv"
    analysis_df.to_csv(analysis_file, index=False)
    print(f"Detailed analysis saved to {analysis_file}")
    
    # Save category summary
    summary_file = f"{output_prefix}_summary.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"Category summary saved to {summary_file}")
    
    # Create emission factors reference table
    ef_reference = analysis_df.groupby(['material_category', 'unit']).agg({
        'ef_numeric': ['mean', 'median', 'std', 'count'],
        'flow_name': 'first'
    }).round(6)
    
    ef_reference.columns = ['_'.join(col).strip() for col in ef_reference.columns.values]
    ef_reference = ef_reference.reset_index()
    
    ef_file = f"{output_prefix}_emission_factors.csv"
    ef_reference.to_csv(ef_file, index=False)
    print(f"Emission factors reference saved to {ef_file}")
    
    return analysis_file, summary_file, ef_file

def generate_packaging_report(summary_df, output_file):
    """Generate a text report of packaging materials analysis"""
    with open(output_file, 'w') as f:
        f.write("PACKAGING MATERIALS ANALYSIS REPORT\n")
        f.write("=" * 50 + "\n\n")
        
        f.write("SUMMARY BY MATERIAL CATEGORY:\n")
        f.write("-" * 30 + "\n")
        
        for _, row in summary_df.iterrows():
            f.write(f"\n{row['material_category']}:\n")
            f.write(f"  Total records: {row['count']}\n")
            f.write(f"  Unique materials: {row['unique_materials']}\n")
            f.write(f"  Average emission factor: {row['avg_emission_factor']:.6f}\n")
            f.write(f"  Median emission factor: {row['median_emission_factor']:.6f}\n")
            f.write(f"  Range: {row['min_emission_factor']:.6f} - {row['max_emission_factor']:.6f}\n")
            f.write(f"  Example materials: {row['example_materials']}\n")
        
        f.write("\n\nRECOMMENDED EMISSION FACTORS FOR CALCULATIONS:\n")
        f.write("-" * 50 + "\n")
        
        for _, row in summary_df.iterrows():
            f.write(f"{row['material_category']}: {row['median_emission_factor']:.6f} kg CO2-eq/{row.get('unit', 'kg')}\n")
    
    print(f"Report saved to {output_file}")

def main():
    """Main function to analyze packaging materials"""
    input_file = "/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials_extracted.csv"
    output_prefix = "/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials"
    
    # Analyze data
    analysis_df, category_stats, summary_df = analyze_packaging_materials(input_file)
    
    if analysis_df is not None:
        # Create packaging database files
        create_packaging_database(analysis_df, summary_df, output_prefix)
        
        # Generate report
        generate_packaging_report(summary_df, f"{output_prefix}_report.txt")
        
        print("\nAnalysis complete!")
        print(f"Total packaging materials analyzed: {len(analysis_df)}")
        print(f"Material categories identified: {len(summary_df)}")
        
        # Print summary to console
        print("\nSUMMARY BY MATERIAL CATEGORY:")
        print(summary_df[['material_category', 'count', 'avg_emission_factor', 'median_emission_factor']].to_string(index=False))
    else:
        print("Analysis failed - no data to process")

if __name__ == "__main__":
    main()