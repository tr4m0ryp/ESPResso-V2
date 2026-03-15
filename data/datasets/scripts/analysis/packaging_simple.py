#!/usr/bin/env python3
"""
Simple analysis script for packaging materials based on extracted data.
"""

import pandas as pd
import numpy as np

def categorize_packaging_material(material_name, description, category):
    """Categorize packaging materials based on their names and descriptions"""
    name_lower = str(material_name).lower()
    desc_lower = str(description).lower()
    
    # Define packaging material categories
    if any(word in name_lower for word in ['paper', 'cardboard', 'carton', 'kraft']):
        return 'Paper/Cardboard'
    elif any(word in name_lower for word in ['plastic', 'polyethylene', 'polypropylene', 'polystyrene', 'pvc', 'pet']):
        return 'Plastic'
    elif any(word in name_lower for word in ['glass', 'bottle']):
        return 'Glass'
    elif any(word in name_lower for word in ['aluminium', 'aluminum', 'steel', 'tin', 'metal']):
        return 'Metal'
    elif any(word in name_lower for word in ['wood', 'timber', 'pallet']):
        return 'Wood'
    elif any(word in name_lower for word in ['bag', 'film', 'wrap', 'foil', 'container', 'box', 'crate', 'can', 'jar']):
        # Generic packaging - try to determine material from description
        if 'paper' in desc_lower:
            return 'Paper/Cardboard'
        elif 'plastic' in desc_lower:
            return 'Plastic'
        elif 'glass' in desc_lower:
            return 'Glass'
        elif 'metal' in desc_lower:
            return 'Metal'
        else:
            return 'General packaging (unspecified)'
    else:
        return 'Other/Unspecified'

def analyze_packaging_materials():
    """Analyze the extracted packaging materials"""
    
    print("Loading packaging materials data...")
    
    try:
        df = pd.read_csv('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials_extracted.csv')
        print(f"Loaded {len(df)} records")
    except Exception as e:
        print(f"Error loading data: {e}")
        return None
    
    # Clean and categorize data
    print("Categorizing packaging materials...")
    df['material_category'] = df.apply(
        lambda row: categorize_packaging_material(
            row['name'], row.get('description', ''), row.get('category', '')
        ), axis=1
    )
    
    # Group by material category and create summary
    print("Creating summary by material category...")
    category_summary = df.groupby('material_category').agg({
        'name': 'count',
        'ref_id': 'nunique'
    }).reset_index()
    
    category_summary.columns = ['material_category', 'count', 'unique_ids']
    
    # Add example materials for each category
    examples = []
    for category in category_summary['material_category']:
        category_materials = df[df['material_category'] == category]['name'].head(3).tolist()
        examples.append('; '.join(category_materials))
    
    category_summary['example_materials'] = examples
    
    return df, category_summary

def create_packaging_report(analysis_df, summary_df):
    """Generate a comprehensive packaging materials report"""
    
    with open('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials_report.txt', 'w') as f:
        f.write("PACKAGING MATERIALS ANALYSIS REPORT\n")
        f.write("=" * 50 + "\n\n")
        
        f.write(f"Total packaging materials analyzed: {len(analysis_df)}\n")
        f.write(f"Material categories identified: {len(summary_df)}\n\n")
        
        f.write("SUMMARY BY MATERIAL CATEGORY:\n")
        f.write("-" * 30 + "\n")
        
        for _, row in summary_df.iterrows():
            f.write(f"\n{row['material_category']}:\n")
            f.write(f"  Count: {row['count']}\n")
            f.write(f"  Example materials: {row['example_materials']}\n")
        
        f.write("\n\nDETAILED MATERIAL LIST:\n")
        f.write("-" * 30 + "\n")
        
        for category in summary_df['material_category']:
            f.write(f"\n{category}:\n")
            category_materials = analysis_df[analysis_df['material_category'] == category]
            for _, material in category_materials.iterrows():
                f.write(f"  - {material['name']}\n")
    
    print(f"Report saved to packaging_materials_report.txt")

def create_sample_emission_factors():
    """Create sample emission factors based on typical values from literature"""
    
    # Sample emission factors (kg CO2-eq/kg of material)
    # These are typical values and should be replaced with actual ecoinvent data
    sample_factors = {
        'Paper/Cardboard': 1.3,  # kg CO2-eq/kg
        'Plastic': 3.5,         # kg CO2-eq/kg
        'Glass': 1.1,           # kg CO2-eq/kg
        'Metal': 8.2,           # kg CO2-eq/kg (average of aluminium, steel, etc.)
        'Wood': 0.5,            # kg CO2-eq/kg
        'General packaging (unspecified)': 2.0,  # kg CO2-eq/kg
        'Other/Unspecified': 2.5,  # kg CO2-eq/kg
    }
    
    # Save sample emission factors
    factors_df = pd.DataFrame([
        {'material_category': category, 'emission_factor': factor, 'unit': 'kg CO2-eq/kg'}
        for category, factor in sample_factors.items()
    ])
    
    factors_df.to_csv('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_sample_emission_factors.csv', index=False)
    print(f"Sample emission factors saved to packaging_sample_emission_factors.csv")
    
    return factors_df

def main():
    """Main function to analyze packaging materials"""
    
    # Analyze materials
    analysis_df, summary_df = analyze_packaging_materials()
    
    if analysis_df is not None:
        # Create report
        create_packaging_report(analysis_df, summary_df)
        
        # Create sample emission factors
        emission_factors_df = create_sample_emission_factors()
        
        # Save detailed analysis
        analysis_df.to_csv('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials_detailed.csv', index=False)
        summary_df.to_csv('/home/tr4moryp/Projects/Carbo_footprint_model/Dataset_analysation/packaging_materials_summary.csv', index=False)
        
        print("\nAnalysis complete!")
        print(f"Total packaging materials analyzed: {len(analysis_df)}")
        print(f"Material categories identified: {len(summary_df)}")
        
        # Print summary to console
        print("\nSUMMARY BY MATERIAL CATEGORY:")
        print(summary_df[['material_category', 'count', 'example_materials']].to_string(index=False))
        
        print(f"\nSample emission factors (kg CO2-eq/kg):")
        for _, row in emission_factors_df.iterrows():
            print(f"  {row['material_category']}: {row['emission_factor']}")
            
    else:
        print("Analysis failed - no data to process")

if __name__ == "__main__":
    main()