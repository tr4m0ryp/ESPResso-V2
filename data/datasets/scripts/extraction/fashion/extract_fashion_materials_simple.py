#!/usr/bin/env python3
"""
Alternative script to extract fashion materials using olca library
or by reading the database structure directly.
"""

import subprocess
import csv
import json
from pathlib import Path

# Database path
DB_PATH = Path(__file__).parent.absolute()

# Fashion keywords
FASHION_KEYWORDS = [
    'cotton', 'polyester', 'nylon', 'wool', 'silk', 'linen', 'rayon',
    'viscose', 'acrylic', 'spandex', 'elastane', 'leather', 'textile',
    'fabric', 'fibre', 'fiber', 'yarn', 'thread', 'dye', 'dyeing',
    'spinning', 'weaving', 'knitting'
]

def try_derby_ij_query():
    """Try using Derby ij tool to query the database."""

    # SQL query to get table names
    sql_query = """
    connect 'jdbc:derby:.';
    SELECT tablename FROM sys.systables WHERE tabletype = 'T';
    exit;
    """

    try:
        # Write SQL to temp file
        sql_file = DB_PATH / "temp_query.sql"
        with open(sql_file, 'w') as f:
            f.write(sql_query)

        # Try to run ij
        result = subprocess.run(
            ['ij', sql_file],
            cwd=DB_PATH,
            capture_output=True,
            text=True,
            timeout=10
        )

        print("Derby ij output:")
        print(result.stdout)
        print(result.stderr)

        sql_file.unlink()  # Clean up

    except FileNotFoundError:
        print("Derby ij tool not found")
    except Exception as e:
        print(f"Error running ij: {e}")

def manual_search_database():
    """
    Manually search through database files for fashion-related terms.
    This is a fallback approach that searches through the data files directly.
    """

    print("Searching database files for fashion-related terms...")

    results = []
    seg0_path = DB_PATH / "seg0"

    if not seg0_path.exists():
        print("Error: seg0 directory not found")
        return results

    # Read through .dat files looking for our keywords
    for dat_file in seg0_path.glob("*.dat"):
        try:
            with open(dat_file, 'rb') as f:
                content = f.read()
                # Convert to string, ignoring errors
                try:
                    text = content.decode('utf-8', errors='ignore')
                except:
                    text = content.decode('latin-1', errors='ignore')

                # Search for keywords
                for keyword in FASHION_KEYWORDS:
                    if keyword.lower() in text.lower():
                        # Extract context around the keyword
                        idx = text.lower().find(keyword.lower())
                        context_start = max(0, idx - 100)
                        context_end = min(len(text), idx + 200)
                        context = text[context_start:context_end]

                        # Clean up the context
                        context = ' '.join(context.split())

                        if context and len(context) > 10:
                            results.append({
                                'keyword': keyword,
                                'file': dat_file.name,
                                'context': context
                            })
        except Exception as e:
            # Skip files we can't read
            pass

    return results

def main():
    print("=" * 70)
    print("Fashion Materials Extraction Tool")
    print("=" * 70)

    # Try Derby ij first
    print("\n1. Trying Derby ij tool...")
    try_derby_ij_query()

    # Manual search as fallback
    print("\n2. Performing manual search through database files...")
    results = manual_search_database()

    if results:
        print(f"\nFound {len(results)} potential matches")

        # Write to CSV
        output_file = DB_PATH / "fashion_materials_raw_search.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Keyword', 'Database File', 'Context'])
            for r in results:
                writer.writerow([r['keyword'], r['file'], r['context']])

        print(f"Results saved to: {output_file}")

        # Also save unique keywords found
        unique_keywords = sorted(set(r['keyword'] for r in results))
        print(f"\nKeywords found: {', '.join(unique_keywords)}")
    else:
        print("\nNo results found with manual search")

    print("\n" + "=" * 70)

if __name__ == "__main__":
    main()
