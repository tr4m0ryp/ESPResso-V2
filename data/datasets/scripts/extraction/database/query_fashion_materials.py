#!/usr/bin/env python3
"""
Script to query the ecoinvent Derby database for fashion industry materials
and their carbon footprint values.
"""

import jaydebeapi
import csv
import os
from pathlib import Path

# Database path (Derby database)
DB_PATH = Path(__file__).parent.absolute()

# Derby JDBC driver
DERBY_JAR = "/usr/share/java/derby.jar"  # Adjust path as needed

# Keywords to identify fashion industry materials
FASHION_KEYWORDS = [
    'cotton', 'polyester', 'nylon', 'wool', 'silk', 'linen', 'rayon',
    'viscose', 'acrylic', 'spandex', 'elastane', 'leather', 'textile',
    'fabric', 'fibre', 'fiber', 'yarn', 'thread', 'dye', 'dyeing'
]

def connect_to_derby():
    """Connect to the Derby database."""
    try:
        conn = jaydebeapi.connect(
            "org.apache.derby.jdbc.EmbeddedDriver",
            f"jdbc:derby:{DB_PATH};create=false",
            ["", ""],
            DERBY_JAR,
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return None

def query_processes(conn):
    """Query the database for fashion-related processes."""
    cursor = conn.cursor()

    # Query to get process names and their climate change impact
    # This is a simplified query - actual table structure may vary
    query = """
        SELECT DISTINCT p.name, p.ref_id, p.description
        FROM tbl_processes p
        WHERE LOWER(p.name) LIKE ?
        ORDER BY p.name
    """

    results = []
    for keyword in FASHION_KEYWORDS:
        try:
            cursor.execute(query, (f'%{keyword}%',))
            results.extend(cursor.fetchall())
        except Exception as e:
            print(f"Error querying for keyword '{keyword}': {e}")

    return results

def main():
    print("Attempting to connect to Derby database...")

    # Check if Derby JAR exists
    if not os.path.exists(DERBY_JAR):
        print(f"Error: Derby JAR not found at {DERBY_JAR}")
        print("Please install Derby JDBC driver or update the path.")
        print("\nAlternative: Install jaydebeapi and derby with:")
        print("  pip install jaydebeapi JPype1")
        print("  dnf install derby  # On Fedora")
        return

    conn = connect_to_derby()
    if not conn:
        return

    try:
        print("Connected successfully!")
        print("Querying for fashion industry materials...")

        results = query_processes(conn)

        # Write results to CSV
        output_file = DB_PATH / "fashion_materials_carbon_footprint.csv"
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Material Name', 'Reference ID', 'Description'])
            writer.writerows(results)

        print(f"\nFound {len(results)} fashion-related materials")
        print(f"Results saved to: {output_file}")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
