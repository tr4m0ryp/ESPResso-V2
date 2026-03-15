#!/usr/bin/env python3
"""
Query ecoinvent Derby database for fashion industry materials and their carbon footprints.
"""

import jaydebeapi
import jpype
import csv
import sys
from pathlib import Path

# Database and Derby JAR paths
DB_PATH = str(Path(__file__).parent.absolute())
DERBY_JAR = str(Path(__file__).parent / "derby.jar")

# Fashion-related keywords
FASHION_KEYWORDS = [
    'cotton', 'polyester', 'nylon', 'wool', 'silk', 'linen', 'rayon',
    'viscose', 'acrylic', 'spandex', 'elastane', 'leather', 'textile',
    'fabric', 'fibre', 'fiber', 'yarn', 'thread', 'dye', 'spinning',
    'weaving', 'knitting'
]

def connect_db():
    """Connect to the Derby database."""
    try:
        # Start JVM if not already started
        if not jpype.isJVMStarted():
            print("Starting JVM...")
            jpype.startJVM(jpype.getDefaultJVMPath(), f"-Djava.class.path={DERBY_JAR}")
            print(" JVM started")

        print("Connecting to Derby database...")
        conn = jaydebeapi.connect(
            "org.apache.derby.jdbc.EmbeddedDriver",
            f"jdbc:derby:{DB_PATH}",
            {"create": "false"},
            DERBY_JAR
        )
        print(" Connected successfully!")
        return conn
    except Exception as e:
        print(f" Connection error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def get_tables(conn):
    """Get list of all tables in the database."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT tablename
            FROM sys.systables
            WHERE tabletype = 'T' AND tablename LIKE 'TBL_%'
            ORDER BY tablename
        """)
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    except Exception as e:
        print(f"Error getting tables: {e}")
        return []
    finally:
        cursor.close()

def explore_table_structure(conn, table_name):
    """Get column information for a table."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT columnname, columndatatype
            FROM sys.syscolumns c
            JOIN sys.systables t ON c.referenceid = t.tableid
            WHERE t.tablename = '{table_name}'
            ORDER BY columnnumber
        """)
        columns = cursor.fetchall()
        return columns
    except Exception as e:
        print(f"Error exploring {table_name}: {e}")
        return []
    finally:
        cursor.close()

def query_fashion_materials(conn):
    """Query for fashion-related materials and their impacts."""
    cursor = conn.cursor()
    results = []

    try:
        # First, let's query the TBL_FLOWS table for fashion materials
        print("\nSearching TBL_FLOWS for fashion materials...")
        for keyword in FASHION_KEYWORDS:
            query = f"""
                SELECT DISTINCT name, ref_id, description, cas_number
                FROM TBL_FLOWS
                WHERE LOWER(name) LIKE '%{keyword}%'
                ORDER BY name
            """
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    results.append({
                        'type': 'flow',
                        'name': row[0],
                        'ref_id': row[1],
                        'description': row[2] if row[2] else '',
                        'cas_number': row[3] if row[3] else '',
                        'keyword': keyword
                    })
            except Exception as e:
                print(f"  Error querying flows for '{keyword}': {e}")

        # Query TBL_PROCESSES for fashion-related processes
        print("Searching TBL_PROCESSES for fashion materials...")
        for keyword in FASHION_KEYWORDS:
            query = f"""
                SELECT DISTINCT name, ref_id, description
                FROM TBL_PROCESSES
                WHERE LOWER(name) LIKE '%{keyword}%'
                ORDER BY name
            """
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                for row in rows:
                    results.append({
                        'type': 'process',
                        'name': row[0],
                        'ref_id': row[1],
                        'description': row[2] if row[2] else '',
                        'cas_number': '',
                        'keyword': keyword
                    })
            except Exception as e:
                print(f"  Error querying processes for '{keyword}': {e}")

    finally:
        cursor.close()

    return results

def get_climate_change_impacts(conn, process_ids):
    """Get climate change impact factors for given process IDs."""
    cursor = conn.cursor()
    impacts = {}

    try:
        # Query TBL_IMPACT_RESULTS or similar tables for climate change impacts
        # Note: This is simplified - actual query may need adjustment based on DB schema
        for ref_id in process_ids:
            query = f"""
                SELECT value, unit
                FROM TBL_IMPACT_RESULTS
                WHERE f_owner = '{ref_id}'
                AND f_impact_category IN (
                    SELECT id FROM TBL_IMPACT_CATEGORIES
                    WHERE LOWER(name) LIKE '%climate change%'
                    OR LOWER(name) LIKE '%global warming%'
                )
            """
            try:
                cursor.execute(query)
                rows = cursor.fetchall()
                if rows:
                    impacts[ref_id] = rows[0]
            except:
                pass  # Table might not exist or query might fail

    except Exception as e:
        print(f"Note: Could not retrieve impact values: {e}")

    finally:
        cursor.close()

    return impacts

def main():
    print("=" * 70)
    print("Ecoinvent Fashion Materials Carbon Footprint Extractor")
    print("=" * 70)

    # Connect to database
    conn = connect_db()

    try:
        # Get and display table structure
        print("\nAvailable tables in database:")
        tables = get_tables(conn)
        for table in tables[:10]:  # Show first 10
            print(f"  - {table}")
        if len(tables) > 10:
            print(f"  ... and {len(tables) - 10} more tables")

        # Query for fashion materials
        print("\n" + "=" * 70)
        results = query_fashion_materials(conn)

        print(f"\n Found {len(results)} fashion-related entries")

        if results:
            # Save to CSV
            output_file = Path(__file__).parent / "fashion_materials_carbon_footprint.csv"
            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow([
                    'Type', 'Material Name', 'Reference ID', 'Description',
                    'CAS Number', 'Matched Keyword'
                ])
                for r in results:
                    writer.writerow([
                        r['type'], r['name'], r['ref_id'], r['description'],
                        r['cas_number'], r['keyword']
                    ])

            print(f"\n Results saved to: {output_file}")

            # Show summary
            print("\nSummary by type:")
            flows = [r for r in results if r['type'] == 'flow']
            processes = [r for r in results if r['type'] == 'process']
            print(f"  - Flows: {len(flows)}")
            print(f"  - Processes: {len(processes)}")

            # Show sample results
            print("\nSample materials found:")
            for r in results[:10]:
                print(f"  [{r['type']}] {r['name']}")
            if len(results) > 10:
                print(f"  ... and {len(results) - 10} more")

        else:
            print("\n No fashion materials found")

    except Exception as e:
        print(f"\n Error: {e}")
        import traceback
        traceback.print_exc()

    finally:
        conn.close()
        print("\n" + "=" * 70)

if __name__ == "__main__":
    main()
