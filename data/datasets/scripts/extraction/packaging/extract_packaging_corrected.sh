#!/bin/bash
# Corrected script to query ecoinvent database for packaging materials

# Use Java 21 (required for Derby tools)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# Set Derby home
export DERBY_HOME=$(dirname $(readlink -f derby.jar))
export CLASSPATH=$DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar

# First, let's check the actual column structure
cat > check_structure.sql <<'EOF'
connect 'jdbc:derby:.';

-- Check structure of TBL_FLOWS
SELECT columnname, columndatatype FROM sys.syscolumns WHERE tablename = 'TBL_FLOWS';

-- Check structure of TBL_PROCESSES  
SELECT columnname, columndatatype FROM sys.syscolumns WHERE tablename = 'TBL_PROCESSES';

-- Check structure of TBL_EXCHANGES
SELECT columnname, columndatatype FROM sys.syscolumns WHERE tablename = 'TBL_EXCHANGES';

exit;
EOF

echo "Checking database structure..."
java -cp $DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar org.apache.derby.tools.ij check_structure.sql > database_structure.txt

# Now create corrected SQL script for packaging materials
cat > query_packaging_corrected.sql <<'EOF'
connect 'jdbc:derby:.';

-- Query for packaging materials in flows (corrected column names)
SELECT name, ref_id, flow_type, unit, description
FROM TBL_FLOWS
WHERE LOWER(name) LIKE '%packag%'
   OR LOWER(name) LIKE '%cardboard%'
   OR LOWER(name) LIKE '%plastic%'
   OR LOWER(name) LIKE '%paper%'
   OR LOWER(name) LIKE '%glass%'
   OR LOWER(name) LIKE '%metal%'
   OR LOWER(name) LIKE '%aluminium%'
   OR LOWER(name) LIKE '%aluminum%'
   OR LOWER(name) LIKE '%steel%'
   OR LOWER(name) LIKE '%tin%'
   OR LOWER(name) LIKE '%wood%'
   OR LOWER(name) LIKE '%bag%'
   OR LOWER(name) LIKE '%film%'
   OR LOWER(name) LIKE '%bottle%'
   OR LOWER(name) LIKE '%container%'
   OR LOWER(name) LIKE '%box%'
   OR LOWER(name) LIKE '%carton%'
   OR LOWER(name) LIKE '%wrap%'
   OR LOWER(name) LIKE '%foil%'
   OR LOWER(name) LIKE '%pallet%'
   OR LOWER(name) LIKE '%crate%'
   OR LOWER(name) LIKE '%can%'
   OR LOWER(name) LIKE '%jar%'
ORDER BY name;

-- Query for packaging-related processes (corrected column names)
SELECT name, ref_id, process_type, description, location
FROM TBL_PROCESSES
WHERE LOWER(name) LIKE '%packag%'
   OR LOWER(name) LIKE '%cardboard%'
   OR LOWER(name) LIKE '%plastic%'
   OR LOWER(name) LIKE '%paper%'
   OR LOWER(name) LIKE '%glass%'
   OR LOWER(name) LIKE '%metal%'
   OR LOWER(name) LIKE '%aluminium%'
   OR LOWER(name) LIKE '%aluminum%'
   OR LOWER(name) LIKE '%steel%'
   OR LOWER(name) LIKE '%tin%'
   OR LOWER(name) LIKE '%wood%'
   OR LOWER(name) LIKE '%bag%'
   OR LOWER(name) LIKE '%film%'
   OR LOWER(name) LIKE '%bottle%'
   OR LOWER(name) LIKE '%container%'
   OR LOWER(name) LIKE '%box%'
   OR LOWER(name) LIKE '%carton%'
   OR LOWER(name) LIKE '%wrap%'
   OR LOWER(name) LIKE '%foil%'
   OR LOWER(name) LIKE '%pallet%'
   OR LOWER(name) LIKE '%crate%'
   OR LOWER(name) LIKE '%can%'
   OR LOWER(name) LIKE '%jar%'
ORDER BY name;

-- Query for exchanges (emission factors) related to packaging (corrected column names)
SELECT 
    f.name as flow_name,
    f.ref_id as flow_ref_id,
    f.flow_type,
    f.unit as flow_unit,
    p.name as process_name,
    p.ref_id as process_ref_id,
    e.value as exchange_value,
    e.unit as exchange_unit,
    e.is_input,
    e.amount
FROM TBL_EXCHANGES e
JOIN TBL_FLOWS f ON e.flow_id = f.id
JOIN TBL_PROCESSES p ON e.process_id = p.id
WHERE (
    LOWER(f.name) LIKE '%packag%'
    OR LOWER(f.name) LIKE '%cardboard%'
    OR LOWER(f.name) LIKE '%plastic%'
    OR LOWER(f.name) LIKE '%paper%'
    OR LOWER(f.name) LIKE '%glass%'
    OR LOWER(f.name) LIKE '%metal%'
    OR LOWER(f.name) LIKE '%aluminium%'
    OR LOWER(f.name) LIKE '%aluminum%'
    OR LOWER(f.name) LIKE '%steel%'
    OR LOWER(f.name) LIKE '%tin%'
    OR LOWER(f.name) LIKE '%wood%'
    OR LOWER(f.name) LIKE '%bag%'
    OR LOWER(f.name) LIKE '%film%'
    OR LOWER(f.name) LIKE '%bottle%'
    OR LOWER(f.name) LIKE '%container%'
    OR LOWER(f.name) LIKE '%box%'
    OR LOWER(f.name) LIKE '%carton%'
    OR LOWER(f.name) LIKE '%wrap%'
    OR LOWER(f.name) LIKE '%foil%'
    OR LOWER(f.name) LIKE '%pallet%'
    OR LOWER(f.name) LIKE '%crate%'
    OR LOWER(f.name) LIKE '%can%'
    OR LOWER(f.name) LIKE '%jar%'
)
AND e.value IS NOT NULL
ORDER BY f.name;

disconnect;
exit;
EOF

echo "Running corrected Derby query for packaging materials..."
java -cp $DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar org.apache.derby.tools.ij query_packaging_corrected.sql > packaging_materials_corrected_output.txt 2>&1

echo "Query complete. Results saved to:"
echo "- database_structure.txt (database schema)"
echo "- packaging_materials_corrected_output.txt (packaging data)"
echo ""
echo "Database structure (first 30 lines):"
head -30 database_structure.txt
echo ""
echo "Packaging data (first 50 lines):"
head -50 packaging_materials_corrected_output.txt