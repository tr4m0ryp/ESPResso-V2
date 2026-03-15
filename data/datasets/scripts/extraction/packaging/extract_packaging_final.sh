#!/bin/bash
# Final working script to extract packaging materials from ecoinvent database

# Use Java 21 (required for Derby tools)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# Set Derby home
export DERBY_HOME=$(dirname $(readlink -f derby.jar))
export CLASSPATH=$DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar

# Create comprehensive SQL script for packaging materials
cat > query_packaging_final.sql <<'EOF'
connect 'jdbc:derby:.';

-- Query for packaging materials in flows
SELECT name, ref_id, description
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
   OR LOWER(name) LIKE '%polyethylene%'
   OR LOWER(name) LIKE '%polypropylene%'
   OR LOWER(name) LIKE '%polystyrene%'
   OR LOWER(name) LIKE '%pvc%'
   OR LOWER(name) LIKE '%pet%'
   OR LOWER(name) LIKE '%kraft%'
ORDER BY name;

-- Query for packaging-related processes
SELECT name, ref_id, description
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
   OR LOWER(name) LIKE '%polyethylene%'
   OR LOWER(name) LIKE '%polypropylene%'
   OR LOWER(name) LIKE '%polystyrene%'
   OR LOWER(name) LIKE '%pvc%'
   OR LOWER(name) LIKE '%pet%'
   OR LOWER(name) LIKE '%kraft%'
ORDER BY name;

-- Query for exchanges (emission factors) related to packaging
SELECT 
    f.name as flow_name,
    f.ref_id as flow_ref_id,
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
    OR LOWER(f.name) LIKE '%polyethylene%'
    OR LOWER(f.name) LIKE '%polypropylene%'
    OR LOWER(f.name) LIKE '%polystyrene%'
    OR LOWER(f.name) LIKE '%pvc%'
    OR LOWER(f.name) LIKE '%pet%'
    OR LOWER(f.name) LIKE '%kraft%'
)
AND e.value IS NOT NULL
ORDER BY f.name;

disconnect;
exit;
EOF

echo "Extracting packaging materials from ecoinvent database..."
java -cp $DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar org.apache.derby.tools.ij query_packaging_final.sql > packaging_materials_final_output.txt 2>&1

echo "Extraction complete! Results saved to packaging_materials_final_output.txt"
echo ""
echo "Summary of extracted data:"
echo "========================="

# Count results
flows_count=$(grep -c "TBL_FLOWS" packaging_materials_final_output.txt | head -1)
exchanges_count=$(grep -c "TBL_EXCHANGES" packaging_materials_final_output.txt | head -1)
processes_count=$(grep -c "TBL_PROCESSES" packaging_materials_final_output.txt | head -1)

echo "Flows (materials) found: $flows_count"
echo "Processes found: $processes_count"
echo "Exchanges (emission factors) found: $exchanges_count"
echo ""
echo "First 100 lines of extracted data:"
head -100 packaging_materials_final_output.txt