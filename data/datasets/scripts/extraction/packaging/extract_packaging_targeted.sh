#!/bin/bash
# Targeted script to extract packaging materials from ecoinvent database

# Use Java 21 (required for Derby tools)
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# Set Derby home
export DERBY_HOME=$(dirname $(readlink -f derby.jar))
export CLASSPATH=$DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar

# Create targeted SQL script for packaging materials
cat > query_packaging_targeted.sql <<'EOF'
connect 'jdbc:derby:.';

-- Query for specific packaging materials in flows
SELECT name, ref_id, description
FROM TBL_FLOWS
WHERE LOWER(name) LIKE '%packaging%'
   OR LOWER(name) LIKE '%container%'
   OR LOWER(name) LIKE '%bottle%'
   OR LOWER(name) LIKE '%carton%'
   OR LOWER(name) LIKE '%box%'
   OR LOWER(name) LIKE '%bag%'
   OR LOWER(name) LIKE '%wrap%'
   OR LOWER(name) LIKE '%film%'
   OR LOWER(name) LIKE '%foil%'
   OR LOWER(name) LIKE '%pallet%'
   OR LOWER(name) LIKE '%crate%'
   OR LOWER(name) LIKE '%can%'
   OR LOWER(name) LIKE '%jar%'
   OR LOWER(name) LIKE '%cardboard%'
   OR LOWER(name) LIKE '%kraft%'
   OR LOWER(name) LIKE '%paperboard%'
   OR LOWER(name) LIKE '%corrugated%'
   OR LOWER(name) LIKE '%disposable%'
   OR LOWER(name) LIKE '%single use%'
   OR LOWER(name) LIKE '%food packaging%'
ORDER BY name;

-- Query for specific packaging processes
SELECT name, ref_id, description
FROM TBL_PROCESSES
WHERE LOWER(name) LIKE '%packaging%'
   OR LOWER(name) LIKE '%container%'
   OR LOWER(name) LIKE '%bottle%'
   OR LOWER(name) LIKE '%carton%'
   OR LOWER(name) LIKE '%box%'
   OR LOWER(name) LIKE '%bag%'
   OR LOWER(name) LIKE '%wrap%'
   OR LOWER(name) LIKE '%film%'
   OR LOWER(name) LIKE '%foil%'
   OR LOWER(name) LIKE '%pallet%'
   OR LOWER(name) LIKE '%crate%'
   OR LOWER(name) LIKE '%can%'
   OR LOWER(name) LIKE '%jar%'
   OR LOWER(name) LIKE '%cardboard%'
   OR LOWER(name) LIKE '%kraft%'
   OR LOWER(name) LIKE '%paperboard%'
   OR LOWER(name) LIKE '%corrugated%'
   OR LOWER(name) LIKE '%disposable%'
   OR LOWER(name) LIKE '%single use%'
   OR LOWER(name) LIKE '%food packaging%'
ORDER BY name;

-- Query for basic packaging materials (plastics, paper, glass, metal, wood)
SELECT name, ref_id, description
FROM TBL_FLOWS
WHERE LOWER(name) LIKE '%polyethylene%'
   OR LOWER(name) LIKE '%polypropylene%'
   OR LOWER(name) LIKE '%polystyrene%'
   OR LOWER(name) LIKE '%pvc%'
   OR LOWER(name) LIKE '%pet%'
   OR LOWER(name) LIKE '%paper%' AND (LOWER(name) LIKE '%packag%' OR LOWER(name) LIKE '%container%' OR LOWER(name) LIKE '%bag%' OR LOWER(name) LIKE '%box%')
   OR LOWER(name) LIKE '%glass%' AND (LOWER(name) LIKE '%bottle%' OR LOWER(name) LIKE '%jar%' OR LOWER(name) LIKE '%container%')
   OR LOWER(name) LIKE '%aluminium%' AND (LOWER(name) LIKE '%foil%' OR LOWER(name) LIKE '%can%' OR LOWER(name) LIKE '%container%')
   OR LOWER(name) LIKE '%steel%' AND (LOWER(name) LIKE '%can%' OR LOWER(name) LIKE '%container%')
   OR LOWER(name) LIKE '%tin%' AND (LOWER(name) LIKE '%can%' OR LOWER(name) LIKE '%container%')
   OR LOWER(name) LIKE '%wood%' AND (LOWER(name) LIKE '%pallet%' OR LOWER(name) LIKE '%crate%' OR LOWER(name) LIKE '%box%')
ORDER BY name;

-- Query for exchanges (emission factors) for packaging materials
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
    LOWER(f.name) LIKE '%packaging%'
    OR LOWER(f.name) LIKE '%container%'
    OR LOWER(f.name) LIKE '%bottle%'
    OR LOWER(f.name) LIKE '%carton%'
    OR LOWER(f.name) LIKE '%box%'
    OR LOWER(f.name) LIKE '%bag%'
    OR LOWER(f.name) LIKE '%wrap%'
    OR LOWER(f.name) LIKE '%film%'
    OR LOWER(f.name) LIKE '%foil%'
    OR LOWER(f.name) LIKE '%pallet%'
    OR LOWER(f.name) LIKE '%crate%'
    OR LOWER(f.name) LIKE '%can%'
    OR LOWER(f.name) LIKE '%jar%'
    OR LOWER(f.name) LIKE '%cardboard%'
    OR LOWER(f.name) LIKE '%kraft%'
    OR LOWER(f.name) LIKE '%paperboard%'
    OR LOWER(f.name) LIKE '%corrugated%'
    OR LOWER(f.name) LIKE '%disposable%'
    OR LOWER(f.name) LIKE '%single use%'
    OR LOWER(f.name) LIKE '%food packaging%'
    OR (LOWER(f.name) LIKE '%polyethylene%' AND LOWER(f.name) NOT LIKE '%production%')
    OR (LOWER(f.name) LIKE '%polypropylene%' AND LOWER(f.name) NOT LIKE '%production%')
    OR (LOWER(f.name) LIKE '%polystyrene%' AND LOWER(f.name) NOT LIKE '%production%')
    OR (LOWER(f.name) LIKE '%pvc%' AND LOWER(f.name) NOT LIKE '%production%')
    OR (LOWER(f.name) LIKE '%pet%' AND LOWER(f.name) NOT LIKE '%production%')
)
AND e.value IS NOT NULL
ORDER BY f.name;

disconnect;
exit;
EOF

echo "Extracting packaging materials from ecoinvent database (targeted search)..."
java -cp $DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar org.apache.derby.tools.ij query_packaging_targeted.sql > packaging_materials_targeted_output.txt 2>&1

echo "Extraction complete! Results saved to packaging_materials_targeted_output.txt"
echo ""

# Parse and count results
echo "Analyzing results..."
python3 -c "
import re

# Read the output file
with open('packaging_materials_targeted_output.txt', 'r') as f:
    content = f.read()

# Extract material names from flows section
flows_matches = []
exchanges_matches = []

# Find flows section
flows_pattern = r'-- Query for specific packaging materials in flows(.*?)(?=-- Query for specific packaging processes)'
flows_match = re.search(flows_pattern, content, re.DOTALL)
if flows_match:
    lines = flows_match.group(1).split('\n')
    for line in lines:
        if '|' in line and not line.startswith('--') and not line.startswith('ij>') and 'NAME' not in line and '---' not in line:
            parts = line.split('|')
            if len(parts) >= 3 and len(parts[0].strip()) > 3:
                flows_matches.append(parts[0].strip())

# Find exchanges section  
exchanges_pattern = r'-- Query for exchanges.*?$(.*?)(?=disconnect)'
exchanges_match = re.search(exchanges_pattern, content, re.DOTALL | re.MULTILINE)
if exchanges_match and exchanges_match.group(1):
    lines = exchanges_match.group(1).split('\n')
    for line in lines:
        if '|' in line and not line.startswith('--') and not line.startswith('ij>') and 'FLOW_NAME' not in line and '---' not in line:
            parts = line.split('|')
            if len(parts) >= 3 and len(parts[0].strip()) > 3:
                exchanges_matches.append(parts[0].strip())

print(f'Packaging materials found: {len(flows_matches)}')
print(f'Exchange records found: {len(exchanges_matches)}')
print()
if flows_matches:
    print('Sample packaging materials:')
    for i, material in enumerate(flows_matches[:10]):
        print(f'  {i+1}. {material}')

if exchanges_matches:
    print()
    print('Sample exchange records:')
    for i, exchange in enumerate(exchanges_matches[:5]):
        print(f'  {i+1}. {exchange}')
"