#!/bin/bash
# Script to query ecoinvent database using Derby ij tool

# Use Java 21
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH

# Set Derby home
export DERBY_HOME=$(dirname $(readlink -f derby.jar))
export CLASSPATH=$DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar

# Create SQL script
cat > query_fashion.sql <<'EOF'
connect 'jdbc:derby:.';

-- Get list of all tables
SELECT tablename FROM sys.systables WHERE tabletype = 'T' AND tablename LIKE 'TBL_%';

-- Query for fashion materials in flows
SELECT name, ref_id, description
FROM TBL_FLOWS
WHERE LOWER(name) LIKE '%cotton%'
   OR LOWER(name) LIKE '%polyester%'
   OR LOWER(name) LIKE '%nylon%'
   OR LOWER(name) LIKE '%wool%'
   OR LOWER(name) LIKE '%silk%'
   OR LOWER(name) LIKE '%linen%'
   OR LOWER(name) LIKE '%textile%'
   OR LOWER(name) LIKE '%fabric%'
   OR LOWER(name) LIKE '%fiber%'
   OR LOWER(name) LIKE '%fibre%';

-- Query for fashion materials in processes
SELECT name, ref_id, description
FROM TBL_PROCESSES
WHERE LOWER(name) LIKE '%cotton%'
   OR LOWER(name) LIKE '%polyester%'
   OR LOWER(name) LIKE '%nylon%'
   OR LOWER(name) LIKE '%wool%'
   OR LOWER(name) LIKE '%silk%'
   OR LOWER(name) LIKE '%linen%'
   OR LOWER(name) LIKE '%textile%'
   OR LOWER(name) LIKE '%fabric%'
   OR LOWER(name) LIKE '%fiber%'
   OR LOWER(name) LIKE '%fibre%';

disconnect;
exit;
EOF

# Run Derby ij
echo "Running Derby ij to query database..."
java -cp $DERBY_HOME/derby-10.17.jar:$DERBY_HOME/derbytools-10.17.jar:$DERBY_HOME/derbyshared-10.17.jar org.apache.derby.tools.ij query_fashion.sql > fashion_materials_output.txt 2>&1

echo "Query complete. Results saved to fashion_materials_output.txt"
cat fashion_materials_output.txt
