connect 'jdbc:derby:.';

-- Get list of all tables to understand database structure
SELECT tablename FROM sys.systables WHERE tabletype = 'T' AND tablename LIKE 'TBL_%';

-- Query for packaging materials in flows
SELECT name, ref_id, category, unit, description
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
   OR LOWER(name) LIKE '%jar%';

-- Query for packaging-related processes
SELECT name, ref_id, category, description, location
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
   OR LOWER(name) LIKE '%jar%';

-- Query for exchanges (emission factors) related to packaging
SELECT 
    f.name as flow_name,
    f.ref_id as flow_ref_id,
    f.category as flow_category,
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
ORDER BY f.category, f.name;

disconnect;
exit;
