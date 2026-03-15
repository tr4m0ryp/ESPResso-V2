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
