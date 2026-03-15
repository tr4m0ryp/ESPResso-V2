connect 'jdbc:derby:.';

-- Check if impact values are linked to processes instead
SELECT
    p.name AS process_name,
    p.ref_id AS process_ref_id,
    ic.name AS impact_category,
    ic.reference_unit AS impact_unit
FROM TBL_PROCESSES p
LEFT JOIN TBL_IMPACT_FACTORS ifac ON p.id = ifac.f_flow
LEFT JOIN TBL_IMPACT_CATEGORIES ic ON ifac.f_impact_category = ic.id
WHERE (
    LOWER(p.name) LIKE '%cotton%'
    OR LOWER(p.name) LIKE '%polyester%'
)
AND ic.name IS NOT NULL
FETCH FIRST 10 ROWS ONLY;

-- Also check the TBL_EXCHANGES table to understand material flows from processes
SELECT columnname, columndatatype
FROM sys.syscolumns c
JOIN sys.systables t ON c.referenceid = t.tableid
WHERE t.tablename = 'TBL_EXCHANGES'
ORDER BY columnnumber;

disconnect;
exit;
