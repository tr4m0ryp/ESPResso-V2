connect 'jdbc:derby:.';

-- Check structure of impact results
SELECT columnname, columndatatype
FROM sys.syscolumns c
JOIN sys.systables t ON c.referenceid = t.tableid
WHERE t.tablename = 'TBL_IMPACT_RESULTS'
ORDER BY columnnumber;

-- Check structure of flow results
SELECT columnname, columndatatype
FROM sys.syscolumns c
JOIN sys.systables t ON c.referenceid = t.tableid
WHERE t.tablename = 'TBL_FLOW_RESULTS'
ORDER BY columnnumber;

-- Count results
SELECT COUNT(*) AS total_impact_results FROM TBL_IMPACT_RESULTS;
SELECT COUNT(*) AS total_flow_results FROM TBL_FLOW_RESULTS;

disconnect;
exit;
