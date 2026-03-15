connect 'jdbc:derby:.';

-- Get column structure of TBL_IMPACT_FACTORS
SELECT columnname, columndatatype
FROM sys.syscolumns c
JOIN sys.systables t ON c.referenceid = t.tableid
WHERE t.tablename = 'TBL_IMPACT_FACTORS'
ORDER BY columnnumber;

disconnect;
exit;
