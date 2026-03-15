connect 'jdbc:derby:.';

-- Check structure of TBL_FLOWS
SELECT columnname, columndatatype FROM sys.syscolumns WHERE tablename = 'TBL_FLOWS';

-- Check structure of TBL_PROCESSES  
SELECT columnname, columndatatype FROM sys.syscolumns WHERE tablename = 'TBL_PROCESSES';

-- Check structure of TBL_EXCHANGES
SELECT columnname, columndatatype FROM sys.syscolumns WHERE tablename = 'TBL_EXCHANGES';

exit;
