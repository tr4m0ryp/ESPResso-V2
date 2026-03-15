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
