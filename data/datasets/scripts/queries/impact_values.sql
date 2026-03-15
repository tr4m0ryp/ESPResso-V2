connect 'jdbc:derby:.';

-- First, let's find climate change related impact categories
SELECT id, ref_id, name, reference_unit
FROM TBL_IMPACT_CATEGORIES
WHERE LOWER(name) LIKE '%climate%'
   OR LOWER(name) LIKE '%global warming%'
   OR LOWER(name) LIKE '%gwp%'
   OR LOWER(name) LIKE '%co2%';

-- Check the structure of impact factors table
SELECT COUNT(*) as total_impact_factors FROM TBL_IMPACT_FACTORS;

-- Sample some impact factors
SELECT f_impact_category, f_flow, value, formula, unit
FROM TBL_IMPACT_FACTORS
WHERE f_impact_category IN (
    SELECT id FROM TBL_IMPACT_CATEGORIES
    WHERE LOWER(name) LIKE '%climate%'
)
FETCH FIRST 20 ROWS ONLY;

disconnect;
exit;
