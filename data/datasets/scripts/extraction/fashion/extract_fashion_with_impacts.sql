connect 'jdbc:derby:.';

-- Extract fashion materials with their climate change impact values
SELECT
    f.name AS material_name,
    f.ref_id AS material_ref_id,
    f.description AS material_description,
    ic.name AS impact_category,
    ic.reference_unit AS impact_unit,
    ifac.value AS carbon_footprint_value,
    u.name AS flow_unit
FROM TBL_FLOWS f
LEFT JOIN TBL_IMPACT_FACTORS ifac ON f.id = ifac.f_flow
LEFT JOIN TBL_IMPACT_CATEGORIES ic ON ifac.f_impact_category = ic.id
LEFT JOIN TBL_UNITS u ON ifac.f_unit = u.id
WHERE (
    LOWER(f.name) LIKE '%cotton%'
    OR LOWER(f.name) LIKE '%polyester%'
    OR LOWER(f.name) LIKE '%nylon%'
    OR LOWER(f.name) LIKE '%wool%'
    OR LOWER(f.name) LIKE '%silk%'
    OR LOWER(f.name) LIKE '%linen%'
    OR LOWER(f.name) LIKE '%textile%'
    OR LOWER(f.name) LIKE '%fabric%'
    OR LOWER(f.name) LIKE '%fiber%'
    OR LOWER(f.name) LIKE '%fibre%'
    OR LOWER(f.name) LIKE '%yarn%'
)
AND (
    ic.id = 999594  -- Main "Climate change" category with kg CO2-Eq
    OR ic.id = 1114775
    OR ic.id = 1211094
    OR ic.id = 1286720
    OR ic.id = 1306919
    OR ic.id = 1343386
    OR ic.id = 1402069
    OR ic.id = 1419100
    OR ic.name = 'Climate change'
)
ORDER BY f.name;

disconnect;
exit;
