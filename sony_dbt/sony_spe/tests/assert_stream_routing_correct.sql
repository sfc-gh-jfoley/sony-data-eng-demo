-- Test: Verify Stream/Task routing worked correctly
-- VERIFIED fans table should have records, GUEST table should have records
-- (We can't check account_type since it's not stored - routing determines the table)
SELECT 'STG_FAN_VERIFIED empty' AS error_type
WHERE NOT EXISTS (SELECT 1 FROM {{ source('stg', 'stg_fan_verified') }})

UNION ALL

SELECT 'STG_FAN_GUEST empty' AS error_type
WHERE NOT EXISTS (SELECT 1 FROM {{ source('stg', 'stg_fan_guest') }})
