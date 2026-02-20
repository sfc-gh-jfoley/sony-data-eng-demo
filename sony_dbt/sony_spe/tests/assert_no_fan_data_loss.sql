-- Test: Verify fan counts are reasonable between layers
-- Due to deduplication, DIM may have fewer unique fans than RAW interactions
-- This test ensures we have data and didn't lose everything
WITH raw_fans AS (
    SELECT COUNT(DISTINCT PARSE_JSON(raw_data):fan_id::STRING) AS raw_count
    FROM {{ source('raw', 'raw_fan_interactions') }}
),
dim_fans AS (
    SELECT COUNT(DISTINCT fan_id) AS dim_count
    FROM {{ ref('dim_fans') }}
)
SELECT 
    raw_count,
    dim_count
FROM raw_fans, dim_fans
WHERE dim_count = 0  -- Fail if no fans made it to dimension
   OR dim_count > raw_count  -- Fail if we somehow have MORE fans (data quality issue)
