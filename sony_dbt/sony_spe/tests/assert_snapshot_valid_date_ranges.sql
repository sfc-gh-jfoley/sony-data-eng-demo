-- Test: Verify SCD Type 2 snapshot has valid date ranges
-- dbt_valid_from should always be <= dbt_valid_to (when not null)
SELECT 
    fan_id,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('snap_dim_fans') }}
WHERE dbt_valid_to IS NOT NULL 
  AND dbt_valid_from > dbt_valid_to
