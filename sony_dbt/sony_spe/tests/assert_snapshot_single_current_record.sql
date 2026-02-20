-- Test: Verify each fan has exactly one current record in snapshot
SELECT 
    fan_id,
    COUNT(*) AS current_record_count
FROM {{ ref('snap_dim_fans') }}
WHERE dbt_valid_to IS NULL
GROUP BY fan_id
HAVING COUNT(*) != 1
