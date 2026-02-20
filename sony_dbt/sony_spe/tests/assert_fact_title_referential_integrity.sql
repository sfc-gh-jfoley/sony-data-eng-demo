-- Test: Verify title referential integrity in facts
-- All title_ids in fact table must exist in dim_titles
SELECT f.title_id
FROM {{ ref('fact_daily_performance') }} f
LEFT JOIN {{ ref('dim_titles') }} d ON f.title_id = d.title_id
WHERE d.title_id IS NULL
