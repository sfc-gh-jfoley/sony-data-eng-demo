-- Test: Verify box office data has no negative revenue
SELECT 
    title_id,
    report_date,
    total_gross_usd
FROM {{ ref('fact_daily_performance') }}
WHERE total_gross_usd < 0
