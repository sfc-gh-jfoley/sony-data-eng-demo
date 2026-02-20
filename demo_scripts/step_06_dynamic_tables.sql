-- =============================================================================
-- STEP 6: DYNAMIC TABLES (Declarative Pipeline)
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- Run as: SYSADMIN
-- Time: ~5 minutes
-- =============================================================================
-- 
-- Dynamic Table DAG (10 DTs):
--
-- BRONZE (raw tables) + SILVER (task-fed tables)
--     │
--     ├── STG_FAN_VERIFIED (Task-populated)
--     ├── STG_FAN_GUEST (Task-populated)
--     └── STG_BOX_OFFICE_CLEAN (Task-populated)
--         │
--         ▼
-- SILVER - Staging DTs (1 min lag)
--     ├── DT_STG_FANS_UNIFIED      (UNION of verified + guest)
--     ├── DT_STG_TITLES_PARSED     (parse raw JSON)
--     └── DT_STG_BOX_OFFICE_DEDUP  (QUALIFY dedup)
--         │
--         ▼
-- SILVER - Intermediate DTs (DOWNSTREAM)
--     ├── DT_INT_FANS_ENRICHED     (fan statistics)
--     └── DT_INT_DAILY_PERFORMANCE (join box office + streaming)
--         │
--         ▼
-- GOLD - Dimensional DTs (DOWNSTREAM)
--     ├── DT_DIM_FANS              (fan dimension)
--     ├── DT_DIM_TITLES            (title dimension)
--     └── DT_FACT_DAILY_PERFORMANCE (fact table)
--         │
--         ▼
-- PLATINUM - Analytics DTs (5 min lag)
--     ├── AGG_FRANCHISE_PERFORMANCE
--     └── AGG_FAN_LIFETIME_VALUE
--
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 6.1 SILVER LAYER: STAGING DYNAMIC TABLES (1 minute lag)
-- =============================================================================

-- Unify verified and guest fan interactions
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_STG_FANS_UNIFIED
    LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    interaction_id,
    fan_id,
    email,
    first_name,
    last_name,
    region,
    country_code,
    event_type,
    event_timestamp,
    title_id,
    device_type,
    'VERIFIED' AS account_type,
    raw_id
FROM SONY_DE.SILVER.STG_FAN_VERIFIED
UNION ALL
SELECT
    interaction_id,
    NULL AS fan_id,
    NULL AS email,
    NULL AS first_name,
    NULL AS last_name,
    region,
    country_code,
    event_type,
    event_timestamp,
    title_id,
    device_type,
    'GUEST' AS account_type,
    raw_id
FROM SONY_DE.SILVER.STG_FAN_GUEST;

-- Parse titles from Bronze (alternative to Task)
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_STG_TITLES_PARSED
    LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    raw_data:title_id::varchar AS title_id,
    raw_data:title_name::varchar AS title_name,
    raw_data:franchise::varchar AS franchise,
    raw_data:release_year::int AS release_year,
    raw_data:genre::varchar AS genre,
    raw_data:sub_genre::varchar AS sub_genre,
    raw_data:rating::varchar AS rating,
    raw_data:runtime_minutes::int AS runtime_minutes,
    raw_data:imdb_score::float AS imdb_score,
    raw_data:budget_usd::number(15,2) AS budget_usd,
    raw_data:production_status::varchar AS production_status,
    raw_data:studio::varchar AS studio,
    raw_data:director::varchar AS director,
    raw_id,
    ingestion_ts
FROM SONY_DE.BRONZE.RAW_TITLE_METADATA;

-- Deduplicate box office (QUALIFY pattern)
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_STG_BOX_OFFICE_DEDUP
    LAG = '1 minute'
    WAREHOUSE = COMPUTE_WH
AS
SELECT * FROM SONY_DE.SILVER.STG_BOX_OFFICE_CLEAN
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY title_id, report_date, theater_id
    ORDER BY ingestion_ts DESC
) = 1;

-- =============================================================================
-- 6.2 SILVER LAYER: INTERMEDIATE DYNAMIC TABLES (DOWNSTREAM)
-- =============================================================================

-- Enrich fans with aggregated statistics
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_INT_FANS_ENRICHED
    LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
WITH fan_stats AS (
    SELECT
        fan_id,
        MIN(event_timestamp) AS first_seen,
        MAX(event_timestamp) AS last_seen,
        COUNT(*) AS interaction_count,
        COUNT(DISTINCT title_id) AS titles_engaged,
        MODE(device_type) AS preferred_device,
        MODE(region) AS primary_region,
        MODE(country_code) AS primary_country
    FROM SONY_DE.SILVER.DT_STG_FANS_UNIFIED
    WHERE fan_id IS NOT NULL
    GROUP BY fan_id
),
fan_details AS (
    SELECT DISTINCT
        fan_id,
        FIRST_VALUE(email) OVER (PARTITION BY fan_id ORDER BY event_timestamp DESC) AS email,
        FIRST_VALUE(first_name) OVER (PARTITION BY fan_id ORDER BY event_timestamp DESC) AS first_name,
        FIRST_VALUE(last_name) OVER (PARTITION BY fan_id ORDER BY event_timestamp DESC) AS last_name,
        FIRST_VALUE(account_type) OVER (PARTITION BY fan_id ORDER BY event_timestamp DESC) AS account_type
    FROM SONY_DE.SILVER.DT_STG_FANS_UNIFIED
    WHERE fan_id IS NOT NULL
)
SELECT
    d.fan_id,
    d.account_type,
    d.email,
    SHA2(d.email, 256) AS email_hash,
    d.first_name,
    d.last_name,
    s.primary_region AS region,
    s.primary_country AS country_code,
    s.first_seen::date AS signup_date,
    s.last_seen::date AS last_active_date,
    s.interaction_count AS lifetime_interactions,
    s.titles_engaged,
    s.preferred_device,
    CURRENT_TIMESTAMP() AS effective_from
FROM fan_details d
JOIN fan_stats s ON d.fan_id = s.fan_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY d.fan_id ORDER BY s.last_seen DESC) = 1;

-- Combine box office + streaming metrics
CREATE OR REPLACE DYNAMIC TABLE SILVER.DT_INT_DAILY_PERFORMANCE
    LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
WITH daily_metrics AS (
    SELECT
        title_id,
        report_date,
        theater_region AS region,
        SUM(tickets_sold) AS total_tickets_sold,
        SUM(gross_revenue_usd) AS total_gross_usd,
        COUNT(DISTINCT theater_id) AS theater_count,
        SUM(screen_count) AS screen_count,
        AVG(gross_revenue_usd / NULLIF(tickets_sold, 0)) AS avg_ticket_price_usd
    FROM SONY_DE.SILVER.DT_STG_BOX_OFFICE_DEDUP
    GROUP BY title_id, report_date, theater_region
),
fan_metrics AS (
    SELECT
        title_id,
        event_timestamp::date AS activity_date,
        region,
        COUNT(CASE WHEN event_type = 'STREAM' THEN 1 END) AS stream_count,
        COUNT(DISTINCT fan_id) AS unique_fans
    FROM SONY_DE.SILVER.DT_STG_FANS_UNIFIED
    GROUP BY title_id, event_timestamp::date, region
)
SELECT
    COALESCE(d.title_id, f.title_id) AS title_id,
    COALESCE(d.report_date, f.activity_date) AS report_date,
    COALESCE(d.region, f.region) AS region,
    COALESCE(d.total_tickets_sold, 0) AS total_tickets_sold,
    COALESCE(d.total_gross_usd, 0) AS total_gross_usd,
    COALESCE(d.theater_count, 0) AS theater_count,
    COALESCE(d.screen_count, 0) AS screen_count,
    COALESCE(d.avg_ticket_price_usd, 0) AS avg_ticket_price_usd,
    COALESCE(f.stream_count, 0) AS stream_count,
    COALESCE(f.unique_fans, 0) AS unique_fans
FROM daily_metrics d
FULL OUTER JOIN fan_metrics f 
    ON d.title_id = f.title_id 
    AND d.report_date = f.activity_date 
    AND d.region = f.region;

-- =============================================================================
-- 6.3 GOLD LAYER: DIMENSIONAL DYNAMIC TABLES (DOWNSTREAM)
-- =============================================================================

-- Fan Dimension
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_DIM_FANS
    LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    MD5(fan_id) AS fan_key,
    fan_id,
    account_type,
    email,
    email_hash,
    first_name,
    last_name,
    region,
    country_code,
    signup_date,
    last_active_date,
    lifetime_interactions,
    titles_engaged,
    preferred_device,
    effective_from,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM SONY_DE.SILVER.DT_INT_FANS_ENRICHED;

-- Title Dimension
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_DIM_TITLES
    LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    MD5(title_id) AS title_key,
    title_id,
    title_name,
    franchise,
    release_year,
    genre,
    sub_genre,
    rating,
    runtime_minutes,
    imdb_score,
    budget_usd,
    production_status,
    studio,
    director,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM SONY_DE.SILVER.DT_STG_TITLES_PARSED;

-- Fact: Daily Performance
CREATE OR REPLACE DYNAMIC TABLE GOLD.DT_FACT_DAILY_PERFORMANCE
    LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    MD5(CONCAT(title_id, report_date, region)) AS performance_key,
    title_id,
    report_date,
    region,
    total_tickets_sold,
    total_gross_usd,
    theater_count,
    screen_count,
    avg_ticket_price_usd,
    stream_count,
    unique_fans,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM SONY_DE.SILVER.DT_INT_DAILY_PERFORMANCE;

-- =============================================================================
-- 6.4 PLATINUM LAYER: ANALYTICS AGGREGATION DTs (5 minute lag)
-- =============================================================================

-- Franchise Performance Aggregate
CREATE OR REPLACE DYNAMIC TABLE PLATINUM.AGG_FRANCHISE_PERFORMANCE
    LAG = '5 minutes'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Real-time franchise performance aggregation'
AS
SELECT
    t.franchise,
    t.genre,
    COUNT(DISTINCT t.title_id) AS title_count,
    SUM(f.total_tickets_sold) AS total_tickets,
    SUM(f.total_gross_usd) AS total_gross_usd,
    AVG(f.avg_ticket_price_usd) AS avg_ticket_price,
    SUM(f.stream_count) AS total_streams,
    SUM(f.unique_fans) AS total_unique_fans,
    MIN(f.report_date) AS first_report_date,
    MAX(f.report_date) AS last_report_date,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM SONY_DE.GOLD.DT_FACT_DAILY_PERFORMANCE f
JOIN SONY_DE.GOLD.DT_DIM_TITLES t ON f.title_id = t.title_id
GROUP BY t.franchise, t.genre;

-- Fan Lifetime Value Aggregate
CREATE OR REPLACE DYNAMIC TABLE PLATINUM.AGG_FAN_LIFETIME_VALUE
    LAG = '5 minutes'
    WAREHOUSE = COMPUTE_WH
    COMMENT = 'Real-time fan lifetime value aggregation'
AS
SELECT
    d.region,
    d.country_code,
    d.account_type,
    COUNT(DISTINCT d.fan_id) AS fan_count,
    AVG(d.lifetime_interactions) AS avg_interactions,
    AVG(d.titles_engaged) AS avg_titles_engaged,
    AVG(DATEDIFF('day', d.signup_date, d.last_active_date)) AS avg_active_days,
    SUM(CASE WHEN d.last_active_date >= DATEADD('day', -30, CURRENT_DATE()) THEN 1 ELSE 0 END) AS active_last_30_days,
    SUM(CASE WHEN d.last_active_date >= DATEADD('day', -7, CURRENT_DATE()) THEN 1 ELSE 0 END) AS active_last_7_days,
    MODE(d.preferred_device) AS top_device,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM SONY_DE.GOLD.DT_DIM_FANS d
GROUP BY d.region, d.country_code, d.account_type;

-- =============================================================================
-- 6.5 VERIFICATION
-- =============================================================================

-- Check Dynamic Table status
SHOW DYNAMIC TABLES IN DATABASE SONY_DE;

-- Check row counts
SELECT 'DT_STG_FANS_UNIFIED' AS dt_name, COUNT(*) AS rows FROM SILVER.DT_STG_FANS_UNIFIED
UNION ALL SELECT 'DT_STG_TITLES_PARSED', COUNT(*) FROM SILVER.DT_STG_TITLES_PARSED
UNION ALL SELECT 'DT_STG_BOX_OFFICE_DEDUP', COUNT(*) FROM SILVER.DT_STG_BOX_OFFICE_DEDUP
UNION ALL SELECT 'DT_INT_FANS_ENRICHED', COUNT(*) FROM SILVER.DT_INT_FANS_ENRICHED
UNION ALL SELECT 'DT_INT_DAILY_PERFORMANCE', COUNT(*) FROM SILVER.DT_INT_DAILY_PERFORMANCE
UNION ALL SELECT 'DT_DIM_FANS', COUNT(*) FROM GOLD.DT_DIM_FANS
UNION ALL SELECT 'DT_DIM_TITLES', COUNT(*) FROM GOLD.DT_DIM_TITLES
UNION ALL SELECT 'DT_FACT_DAILY_PERFORMANCE', COUNT(*) FROM GOLD.DT_FACT_DAILY_PERFORMANCE
UNION ALL SELECT 'AGG_FRANCHISE_PERFORMANCE', COUNT(*) FROM PLATINUM.AGG_FRANCHISE_PERFORMANCE
UNION ALL SELECT 'AGG_FAN_LIFETIME_VALUE', COUNT(*) FROM PLATINUM.AGG_FAN_LIFETIME_VALUE;

-- Expected row counts:
-- DT_STG_FANS_UNIFIED: ~110,000
-- DT_STG_TITLES_PARSED: 15-45
-- DT_STG_BOX_OFFICE_DEDUP: ~45,000
-- DT_INT_FANS_ENRICHED: ~28,000 (unique verified fans)
-- DT_INT_DAILY_PERFORMANCE: ~3,000
-- DT_DIM_FANS: ~28,000
-- DT_DIM_TITLES: 15-45
-- DT_FACT_DAILY_PERFORMANCE: ~3,000
-- AGG_FRANCHISE_PERFORMANCE: ~12
-- AGG_FAN_LIFETIME_VALUE: ~35
