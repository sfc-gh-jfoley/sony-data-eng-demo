-- =============================================================================
-- STEP 4: STREAMS & TASKS (Imperative Pipeline)
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- Run as: SYSADMIN
-- Time: ~2 minutes
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 4.1 CREATE STREAM (CDC on Bronze Fan Interactions)
-- =============================================================================

CREATE STREAM IF NOT EXISTS BRONZE.STREAM_FAN_INTERACTIONS
    ON TABLE BRONZE.RAW_FAN_INTERACTIONS
    APPEND_ONLY = TRUE
    COMMENT = 'CDC stream for fan interaction routing';

-- =============================================================================
-- 4.2 CREATE SILVER STAGING TABLES (for Task routing)
-- =============================================================================

-- Verified fans (linked to ticket purchases)
CREATE TABLE IF NOT EXISTS SILVER.STG_FAN_VERIFIED (
    interaction_id VARCHAR NOT NULL,
    fan_id VARCHAR NOT NULL,
    email VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    region VARCHAR,
    country_code VARCHAR(3),
    event_type VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    title_id VARCHAR,
    device_type VARCHAR,
    raw_id VARCHAR,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

-- Guest/anonymous fans
CREATE TABLE IF NOT EXISTS SILVER.STG_FAN_GUEST (
    interaction_id VARCHAR NOT NULL,
    session_id VARCHAR,
    ip_address VARCHAR,
    region VARCHAR,
    country_code VARCHAR(3),
    event_type VARCHAR,
    event_timestamp TIMESTAMP_NTZ,
    title_id VARCHAR,
    device_type VARCHAR,
    raw_id VARCHAR,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CHANGE_TRACKING = TRUE;

-- =============================================================================
-- 4.3 CREATE ROUTING TASK (Fan-Out Pattern)
-- =============================================================================
-- Key Pattern: INSERT ALL with conditional routing
-- Only runs when stream has data (cost-efficient)

CREATE OR REPLACE TASK BRONZE.TASK_ROUTE_FAN_DATA
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '1 MINUTE'
    COMMENT = 'Routes fan interactions to VERIFIED or GUEST tables'
    WHEN SYSTEM$STREAM_HAS_DATA('SONY_DE.BRONZE.STREAM_FAN_INTERACTIONS')
AS
INSERT ALL
    -- Path A: Verified fans (account_type = 'VERIFIED' - note: data uses 'VERIFIED' not 'VERIFIED_LINKED')
    WHEN account_type = 'VERIFIED' THEN
        INTO SONY_DE.SILVER.STG_FAN_VERIFIED (
            interaction_id, fan_id, email, first_name, last_name, 
            region, country_code, event_type, event_timestamp, title_id, device_type, raw_id
        )
        VALUES (interaction_id, fan_id, email, first_name, last_name,
                region, country_code, event_type, event_timestamp, title_id, device_type, raw_id)
    -- Path B: Guest fans
    ELSE
        INTO SONY_DE.SILVER.STG_FAN_GUEST (
            interaction_id, session_id, ip_address,
            region, country_code, event_type, event_timestamp, title_id, device_type, raw_id
        )
        VALUES (interaction_id, session_id, ip_address,
                region, country_code, event_type, event_timestamp, title_id, device_type, raw_id)
SELECT
    raw_id,
    raw_data:interaction_id::VARCHAR AS interaction_id,
    raw_data:fan_id::VARCHAR AS fan_id,
    raw_data:session_id::VARCHAR AS session_id,
    raw_data:account_type::VARCHAR AS account_type,
    raw_data:email::VARCHAR AS email,
    raw_data:first_name::VARCHAR AS first_name,
    raw_data:last_name::VARCHAR AS last_name,
    raw_data:ip_address::VARCHAR AS ip_address,
    raw_data:region::VARCHAR AS region,
    raw_data:country_code::VARCHAR AS country_code,
    raw_data:event_type::VARCHAR AS event_type,
    raw_data:event_timestamp::TIMESTAMP_NTZ AS event_timestamp,
    raw_data:title_id::VARCHAR AS title_id,
    raw_data:device_type::VARCHAR AS device_type
FROM SONY_DE.BRONZE.STREAM_FAN_INTERACTIONS;

-- =============================================================================
-- 4.4 CREATE INCREMENTAL TITLE/BOX OFFICE TASK DAG
-- =============================================================================

-- Clean box office staging table
CREATE TABLE IF NOT EXISTS SILVER.STG_BOX_OFFICE_CLEAN (
    record_id VARCHAR PRIMARY KEY,
    title_id VARCHAR,
    report_date DATE,
    theater_id VARCHAR,
    theater_name VARCHAR,
    theater_country VARCHAR,
    theater_region VARCHAR,
    tickets_sold INT,
    gross_revenue_local NUMBER(15,2),
    local_currency VARCHAR(3),
    exchange_rate_usd FLOAT,
    gross_revenue_usd NUMBER(15,2),
    screen_count INT,
    showtime_count INT,
    raw_id VARCHAR,
    ingestion_ts TIMESTAMP_NTZ
)
CHANGE_TRACKING = TRUE;

-- Parsed titles staging table
CREATE TABLE IF NOT EXISTS SILVER.STG_TITLES_PARSED (
    title_id VARCHAR PRIMARY KEY,
    title_name VARCHAR,
    franchise VARCHAR,
    release_year INT,
    genre VARCHAR,
    sub_genre VARCHAR,
    rating VARCHAR,
    runtime_minutes INT,
    imdb_score FLOAT,
    budget_usd NUMBER(15,2),
    production_status VARCHAR,
    studio VARCHAR,
    director VARCHAR,
    raw_id VARCHAR,
    ingestion_ts TIMESTAMP_NTZ
);

-- DAG Start Task (root - scheduled)
CREATE OR REPLACE TASK SILVER.TASK_DAG_START
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 6 * * * America/Los_Angeles'
    COMMENT = 'Daily DAG start - 6 AM PT'
AS
SELECT 'STG_INCREMENTAL_DAG_STARTED' AS status, CURRENT_TIMESTAMP() AS started_at;

-- Incremental Titles Task (child)
CREATE OR REPLACE TASK SILVER.TASK_INCR_TITLES
    WAREHOUSE = 'COMPUTE_WH'
    AFTER SILVER.TASK_DAG_START
AS
MERGE INTO SONY_DE.SILVER.STG_TITLES_PARSED AS tgt
USING (
    SELECT
        raw_data:title_id::varchar as title_id,
        raw_data:title_name::varchar as title_name,
        raw_data:franchise::varchar as franchise,
        raw_data:release_year::int as release_year,
        raw_data:genre::varchar as genre,
        raw_data:sub_genre::varchar as sub_genre,
        raw_data:rating::varchar as rating,
        raw_data:runtime_minutes::int as runtime_minutes,
        raw_data:imdb_score::float as imdb_score,
        raw_data:budget_usd::number(15,2) as budget_usd,
        raw_data:production_status::varchar as production_status,
        raw_data:studio::varchar as studio,
        raw_data:director::varchar as director,
        raw_id,
        ingestion_ts
    FROM SONY_DE.BRONZE.RAW_TITLE_METADATA
    WHERE ingestion_ts > (SELECT COALESCE(MAX(ingestion_ts), '1900-01-01') FROM SONY_DE.SILVER.STG_TITLES_PARSED)
) AS src
ON tgt.title_id = src.title_id
WHEN MATCHED THEN UPDATE SET
    title_name = src.title_name, franchise = src.franchise, imdb_score = src.imdb_score,
    budget_usd = src.budget_usd, ingestion_ts = src.ingestion_ts
WHEN NOT MATCHED THEN INSERT VALUES (
    src.title_id, src.title_name, src.franchise, src.release_year, src.genre,
    src.sub_genre, src.rating, src.runtime_minutes, src.imdb_score, src.budget_usd,
    src.production_status, src.studio, src.director, src.raw_id, src.ingestion_ts
);

-- Incremental Box Office Task (child)
CREATE OR REPLACE TASK SILVER.TASK_INCR_BOX_OFFICE
    WAREHOUSE = 'COMPUTE_WH'
    AFTER SILVER.TASK_DAG_START
AS
MERGE INTO SONY_DE.SILVER.STG_BOX_OFFICE_CLEAN AS tgt
USING (
    SELECT
        raw_data:record_id::varchar as record_id,
        raw_data:title_id::varchar as title_id,
        raw_data:report_date::date as report_date,
        raw_data:theater_id::varchar as theater_id,
        raw_data:theater_name::varchar as theater_name,
        raw_data:theater_country::varchar as theater_country,
        raw_data:theater_region::varchar as theater_region,
        raw_data:tickets_sold::int as tickets_sold,
        raw_data:gross_revenue_local::number(15,2) as gross_revenue_local,
        raw_data:local_currency::varchar as local_currency,
        raw_data:exchange_rate_usd::float as exchange_rate_usd,
        raw_data:gross_revenue_usd::number(15,2) as gross_revenue_usd,
        raw_data:screen_count::int as screen_count,
        raw_data:showtime_count::int as showtime_count,
        raw_id,
        ingestion_ts
    FROM SONY_DE.BRONZE.RAW_BOX_OFFICE
    WHERE ingestion_ts > (SELECT COALESCE(MAX(ingestion_ts), '1900-01-01') FROM SONY_DE.SILVER.STG_BOX_OFFICE_CLEAN)
) AS src
ON tgt.record_id = src.record_id
WHEN MATCHED THEN UPDATE SET
    title_id = src.title_id, report_date = src.report_date, tickets_sold = src.tickets_sold,
    gross_revenue_usd = src.gross_revenue_usd, ingestion_ts = src.ingestion_ts
WHEN NOT MATCHED THEN INSERT VALUES (
    src.record_id, src.title_id, src.report_date, src.theater_id, src.theater_name,
    src.theater_country, src.theater_region, src.tickets_sold, src.gross_revenue_local,
    src.local_currency, src.exchange_rate_usd, src.gross_revenue_usd, src.screen_count,
    src.showtime_count, src.raw_id, src.ingestion_ts
);

-- DAG Complete Task (finalizer)
CREATE OR REPLACE TASK SILVER.TASK_DAG_COMPLETE
    WAREHOUSE = 'COMPUTE_WH'
    AFTER SILVER.TASK_INCR_TITLES, SILVER.TASK_INCR_BOX_OFFICE
AS
SELECT 'STG_INCREMENTAL_DAG_COMPLETED' AS status, CURRENT_TIMESTAMP() AS completed_at;

-- =============================================================================
-- 4.5 ENABLE TASKS
-- =============================================================================

-- Enable routing task
ALTER TASK BRONZE.TASK_ROUTE_FAN_DATA RESUME;

-- Enable DAG (must enable children first, then root)
ALTER TASK SILVER.TASK_DAG_COMPLETE RESUME;
ALTER TASK SILVER.TASK_INCR_TITLES RESUME;
ALTER TASK SILVER.TASK_INCR_BOX_OFFICE RESUME;
ALTER TASK SILVER.TASK_DAG_START RESUME;

-- =============================================================================
-- 4.6 MANUAL EXECUTION (for initial load)
-- =============================================================================

-- Execute fan routing task manually
EXECUTE TASK BRONZE.TASK_ROUTE_FAN_DATA;

-- Execute DAG manually
EXECUTE TASK SILVER.TASK_DAG_START;

-- =============================================================================
-- 4.7 VERIFICATION
-- =============================================================================

-- Check stream status
SELECT SYSTEM$STREAM_HAS_DATA('SONY_DE.BRONZE.STREAM_FAN_INTERACTIONS') AS stream_has_data;

-- Check task states
SHOW TASKS IN DATABASE SONY_DE;

-- Check routed data counts
SELECT 'STG_FAN_VERIFIED' AS table_name, COUNT(*) AS row_count FROM SILVER.STG_FAN_VERIFIED
UNION ALL SELECT 'STG_FAN_GUEST', COUNT(*) FROM SILVER.STG_FAN_GUEST
UNION ALL SELECT 'STG_TITLES_PARSED', COUNT(*) FROM SILVER.STG_TITLES_PARSED
UNION ALL SELECT 'STG_BOX_OFFICE_CLEAN', COUNT(*) FROM SILVER.STG_BOX_OFFICE_CLEAN;

-- Expected:
-- STG_FAN_VERIFIED: ~36,000 rows (1/3 of fans)
-- STG_FAN_GUEST: ~72,000 rows (2/3 of fans)  
-- STG_TITLES_PARSED: 15 rows
-- STG_BOX_OFFICE_CLEAN: ~100,000 rows
