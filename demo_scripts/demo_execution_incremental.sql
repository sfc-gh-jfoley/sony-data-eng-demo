-- =============================================================================
-- DEMO EXECUTION GUIDE: Incremental Pipeline Demo
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- This script guides you through demonstrating incremental data flows
-- through the Medallion Architecture pipeline
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- DEMO FLOW OVERVIEW
-- =============================================================================
-- 
-- BATCH 1: Initial Load
--   → Establishes baseline (10K fans, 15 titles, 50 theaters × 30 days)
--   → Stream is created and starts tracking changes
--
-- BATCH 2: Day 2 Incremental  
--   → Adds 5K fans (3K new + 2K returning)
--   → Adds 10 new theaters with today's data
--   → Stream captures fan changes → Task routes → DTs refresh
--
-- BATCH 3: Release Weekend
--   → Title update: Karate Kid released with IMDB score!
--   → 4K new fans with heavy Karate Kid activity
--   → Huge box office numbers for opening weekend
--   → Shows aggregates updating in PLATINUM layer
--
-- =============================================================================

-- =============================================================================
-- STEP 1: RESET FOR DEMO (Optional - for fresh start)
-- =============================================================================

-- Run these to reset:
-- TRUNCATE TABLE BRONZE.RAW_FAN_INTERACTIONS;
-- TRUNCATE TABLE BRONZE.RAW_TITLE_METADATA;
-- TRUNCATE TABLE BRONZE.RAW_BOX_OFFICE;

-- =============================================================================
-- STEP 2: RUN BATCH 1 (Initial Load)
-- =============================================================================

-- Execute: demo_scripts/data_batch_01_initial.sql
-- Then verify:

SELECT 'After Batch 1' AS checkpoint, 
       'RAW Tables' AS layer,
       (SELECT COUNT(*) FROM BRONZE.RAW_FAN_INTERACTIONS) AS fan_interactions,
       (SELECT COUNT(*) FROM BRONZE.RAW_TITLE_METADATA) AS title_metadata,
       (SELECT COUNT(*) FROM BRONZE.RAW_BOX_OFFICE) AS box_office;

-- Check stream status (should have data from initial load)
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_FAN_INTERACTIONS') AS stream_has_data;

-- =============================================================================
-- STEP 3: TRIGGER TASK TO PROCESS STREAM
-- =============================================================================

-- Manually execute the routing task
EXECUTE TASK BRONZE.TASK_ROUTE_FAN_DATA;

-- Wait a few seconds, then check staging tables
SELECT 'SILVER Staging' AS layer,
       (SELECT COUNT(*) FROM SILVER.STG_FAN_VERIFIED) AS verified_fans,
       (SELECT COUNT(*) FROM SILVER.STG_FAN_GUEST) AS guest_fans;

-- =============================================================================
-- STEP 4: CHECK DYNAMIC TABLE REFRESH
-- =============================================================================

-- Check DT status
SELECT NAME, TARGET_LAG, REFRESH_MODE, DATA_TIMESTAMP, LAST_COMPLETED_REFRESH_TIME
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE NAME LIKE 'DT_%' OR NAME LIKE 'AGG_%'
ORDER BY LAST_COMPLETED_REFRESH_TIME DESC
LIMIT 15;

-- Force refresh if needed (DTs with 1-minute lag auto-refresh)
-- ALTER DYNAMIC TABLE SILVER.DT_STG_FANS_UNIFIED REFRESH;

-- Check pipeline progression
SELECT 
    'SILVER - DT_STG_FANS_UNIFIED' AS dt_name, COUNT(*) AS rows FROM SILVER.DT_STG_FANS_UNIFIED UNION ALL
SELECT 'GOLD - DT_DIM_FANS', COUNT(*) FROM GOLD.DT_DIM_FANS UNION ALL
SELECT 'GOLD - DT_DIM_TITLES', COUNT(*) FROM GOLD.DT_DIM_TITLES UNION ALL
SELECT 'PLATINUM - AGG_FRANCHISE_PERFORMANCE', COUNT(*) FROM PLATINUM.AGG_FRANCHISE_PERFORMANCE;

-- =============================================================================
-- STEP 5: RUN BATCH 2 (Incremental Load)
-- =============================================================================

-- Execute: demo_scripts/data_batch_02_incremental.sql
-- Then verify growth:

SELECT 
    SOURCE_FILE,
    COUNT(*) AS records
FROM BRONZE.RAW_FAN_INTERACTIONS
GROUP BY SOURCE_FILE
ORDER BY SOURCE_FILE;

-- Stream should have new data
SELECT SYSTEM$STREAM_HAS_DATA('BRONZE.STREAM_FAN_INTERACTIONS') AS stream_has_data_batch2;

-- Run task again
EXECUTE TASK BRONZE.TASK_ROUTE_FAN_DATA;

-- Watch staging tables grow
SELECT 'After Batch 2' AS checkpoint,
       (SELECT COUNT(*) FROM SILVER.STG_FAN_VERIFIED) AS verified_fans,
       (SELECT COUNT(*) FROM SILVER.STG_FAN_GUEST) AS guest_fans;

-- =============================================================================
-- STEP 6: RUN BATCH 3 (Release Weekend!)
-- =============================================================================

-- Execute: demo_scripts/data_batch_03_release.sql
-- Then verify title updates:

SELECT 
    SOURCE_FILE,
    COUNT(*) AS titles,
    SUM(CASE WHEN RAW_DATA:production_status::STRING = 'RELEASED' THEN 1 ELSE 0 END) AS released
FROM BRONZE.RAW_TITLE_METADATA
GROUP BY SOURCE_FILE;

-- Check Karate Kid specifically
SELECT 
    RAW_DATA:title_name::STRING AS title,
    RAW_DATA:production_status::STRING AS status,
    RAW_DATA:imdb_score::FLOAT AS imdb_score,
    SOURCE_FILE
FROM BRONZE.RAW_TITLE_METADATA
WHERE RAW_DATA:title_id::STRING = 'SPE015';

-- Process fans
EXECUTE TASK BRONZE.TASK_ROUTE_FAN_DATA;

-- =============================================================================
-- STEP 7: VERIFY FULL PIPELINE DATA LINEAGE
-- =============================================================================

-- Show complete data lineage by batch file
SELECT 
    'BRONZE' AS layer,
    'RAW_FAN_INTERACTIONS' AS table_name,
    SOURCE_FILE,
    COUNT(*) AS records,
    MIN(INGESTED_AT) AS first_record,
    MAX(INGESTED_AT) AS last_record
FROM BRONZE.RAW_FAN_INTERACTIONS
GROUP BY SOURCE_FILE

UNION ALL

SELECT 'BRONZE', 'RAW_BOX_OFFICE', SOURCE_FILE, COUNT(*), MIN(INGESTED_AT), MAX(INGESTED_AT)
FROM BRONZE.RAW_BOX_OFFICE
GROUP BY SOURCE_FILE

UNION ALL

SELECT 'BRONZE', 'RAW_TITLE_METADATA', SOURCE_FILE, COUNT(*), MIN(INGESTED_AT), MAX(INGESTED_AT)
FROM BRONZE.RAW_TITLE_METADATA
GROUP BY SOURCE_FILE

ORDER BY layer, table_name, SOURCE_FILE;

-- =============================================================================
-- STEP 8: SHOW BUSINESS INSIGHTS (Platinum Layer)
-- =============================================================================

-- Franchise performance after Karate Kid release
SELECT 
    FRANCHISE,
    TOTAL_FANS,
    TOTAL_INTERACTIONS,
    TOTAL_BOX_OFFICE_USD,
    AVG_IMDB_SCORE,
    LAST_UPDATED
FROM PLATINUM.AGG_FRANCHISE_PERFORMANCE
ORDER BY TOTAL_BOX_OFFICE_USD DESC;

-- Fan lifetime value by engagement tier
SELECT 
    ENGAGEMENT_TIER,
    COUNT(*) AS fan_count,
    ROUND(AVG(TOTAL_INTERACTIONS), 1) AS avg_interactions,
    ROUND(AVG(LIFETIME_VALUE_USD), 2) AS avg_ltv
FROM PLATINUM.AGG_FAN_LIFETIME_VALUE
GROUP BY ENGAGEMENT_TIER
ORDER BY avg_ltv DESC;

-- =============================================================================
-- STEP 9: DATA QUALITY CHECK
-- =============================================================================

-- Show DMF violations
SELECT 
    MEASUREMENT_TIME,
    METRIC_DATABASE || '.' || METRIC_SCHEMA || '.' || METRIC_NAME AS metric,
    TABLE_DATABASE || '.' || TABLE_SCHEMA || '.' || TABLE_NAME AS target_table,
    VALUE AS violations
FROM GOVERNANCE.V_DATA_QUALITY_METRICS
WHERE VALUE > 0
ORDER BY MEASUREMENT_TIME DESC
LIMIT 20;

-- =============================================================================
-- KEY DEMO TALKING POINTS
-- =============================================================================
-- 
-- 1. DATA LINEAGE: SOURCE_FILE column tracks exactly which batch each record came from
-- 
-- 2. INCREMENTAL PROCESSING: Stream + Task pattern only processes NEW data
-- 
-- 3. DECLARATIVE REFRESH: Dynamic Tables automatically stay current (LAG setting)
-- 
-- 4. QUALITY GATES: DMFs continuously monitor data quality across pipeline
-- 
-- 5. BUSINESS VALUE: Platinum aggregates always reflect latest data for decisions
-- 
-- =============================================================================
