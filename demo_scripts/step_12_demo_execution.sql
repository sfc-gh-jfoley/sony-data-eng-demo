-- =============================================================================
-- STEP 12: DEMO EXECUTION SCRIPT
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- 
-- This script provides a single-execution demo of all components.
-- Run this to verify the entire pipeline is working.
--
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 12.1 VERIFICATION CHECKLIST
-- =============================================================================

-- [1] Database and Schemas
SELECT 'ENVIRONMENT' AS section, 'Database exists' AS check_item, 
       CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM INFORMATION_SCHEMA.DATABASES WHERE DATABASE_NAME = 'SONY_DE';

-- [2] Schemas (expect 7+)
SELECT 'ENVIRONMENT' AS section, 'Schemas exist' AS check_item,
       CASE WHEN COUNT(*) >= 7 THEN '✅ PASS (' || COUNT(*) || ' schemas)' ELSE '❌ FAIL' END AS status
FROM SONY_DE.INFORMATION_SCHEMA.SCHEMATA 
WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA');

-- [3] Bronze Tables (expect 3)
SELECT 'BRONZE LAYER' AS section, 'Raw tables exist' AS check_item,
       CASE WHEN COUNT(*) >= 3 THEN '✅ PASS (' || COUNT(*) || ' tables)' ELSE '❌ FAIL' END AS status
FROM SONY_DE.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'BRONZE' AND TABLE_TYPE = 'BASE TABLE';

-- [4] Bronze Data
SELECT 'BRONZE LAYER' AS section, 'Raw data populated' AS check_item,
       CASE WHEN SUM(row_count) > 100000 THEN '✅ PASS (' || SUM(row_count) || ' rows)' ELSE '❌ FAIL' END AS status
FROM (
    SELECT COUNT(*) AS row_count FROM SONY_DE.BRONZE.RAW_FAN_INTERACTIONS
    UNION ALL SELECT COUNT(*) FROM SONY_DE.BRONZE.RAW_TITLE_METADATA
    UNION ALL SELECT COUNT(*) FROM SONY_DE.BRONZE.RAW_BOX_OFFICE
);

-- [5] Stream
SELECT 'STREAMS/TASKS' AS section, 'Stream created' AS check_item,
       CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM SONY_DE.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'STREAM' AND TABLE_SCHEMA = 'BRONZE';

-- [6] Tasks
SELECT 'STREAMS/TASKS' AS section, 'Tasks created' AS check_item,
       CASE WHEN COUNT(*) >= 4 THEN '✅ PASS (' || COUNT(*) || ' tasks)' ELSE '❌ FAIL' END AS status
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY()) 
WHERE DATABASE_NAME = 'SONY_DE';

-- [7] Silver Staging Tables
SELECT 'SILVER LAYER' AS section, 'Staging tables populated' AS check_item,
       CASE WHEN SUM(row_count) > 50000 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM (
    SELECT COUNT(*) AS row_count FROM SONY_DE.SILVER.STG_FAN_VERIFIED
    UNION ALL SELECT COUNT(*) FROM SONY_DE.SILVER.STG_FAN_GUEST
);

-- [8] Dynamic Tables (expect 10)
SELECT 'DYNAMIC TABLES' AS section, 'DTs created' AS check_item,
       CASE WHEN COUNT(*) >= 10 THEN '✅ PASS (' || COUNT(*) || ' DTs)' ELSE '❌ FAIL' END AS status
FROM SONY_DE.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_TYPE = 'DYNAMIC TABLE';

-- [9] Gold Layer
SELECT 'GOLD LAYER' AS section, 'Dimensions populated' AS check_item,
       CASE WHEN COUNT(*) > 25000 THEN '✅ PASS (' || COUNT(*) || ' fans)' ELSE '❌ FAIL' END AS status
FROM SONY_DE.GOLD.DT_DIM_FANS;

-- [10] Platinum Layer
SELECT 'PLATINUM LAYER' AS section, 'Aggregations populated' AS check_item,
       CASE WHEN COUNT(*) > 10 THEN '✅ PASS (' || COUNT(*) || ' franchises)' ELSE '❌ FAIL' END AS status
FROM SONY_DE.PLATINUM.AGG_FRANCHISE_PERFORMANCE;

-- [11] DMFs
SELECT 'GOVERNANCE' AS section, 'DMFs created' AS check_item,
       CASE WHEN COUNT(*) >= 6 THEN '✅ PASS (' || COUNT(*) || ' DMFs)' ELSE '❌ FAIL' END AS status
FROM SONY_DE.INFORMATION_SCHEMA.FUNCTIONS 
WHERE FUNCTION_SCHEMA = 'GOVERNANCE' AND FUNCTION_NAME LIKE '%COUNT%' OR FUNCTION_NAME LIKE '%VALID%';

-- [12] Governance Views
SELECT 'GOVERNANCE' AS section, 'Governance views exist' AS check_item,
       CASE WHEN COUNT(*) >= 2 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM SONY_DE.INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'GOVERNANCE';

-- [13] Semantic Stage
SELECT 'CORTEX' AS section, 'Semantic model staged' AS check_item,
       CASE WHEN COUNT(*) > 0 THEN '✅ PASS' ELSE '❌ FAIL' END AS status
FROM DIRECTORY(@SONY_DE.ANALYTICS.SEMANTIC_STAGE);

-- =============================================================================
-- 12.2 DATA QUALITY SUMMARY
-- =============================================================================

SELECT 
    table_name,
    metric,
    violation_count,
    CASE WHEN violation_count = 0 THEN '✅' ELSE '❌' END AS status
FROM SONY_DE.GOVERNANCE.V_DATA_QUALITY_DASHBOARD
ORDER BY violation_count DESC;

-- =============================================================================
-- 12.3 LAYER ROW COUNTS
-- =============================================================================

SELECT * FROM SONY_DE.GOVERNANCE.V_LAYER_ROW_COUNTS
ORDER BY 
    CASE layer 
        WHEN 'BRONZE' THEN 1 
        WHEN 'SILVER' THEN 2 
        WHEN 'GOLD' THEN 3 
        WHEN 'PLATINUM' THEN 4 
    END;

-- =============================================================================
-- 12.4 DYNAMIC TABLE STATUS
-- =============================================================================

SHOW DYNAMIC TABLES IN DATABASE SONY_DE;

-- =============================================================================
-- 12.5 PIPELINE HEALTH CHECK
-- =============================================================================

-- Stream status
SELECT 
    'STREAM_FAN_INTERACTIONS' AS stream_name,
    SYSTEM$STREAM_HAS_DATA('SONY_DE.BRONZE.STREAM_FAN_INTERACTIONS') AS has_pending_data;

-- Task status
SHOW TASKS IN DATABASE SONY_DE;

-- =============================================================================
-- 12.6 SAMPLE QUERIES (Demo these to your coworker)
-- =============================================================================

-- Query 1: Top franchises by revenue
SELECT 
    franchise,
    genre,
    title_count,
    total_gross_usd,
    total_tickets,
    total_streams
FROM SONY_DE.PLATINUM.AGG_FRANCHISE_PERFORMANCE
ORDER BY total_gross_usd DESC
LIMIT 5;

-- Query 2: Fan engagement by region
SELECT 
    region,
    account_type,
    fan_count,
    avg_interactions,
    avg_titles_engaged,
    active_last_30_days
FROM SONY_DE.PLATINUM.AGG_FAN_LIFETIME_VALUE
ORDER BY fan_count DESC;

-- Query 3: Top rated movies
SELECT 
    title_name,
    franchise,
    release_year,
    imdb_score,
    budget_usd,
    rating
FROM SONY_DE.GOLD.DT_DIM_TITLES
WHERE imdb_score IS NOT NULL
ORDER BY imdb_score DESC
LIMIT 10;

-- Query 4: Daily performance trend
SELECT 
    title_id,
    region,
    report_date,
    total_tickets_sold,
    total_gross_usd,
    stream_count
FROM SONY_DE.GOLD.DT_FACT_DAILY_PERFORMANCE
WHERE report_date >= DATEADD('day', -7, CURRENT_DATE())
ORDER BY report_date DESC, total_gross_usd DESC
LIMIT 20;

-- =============================================================================
-- 12.7 DEMO SCRIPT SUMMARY
-- =============================================================================
/*
Demo Flow (30 min):

1. INTRO (5 min)
   - Show architecture diagram (Medallion: Bronze → Silver → Gold → Platinum)
   - Explain hybrid pipeline: Imperative (Streams/Tasks) + Declarative (DTs)

2. DATA FLOW (10 min)
   - Show Bronze tables and JSON structure
   - Run Step 3 INSERT to add new data
   - Watch Stream capture changes
   - Show Task routing to STG_FAN_VERIFIED and STG_FAN_GUEST
   - Observe Dynamic Tables refresh automatically

3. DATA QUALITY (5 min)
   - Show DMF violations in V_DATA_QUALITY_DASHBOARD
   - Explain IMDB score violations (NULL for unreleased titles)
   - Run dbt tests: cd sony_dbt/sony_spe && dbt test

4. ANALYTICS (5 min)
   - Query Platinum aggregations
   - Show Streamlit dashboard (if deployed)
   - Demo Cortex Analyst with semantic model

5. Q&A (5 min)
   - RBAC walkthrough
   - Scaling considerations
   - Next steps

Key Talking Points:
- INSERT ALL fan-out pattern for conditional routing
- QUALIFY for deduplication
- DOWNSTREAM lag for cascading refreshes
- DMFs for data quality at scale
- dbt for testing Dynamic Tables (ephemeral models)
- Semantic models enable natural language queries
*/

COMMIT;
-- Demo ready!
