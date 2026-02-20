-- =============================================================================
-- STEP 7: DATA METRIC FUNCTIONS (Governance)
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- Run as: SYSADMIN
-- Time: ~2 minutes
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE SCHEMA GOVERNANCE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 7.1 CREATE DATA METRIC FUNCTIONS (DMFs)
-- =============================================================================

-- Valid IMDB Score (0-10 range)
CREATE OR REPLACE DATA METRIC FUNCTION VALID_IMDB_SCORE(ARG_T TABLE(imdb_score FLOAT))
    RETURNS NUMBER
    COMMENT = 'Returns count of rows with invalid IMDB scores (outside 0-10 range)'
AS
$$
SELECT COUNT_IF(imdb_score < 0 OR imdb_score > 10 OR imdb_score IS NULL) FROM ARG_T
$$;

-- Valid Email Hash (SHA256 format - 64 hex chars)
CREATE OR REPLACE DATA METRIC FUNCTION VALID_EMAIL_HASH(ARG_T TABLE(email_hash VARCHAR))
    RETURNS NUMBER
    COMMENT = 'Returns count of rows with invalid SHA256 email hashes'
AS
$$
SELECT COUNT_IF(
    email_hash IS NULL 
    OR LENGTH(email_hash) != 64 
    OR NOT REGEXP_LIKE(email_hash, '^[a-f0-9]{64}$')
) FROM ARG_T
$$;

-- Valid Country Code (ISO 3166-1 alpha-3)
CREATE OR REPLACE DATA METRIC FUNCTION VALID_COUNTRY_CODE(ARG_T TABLE(country_code VARCHAR))
    RETURNS NUMBER
    COMMENT = 'Returns count of rows with invalid country codes (not 3 uppercase letters)'
AS
$$
SELECT COUNT_IF(
    country_code IS NULL 
    OR LENGTH(country_code) != 3 
    OR NOT REGEXP_LIKE(country_code, '^[A-Z]{3}$')
) FROM ARG_T
$$;

-- Positive Revenue (no negative values)
CREATE OR REPLACE DATA METRIC FUNCTION POSITIVE_REVENUE(ARG_T TABLE(revenue NUMBER))
    RETURNS NUMBER
    COMMENT = 'Returns count of rows with negative revenue values'
AS
$$
SELECT COUNT_IF(revenue < 0) FROM ARG_T
$$;

-- NULL Count (generic)
CREATE OR REPLACE DATA METRIC FUNCTION NULL_COUNT(ARG_T TABLE(col VARCHAR))
    RETURNS NUMBER
    COMMENT = 'Returns count of NULL values in specified column'
AS
$$
SELECT COUNT_IF(col IS NULL) FROM ARG_T
$$;

-- Duplicate Count (generic)
CREATE OR REPLACE DATA METRIC FUNCTION DUPLICATE_COUNT(ARG_T TABLE(col VARCHAR))
    RETURNS NUMBER
    COMMENT = 'Returns count of duplicate values (total rows - distinct values)'
AS
$$
SELECT COUNT(*) - COUNT(DISTINCT col) FROM ARG_T
$$;

-- =============================================================================
-- 7.2 CREATE GOVERNANCE VIEWS
-- =============================================================================

-- Data Quality Dashboard View (runs DMFs against Gold tables)
CREATE OR REPLACE VIEW V_DATA_QUALITY_DASHBOARD AS
SELECT 
    'DT_DIM_TITLES' AS table_name,
    'VALID_IMDB_SCORE' AS metric,
    'IMDB scores outside 0-10 range' AS description,
    SONY_DE.GOVERNANCE.VALID_IMDB_SCORE(SELECT IMDB_SCORE FROM SONY_DE.GOLD.DT_DIM_TITLES) AS violation_count,
    CURRENT_TIMESTAMP() AS measured_at
UNION ALL
SELECT 'DT_DIM_FANS', 'VALID_COUNTRY_CODE', 'Invalid country codes (not 3 uppercase letters)',
       SONY_DE.GOVERNANCE.VALID_COUNTRY_CODE(SELECT COUNTRY_CODE FROM SONY_DE.GOLD.DT_DIM_FANS),
       CURRENT_TIMESTAMP()
UNION ALL
SELECT 'DT_DIM_FANS', 'VALID_EMAIL_HASH', 'Invalid SHA256 email hashes',
       SONY_DE.GOVERNANCE.VALID_EMAIL_HASH(SELECT EMAIL_HASH FROM SONY_DE.GOLD.DT_DIM_FANS),
       CURRENT_TIMESTAMP()
UNION ALL
SELECT 'DT_DIM_FANS', 'NULL_COUNT (FAN_ID)', 'NULL fan IDs',
       SONY_DE.GOVERNANCE.NULL_COUNT(SELECT FAN_ID FROM SONY_DE.GOLD.DT_DIM_FANS),
       CURRENT_TIMESTAMP()
UNION ALL
SELECT 'DT_DIM_FANS', 'DUPLICATE_COUNT (FAN_ID)', 'Duplicate fan IDs',
       SONY_DE.GOVERNANCE.DUPLICATE_COUNT(SELECT FAN_ID FROM SONY_DE.GOLD.DT_DIM_FANS),
       CURRENT_TIMESTAMP()
UNION ALL
SELECT 'DT_FACT_DAILY_PERFORMANCE', 'POSITIVE_REVENUE', 'Negative revenue values',
       SONY_DE.GOVERNANCE.POSITIVE_REVENUE(SELECT TOTAL_GROSS_USD FROM SONY_DE.GOLD.DT_FACT_DAILY_PERFORMANCE),
       CURRENT_TIMESTAMP();

-- Layer Row Counts View (for monitoring)
CREATE OR REPLACE VIEW V_LAYER_ROW_COUNTS AS
SELECT 'BRONZE' AS layer, 'BRONZE.RAW_FAN_INTERACTIONS' AS table_name,
       (SELECT COUNT(*) FROM SONY_DE.BRONZE.RAW_FAN_INTERACTIONS) AS row_count
UNION ALL
SELECT 'BRONZE', 'BRONZE.RAW_TITLE_METADATA', (SELECT COUNT(*) FROM SONY_DE.BRONZE.RAW_TITLE_METADATA)
UNION ALL
SELECT 'BRONZE', 'BRONZE.RAW_BOX_OFFICE', (SELECT COUNT(*) FROM SONY_DE.BRONZE.RAW_BOX_OFFICE)
UNION ALL
SELECT 'SILVER', 'SILVER.STG_FAN_VERIFIED', (SELECT COUNT(*) FROM SONY_DE.SILVER.STG_FAN_VERIFIED)
UNION ALL
SELECT 'SILVER', 'SILVER.STG_FAN_GUEST', (SELECT COUNT(*) FROM SONY_DE.SILVER.STG_FAN_GUEST)
UNION ALL
SELECT 'GOLD', 'GOLD.DT_DIM_FANS', (SELECT COUNT(*) FROM SONY_DE.GOLD.DT_DIM_FANS)
UNION ALL
SELECT 'GOLD', 'GOLD.DT_DIM_TITLES', (SELECT COUNT(*) FROM SONY_DE.GOLD.DT_DIM_TITLES)
UNION ALL
SELECT 'GOLD', 'GOLD.DT_FACT_DAILY_PERFORMANCE', (SELECT COUNT(*) FROM SONY_DE.GOLD.DT_FACT_DAILY_PERFORMANCE)
UNION ALL
SELECT 'PLATINUM', 'PLATINUM.AGG_FRANCHISE_PERFORMANCE', (SELECT COUNT(*) FROM SONY_DE.PLATINUM.AGG_FRANCHISE_PERFORMANCE)
UNION ALL
SELECT 'PLATINUM', 'PLATINUM.AGG_FAN_LIFETIME_VALUE', (SELECT COUNT(*) FROM SONY_DE.PLATINUM.AGG_FAN_LIFETIME_VALUE);

-- =============================================================================
-- 7.3 VERIFICATION
-- =============================================================================

-- Test DMFs directly
SELECT 
    'VALID_IMDB_SCORE' AS dmf,
    SONY_DE.GOVERNANCE.VALID_IMDB_SCORE(SELECT IMDB_SCORE FROM SONY_DE.GOLD.DT_DIM_TITLES) AS violations;

-- Test Data Quality Dashboard
SELECT * FROM SONY_DE.GOVERNANCE.V_DATA_QUALITY_DASHBOARD;

-- Test Layer Row Counts
SELECT * FROM SONY_DE.GOVERNANCE.V_LAYER_ROW_COUNTS;

-- List all DMFs
SHOW USER FUNCTIONS IN SCHEMA SONY_DE.GOVERNANCE;

-- Expected results:
-- 6 DMFs created
-- V_DATA_QUALITY_DASHBOARD: 6 rows (one per metric)
-- V_LAYER_ROW_COUNTS: 10 rows (one per table)
-- All violation_count should be 0 for clean data
