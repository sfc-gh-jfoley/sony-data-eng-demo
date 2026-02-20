-- =============================================================================
-- STEP 1: ENVIRONMENT SETUP
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- Run as: SYSADMIN or ACCOUNTADMIN
-- Time: ~1 minute
-- =============================================================================

USE ROLE SYSADMIN;

-- =============================================================================
-- 1.1 CREATE DATABASE
-- =============================================================================
CREATE DATABASE IF NOT EXISTS SONY_DE
    COMMENT = 'Sony Pictures Entertainment Data Engineering Demo - Medallion Architecture';

USE DATABASE SONY_DE;

-- =============================================================================
-- 1.2 CREATE SCHEMAS (Medallion Architecture)
-- =============================================================================

-- Bronze Layer: Raw immutable data landing zone
CREATE SCHEMA IF NOT EXISTS BRONZE
    COMMENT = 'Bronze Layer - Raw immutable data';

-- Silver Layer: Cleansed and staged data
CREATE SCHEMA IF NOT EXISTS SILVER
    COMMENT = 'Silver Layer - Staging/cleansed data';

-- Gold Layer: Dimensional model (dimensions and facts)
CREATE SCHEMA IF NOT EXISTS GOLD
    COMMENT = 'Gold Layer - Dimensional model (dims + facts)';

-- Platinum Layer: Analytics aggregations
CREATE SCHEMA IF NOT EXISTS PLATINUM
    COMMENT = 'Platinum Layer - Analytics aggregations';

-- Governance: Data quality metrics and monitoring
CREATE SCHEMA IF NOT EXISTS GOVERNANCE
    COMMENT = 'Data quality metrics and monitoring';

-- Secure: PII vault (restricted access)
CREATE SCHEMA IF NOT EXISTS SECURE
    COMMENT = 'PII vault - restricted access';

-- Analytics: Semantic views and Cortex Agent
CREATE SCHEMA IF NOT EXISTS ANALYTICS
    COMMENT = 'Semantic views and Cortex Agent';

-- =============================================================================
-- 1.3 CREATE WAREHOUSE
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'General compute warehouse for SONY_DE';

-- =============================================================================
-- 1.4 VERIFICATION
-- =============================================================================
SHOW SCHEMAS IN DATABASE SONY_DE;
SHOW WAREHOUSES LIKE 'COMPUTE_WH';

-- Expected output:
-- Schemas: BRONZE, SILVER, GOLD, PLATINUM, GOVERNANCE, SECURE, ANALYTICS, PUBLIC, INFORMATION_SCHEMA
-- Warehouse: COMPUTE_WH (XSMALL, AUTO_SUSPEND=60)
