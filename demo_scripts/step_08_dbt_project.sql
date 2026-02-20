-- =============================================================================
-- STEP 8: dbt PROJECT SETUP
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- 
-- This is a DOCUMENTATION step. The dbt project already exists.
-- dbt is used here to TEST Dynamic Tables, NOT to materialize them.
--
-- Pattern: Dynamic Tables handle transformation, dbt handles testing
--
-- =============================================================================

-- =============================================================================
-- 8.1 dbt PROJECT STRUCTURE
-- =============================================================================
/*
sony_dbt/sony_spe/
├── dbt_project.yml            # Project config (ephemeral models for testing)
├── profiles.yml               # Connection config (uses Snowflake connection)
├── packages.yml               # Dependencies (dbt_utils, dbt_expectations)
│
├── models/
│   ├── staging/
│   │   ├── sources.yml        # Source definitions (Bronze + Silver)
│   │   ├── schema.yml         # Column tests
│   │   ├── stg_fans_unified.sql
│   │   ├── stg_titles_parsed.sql
│   │   └── stg_box_office_*.sql
│   │
│   ├── intermediate/
│   │   ├── int_fans_enriched.sql
│   │   └── int_daily_performance.sql
│   │
│   ├── marts/
│   │   ├── dims/
│   │   │   ├── dim_fans.sql
│   │   │   ├── dim_titles.sql
│   │   │   └── schema.yml     # Dim tests
│   │   └── facts/
│   │       ├── fact_daily_performance.sql
│   │       └── schema.yml     # Fact tests
│   │
│   └── tests/
│       └── dynamic_tables.yml  # Tests against DTs as SOURCES
│
├── tests/                      # Custom data tests
│   ├── assert_no_fan_data_loss.sql
│   ├── assert_stream_routing_correct.sql
│   ├── assert_dynamic_tables_populated.sql
│   ├── assert_fact_title_referential_integrity.sql
│   └── assert_no_negative_revenue.sql
│
└── snapshots/
    └── snap_dim_fans.sql       # SCD Type 2 snapshot

Key Design Decision:
- All models are +materialized: ephemeral (no tables created)
- Dynamic Tables are registered as SOURCES
- dbt tests run against DT sources directly
- This gives data quality testing WITHOUT duplicate materialization
*/

-- =============================================================================
-- 8.2 SAMPLE SOURCE CONFIG (models/tests/dynamic_tables.yml)
-- =============================================================================
/*
sources:
  - name: gold
    database: SONY_DE
    schema: GOLD
    tables:
      - name: dt_dim_fans
        columns:
          - name: fan_key
            tests: [unique, not_null]
          - name: fan_id
            tests: [unique, not_null]
          - name: account_type
            tests:
              - accepted_values:
                  values: ['VERIFIED', 'GUEST']
      
      - name: dt_dim_titles
        columns:
          - name: imdb_score
            tests: [not_null]  # Note: May fail for unreleased titles
      
      - name: dt_fact_daily_performance
        columns:
          - name: title_id
            tests:
              - relationships:
                  to: source('gold', 'dt_dim_titles')
                  field: title_id
*/

-- =============================================================================
-- 8.3 CUSTOM DATA TESTS
-- =============================================================================

-- Test: assert_no_fan_data_loss.sql
/*
WITH raw_fans AS (
    SELECT COUNT(DISTINCT PARSE_JSON(raw_data):fan_id::STRING) AS raw_count
    FROM {{ source('raw', 'raw_fan_interactions') }}
),
dim_fans AS (
    SELECT COUNT(DISTINCT fan_id) AS dim_count
    FROM {{ ref('dim_fans') }}
)
SELECT raw_count, dim_count
FROM raw_fans, dim_fans
WHERE dim_count = 0 OR dim_count > raw_count;
*/

-- Test: assert_dynamic_tables_populated.sql
/*
SELECT name, rows
FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
WHERE database_name = 'SONY_DE'
  AND rows = 0;  -- Fail if any DT has 0 rows after refresh
*/

-- =============================================================================
-- 8.4 dbt COMMANDS
-- =============================================================================

-- Install dependencies
-- cd sony_dbt/sony_spe && dbt deps

-- Run tests against Dynamic Tables (as sources)
-- dbt test --select source:gold.* source:platinum.*

-- Run custom data tests
-- dbt test --select test_type:data

-- Run all tests
-- dbt test

-- Expected test results:
-- - PASS: Most tests pass
-- - FAIL: dt_dim_titles.imdb_score not_null (4 failures for unreleased titles)

-- =============================================================================
-- 8.5 SCHEDULED dbt TASK
-- =============================================================================

CREATE OR REPLACE TASK SILVER.TASK_DBT_TESTS
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = 'USING CRON 0 7 * * * America/Los_Angeles'
    COMMENT = 'Runs dbt tests against Dynamic Tables daily at 7 AM PT'
AS
EXECUTE DBT PROJECT SONY_DE.SILVER.SONY_SPE_PIPELINE
    ARGS = 'test --select source:gold.* source:platinum.*';

-- Note: Task requires DBT project to be deployed to Snowflake
-- For local testing, use: cd sony_dbt/sony_spe && dbt test

-- =============================================================================
-- 8.6 VERIFICATION
-- =============================================================================

-- Check if dbt project exists
-- ls -la sony_dbt/sony_spe/

-- Test dbt connection
-- cd sony_dbt/sony_spe && dbt debug

-- Run tests
-- cd sony_dbt/sony_spe && dbt test --select source:gold.*

-- Expected:
-- dbt_project.yml exists
-- models/, tests/ directories populated
-- dbt test runs and shows pass/fail for each test
