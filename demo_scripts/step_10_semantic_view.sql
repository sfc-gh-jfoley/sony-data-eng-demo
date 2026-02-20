-- =============================================================================
-- STEP 10: SEMANTIC VIEW & CORTEX ANALYST
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- 
-- Cortex Analyst enables natural language querying over structured data
-- using semantic models. This step sets up the semantic model for text-to-SQL.
--
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 10.1 CREATE SEMANTIC STAGE
-- =============================================================================

CREATE STAGE IF NOT EXISTS SEMANTIC_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for semantic model YAML files';

-- =============================================================================
-- 10.2 SEMANTIC MODEL STRUCTURE
-- =============================================================================
/*
Semantic Model: sony_entertainment_analytics_semantic_model.yaml

Tables defined:
├── franchise_performance (PLATINUM.AGG_FRANCHISE_PERFORMANCE)
│   ├── Dimensions: franchise, genre, first_report_date, last_report_date
│   ├── Time Dimensions: refreshed_at
│   └── Facts: title_count, total_tickets, total_gross_usd, avg_ticket_price, total_streams, total_unique_fans
│
├── fan_lifetime_value (PLATINUM.AGG_FAN_LIFETIME_VALUE)
│   ├── Dimensions: region, country_code, account_type, top_device
│   ├── Time Dimensions: refreshed_at
│   └── Facts: fan_count, avg_interactions, avg_titles_engaged, avg_active_days, active_last_30_days, active_last_7_days
│
├── dim_titles (GOLD.DT_DIM_TITLES)
│   ├── Primary Key: title_id
│   ├── Dimensions: title_id, title_name, franchise, release_year, genre, sub_genre, rating, production_status, studio, director
│   └── Facts: runtime_minutes, imdb_score, budget_usd
│
├── dim_fans (GOLD.DT_DIM_FANS)
│   ├── Dimensions: fan_id, account_type, region, country_code, preferred_device
│   ├── Time Dimensions: signup_date, last_active_date
│   └── Facts: lifetime_interactions, titles_engaged
│
└── fact_daily_performance (GOLD.DT_FACT_DAILY_PERFORMANCE)
    ├── Primary Key: performance_key
    ├── Dimensions: title_id, region
    ├── Time Dimensions: report_date
    └── Facts: total_tickets_sold, total_gross_usd, theater_count, screen_count, avg_ticket_price_usd, stream_count, unique_fans

Key features:
- Synonyms for natural language understanding
- Sample values for context
- Verified queries for common use cases
*/

-- =============================================================================
-- 10.3 UPLOAD SEMANTIC MODEL TO STAGE
-- =============================================================================

-- From CLI (run in project root):
-- PUT file://semantic_view_creation/sony_entertainment_analytics_semantic_model.yaml @SONY_DE.ANALYTICS.SEMANTIC_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Verify upload
LIST @SONY_DE.ANALYTICS.SEMANTIC_STAGE;

-- =============================================================================
-- 10.4 CREATE SEMANTIC VIEW (Optional - Alternative to Stage File)
-- =============================================================================
/*
-- Semantic views provide a SQL-based alternative to YAML files
-- They reference the same underlying tables but are defined in SQL

CREATE OR REPLACE SEMANTIC VIEW SONY_ENTERTAINMENT_ANALYTICS_SV
AS
SELECT * FROM SONY_DE.GOLD.DT_DIM_TITLES
-- WITH SEMANTIC MODEL = '@SONY_DE.ANALYTICS.SEMANTIC_STAGE/sony_entertainment_analytics_semantic_model.yaml';

-- Note: As of Feb 2025, semantic views are in preview
-- The stage-based YAML approach is more common
*/

-- =============================================================================
-- 10.5 VERIFIED QUERIES (VQRs)
-- =============================================================================
/*
The semantic model includes verified queries to improve accuracy:

verified_queries:
  - name: total_box_office_by_franchise
    question: "What is the total box office revenue by franchise?"
    sql: |
      SELECT franchise, SUM(total_gross_usd) AS total_revenue
      FROM SONY_DE.PLATINUM.AGG_FRANCHISE_PERFORMANCE
      GROUP BY franchise
      ORDER BY total_revenue DESC

  - name: fan_count_by_region
    question: "How many fans are in each region?"
    sql: |
      SELECT region, SUM(fan_count) AS total_fans
      FROM SONY_DE.PLATINUM.AGG_FAN_LIFETIME_VALUE
      GROUP BY region
      ORDER BY total_fans DESC

  - name: top_rated_movies
    question: "What are the top 5 highest rated movies?"
    sql: |
      SELECT title_name, imdb_score, franchise, release_year
      FROM SONY_DE.GOLD.DT_DIM_TITLES
      WHERE imdb_score IS NOT NULL
      ORDER BY imdb_score DESC
      LIMIT 5
*/

-- =============================================================================
-- 10.6 QUERY CORTEX ANALYST
-- =============================================================================

-- Example: Query using Cortex Analyst API
/*
Python SDK example:
```python
from snowflake.core import Root

root = Root(session)
analyst = root.databases["SONY_DE"].schemas["ANALYTICS"].cortex_analyst_services["SONY_ANALYST"]

response = analyst.send_message(
    semantic_model="@SONY_DE.ANALYTICS.SEMANTIC_STAGE/sony_entertainment_analytics_semantic_model.yaml",
    messages=[{
        "role": "user",
        "content": "What is the total box office revenue by franchise?"
    }]
)

print(response.content[0].text)  # Generated SQL
```

Streamlit example:
```python
from snowflake.cortex import analyst

response = analyst.message(
    session=session,
    semantic_model_file="@SONY_DE.ANALYTICS.SEMANTIC_STAGE/sony_entertainment_analytics_semantic_model.yaml",
    messages=[{"role": "user", "content": "Show me top grossing franchises"}]
)
```
*/

-- =============================================================================
-- 10.7 VERIFICATION
-- =============================================================================

-- Check stage contents
LIST @SONY_DE.ANALYTICS.SEMANTIC_STAGE;

-- Verify tables referenced in semantic model exist and have data
SELECT 'AGG_FRANCHISE_PERFORMANCE' AS table_name, COUNT(*) AS rows FROM SONY_DE.PLATINUM.AGG_FRANCHISE_PERFORMANCE
UNION ALL SELECT 'AGG_FAN_LIFETIME_VALUE', COUNT(*) FROM SONY_DE.PLATINUM.AGG_FAN_LIFETIME_VALUE
UNION ALL SELECT 'DT_DIM_TITLES', COUNT(*) FROM SONY_DE.GOLD.DT_DIM_TITLES
UNION ALL SELECT 'DT_DIM_FANS', COUNT(*) FROM SONY_DE.GOLD.DT_DIM_FANS
UNION ALL SELECT 'DT_FACT_DAILY_PERFORMANCE', COUNT(*) FROM SONY_DE.GOLD.DT_FACT_DAILY_PERFORMANCE;

-- Expected:
-- SEMANTIC_STAGE contains sony_entertainment_analytics_semantic_model.yaml (~15KB)
-- All referenced tables have data
-- Cortex Analyst can be queried via API or Streamlit

-- Sample questions to test:
-- 1. "What is the total box office revenue for Spider-Man franchise?"
-- 2. "How many verified fans are in North America?"
-- 3. "Which movie has the highest IMDb rating?"
-- 4. "Show me fan engagement by region"
-- 5. "What was the average ticket price last month?"
