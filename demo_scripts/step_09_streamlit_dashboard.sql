-- =============================================================================
-- STEP 9: STREAMLIT DASHBOARD
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- 
-- This step sets up the Streamlit monitoring dashboard.
-- The app can run locally OR deploy to Snowflake Streamlit in Snowflake (SiS).
--
-- =============================================================================

-- =============================================================================
-- 9.1 STREAMLIT APP OVERVIEW
-- =============================================================================
/*
Dashboard Features:
├── Tab 1: Overview
│   ├── Layer metrics (Bronze/Silver/Gold/Platinum row counts)
│   └── Bar chart visualization of data volumes
│
├── Tab 2: Data Quality
│   ├── DMF check results (pass/fail)
│   └── Violation counts by metric
│
├── Tab 3: Pipeline Health
│   ├── Stream status (pending data check)
│   ├── Task execution history
│   └── Dynamic Table row counts
│
├── Tab 4: Data Lineage
│   ├── Visual DAG (plotly graph)
│   └── Component inventory
│
└── Tab 5: Test Results
    ├── dbt test configuration
    └── Custom test descriptions
*/

-- =============================================================================
-- 9.2 LOCAL DEVELOPMENT SETUP
-- =============================================================================

-- Install dependencies (run in streamlit_app directory)
-- pip install streamlit pandas plotly snowflake-connector-python

-- Run locally (from project root)
-- cd streamlit_app
-- streamlit run app.py

-- =============================================================================
-- 9.3 STREAMLIT IN SNOWFLAKE (SiS) DEPLOYMENT
-- =============================================================================

-- Create stage for Streamlit app
CREATE STAGE IF NOT EXISTS SONY_DE.ANALYTICS.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE);

-- Upload the app file (run from CLI)
-- PUT file://streamlit_app/app_sis.py @SONY_DE.ANALYTICS.STREAMLIT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Create Streamlit app in Snowflake
CREATE OR REPLACE STREAMLIT SONY_DE.ANALYTICS.SONY_PIPELINE_MONITOR
    ROOT_LOCATION = '@SONY_DE.ANALYTICS.STREAMLIT_STAGE'
    MAIN_FILE = '/app_sis.py'
    QUERY_WAREHOUSE = COMPUTE_WH
    COMMENT = 'Sony Pictures Data Engineering Pipeline Monitor';

-- Grant access to the app
GRANT USAGE ON STREAMLIT SONY_DE.ANALYTICS.SONY_PIPELINE_MONITOR TO ROLE SONY_DE_ANALYST;
GRANT USAGE ON STREAMLIT SONY_DE.ANALYTICS.SONY_PIPELINE_MONITOR TO ROLE SONY_DE_DATA_ENGINEER;

-- =============================================================================
-- 9.4 STREAMLIT APP CODE (app_sis.py)
-- =============================================================================
/*
Key imports for Snowflake Streamlit:
```python
import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()

def run_query(query):
    return session.sql(query).to_pandas()
```

Dashboard tabs:
1. Overview - Row counts from V_LAYER_ROW_COUNTS
2. Data Quality - Results from V_DATA_QUALITY_DASHBOARD
3. Pipeline Health - Stream/Task/DT status
4. Data Lineage - Visual DAG with plotly
5. Test Results - dbt test configuration

Key visualizations:
- plotly.express.bar() for volume charts
- plotly.graph_objects for lineage DAG
- st.metric() for KPIs
- st.dataframe() for tabular data
*/

-- =============================================================================
-- 9.5 VERIFICATION
-- =============================================================================

-- Check if Streamlit app exists
SHOW STREAMLITS IN SCHEMA SONY_DE.ANALYTICS;

-- Verify governance views (used by dashboard)
SELECT * FROM SONY_DE.GOVERNANCE.V_LAYER_ROW_COUNTS;
SELECT * FROM SONY_DE.GOVERNANCE.V_DATA_QUALITY_DASHBOARD;

-- For local testing:
-- 1. cd streamlit_app
-- 2. streamlit run app.py
-- 3. Dashboard opens at http://localhost:8501

-- For Snowflake SiS:
-- 1. Navigate to Snowsight → Streamlit
-- 2. Find SONY_PIPELINE_MONITOR
-- 3. Click to open dashboard
