# Sony Pictures Entertainment - Data Engineering Demo

A comprehensive Snowflake data engineering demo showcasing the **Medallion Architecture** with a **Hybrid Pipeline Pattern** (Imperative + Declarative).

## Architecture Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   BRONZE    │    │   SILVER    │    │    GOLD     │    │  PLATINUM   │
│   (RAW)     │───▶│   (STG)     │───▶│ (DIMS/FACTS)│───▶│   (AGGS)    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
     │                   │                   │                   │
  JSON/CSV          Stream/Task         Dynamic Tables      Analytics
  Ingestion          Routing            Auto-Refresh        Real-time
```

## Quick Start

### Prerequisites
- Snowflake account with SYSADMIN/SECURITYADMIN access
- `snow` CLI (optional, for uploads)
- Python 3.11+ with dbt (optional, for testing)

### Deploy in Order

```sql
-- Run each script in sequence:
demo_scripts/step_01_environment_setup.sql
demo_scripts/step_02_rbac_setup.sql
demo_scripts/step_03_bronze_layer.sql
demo_scripts/step_04_streams_tasks.sql
demo_scripts/step_06_dynamic_tables.sql
demo_scripts/step_07_data_quality.sql
demo_scripts/step_10_semantic_view.sql
demo_scripts/step_11_cortex_agent.sql
demo_scripts/step_12_demo_execution.sql  -- Verification
```

### Incremental Demo Data (for Live Demos)

To demonstrate pipeline in action with **data lineage tracking**:

```sql
-- Run in sequence, observing pipeline between each:
demo_scripts/data_batch_01_initial.sql   -- 10K fans, 15 titles, 30 days box office
demo_scripts/data_batch_02_incremental.sql -- +5K fans, +10 theaters
demo_scripts/data_batch_03_release.sql   -- Karate Kid release weekend!

-- Use this guide for step-by-step demo flow:
demo_scripts/demo_execution_incremental.sql
```

Each batch tracks `SOURCE_FILE` for lineage:
- `fan_interactions_batch_001.json` → `batch_002.json` → `batch_003.json`
- `box_office_batch_001.csv` → `batch_002.csv` → `batch_003.csv`
- `titles_batch_001.json` → `batch_002.json`

## Key Components

| Component | Location | Description |
|-----------|----------|-------------|
| **Database** | `SONY_DE` | Main data warehouse |
| **Schemas** | BRONZE, SILVER, GOLD, PLATINUM, GOVERNANCE, SECURE, ANALYTICS | Medallion layers + utilities |
| **Stream** | `BRONZE.STREAM_FAN_INTERACTIONS` | CDC capture for real-time routing |
| **Tasks** | `BRONZE.TASK_ROUTE_FAN_DATA`, `SILVER.TASK_DAG_*` | ETL orchestration |
| **Dynamic Tables** | 10 DTs across SILVER/GOLD/PLATINUM | Auto-refreshing transformations |
| **DMFs** | `GOVERNANCE.VALID_*`, `NULL_COUNT`, etc. | Data quality metrics |
| **Semantic Model** | `@ANALYTICS.SEMANTIC_STAGE/*.yaml` | Cortex Analyst text-to-SQL |
| **Cortex Agent** | `ANALYTICS.SONY_ENTERTAINMENT_AGENT` | Conversational AI interface |

## Data Volumes

| Layer | Table | Rows |
|-------|-------|------|
| BRONZE | RAW_FAN_INTERACTIONS | ~110K |
| BRONZE | RAW_TITLE_METADATA | 45 |
| BRONZE | RAW_BOX_OFFICE | ~102K |
| SILVER | STG_FAN_VERIFIED | ~53K |
| SILVER | STG_FAN_GUEST | ~57K |
| GOLD | DT_DIM_FANS | ~28K |
| GOLD | DT_DIM_TITLES | 45 |
| PLATINUM | AGG_FRANCHISE_PERFORMANCE | 12 |

## Key Patterns Demonstrated

### 1. INSERT ALL Fan-Out Pattern
```sql
INSERT ALL
    WHEN account_type = 'VERIFIED' THEN INTO STG_FAN_VERIFIED...
    ELSE INTO STG_FAN_GUEST...
SELECT * FROM STREAM_FAN_INTERACTIONS;
```

### 2. QUALIFY Deduplication
```sql
SELECT * FROM STG_BOX_OFFICE_CLEAN
QUALIFY ROW_NUMBER() OVER (PARTITION BY title_id, report_date, theater_id ORDER BY ingestion_ts DESC) = 1;
```

### 3. DOWNSTREAM Cascading Refresh
```sql
CREATE DYNAMIC TABLE DT_DIM_FANS LAG = 'DOWNSTREAM' AS ...
-- Refreshes automatically when upstream DTs change
```

### 4. Data Metric Functions
```sql
CREATE DATA METRIC FUNCTION VALID_IMDB_SCORE(ARG_T TABLE(imdb_score FLOAT))
RETURNS NUMBER AS $$ SELECT COUNT_IF(imdb_score < 0 OR imdb_score > 10) FROM ARG_T $$;
```

## Demo Flow (30 min)

1. **Intro** (5 min) - Architecture overview, hybrid pipeline concept
2. **Data Flow** (10 min) - Insert data, watch Stream/Task/DT cascade
3. **Data Quality** (5 min) - DMF violations, dbt tests
4. **Analytics** (5 min) - Query Platinum, Cortex Analyst demo
5. **Q&A** (5 min) - RBAC, scaling, next steps

## Project Structure

```
data_eng_demo/
├── demo_scripts/               # SQL scripts for each step
│   ├── step_01_environment_setup.sql
│   ├── step_02_rbac_setup.sql
│   ├── step_03_bronze_layer.sql
│   ├── step_04_streams_tasks.sql
│   ├── step_06_dynamic_tables.sql
│   ├── step_07_data_quality.sql
│   ├── step_08_dbt_project.sql
│   ├── step_09_streamlit_dashboard.sql
│   ├── step_10_semantic_view.sql
│   ├── step_11_cortex_agent.sql
│   ├── step_12_demo_execution.sql
│   ├── data_batch_01_initial.sql       # Initial data load
│   ├── data_batch_02_incremental.sql   # Day 2 incremental
│   ├── data_batch_03_release.sql       # Release weekend surge
│   └── demo_execution_incremental.sql  # Live demo guide
├── semantic_view_creation/     # Cortex Analyst semantic model
│   └── sony_entertainment_analytics_semantic_model.yaml
├── sony_dbt/                   # dbt project for testing
│   └── sony_spe/
├── streamlit_app/              # Monitoring dashboard
│   └── app_sis.py
└── SONY_DE_ANALYTICS_sony_entertainment_agent/  # Cortex Agent spec
    └── versions/v20260218-1903/agent_spec.json
```

## Verification Commands

```sql
-- Check all components
SELECT * FROM SONY_DE.GOVERNANCE.V_LAYER_ROW_COUNTS;
SELECT * FROM SONY_DE.GOVERNANCE.V_DATA_QUALITY_DASHBOARD;
SHOW DYNAMIC TABLES IN DATABASE SONY_DE;
SHOW TASKS IN DATABASE SONY_DE;
```

## Contact

Demo created for Sony Pictures Entertainment Data Engineering team.
