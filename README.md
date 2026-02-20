# Sony Pictures Entertainment - Data Engineering Demo

A comprehensive Snowflake data engineering demo showcasing the **Medallion Architecture** with a **Hybrid Pipeline Pattern** combining imperative (Streams/Tasks) and declarative (Dynamic Tables) approaches.

## Quick Start

```bash
# Clone the repo
git clone https://github.com/sfc-gh-jfoley/sony-data-eng-demo.git
cd sony-data-eng-demo

# Run SQL scripts in order (see demo_scripts/ below)
```

## Architecture Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   BRONZE    │    │   SILVER    │    │    GOLD     │    │  PLATINUM   │
│   (RAW)     │───▶│   (STG)     │───▶│ (DIMS/FACTS)│───▶│   (AGGS)    │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
      │                  │                  │                  │
   JSON/CSV          Stream/Task       Dynamic Tables      Analytics
   Ingestion          Routing          Auto-Refresh        Real-time
```

## Directory Structure

```
sony-data-eng-demo/
│
├── demo_scripts/                    # 🎯 START HERE - SQL scripts to deploy the demo
│   │
│   │  # Setup Scripts (run in order)
│   ├── step_01_environment_setup.sql    # Database, schemas, warehouse
│   ├── step_02_rbac_setup.sql           # 15+ roles with hierarchy
│   ├── step_03_bronze_layer.sql         # RAW tables + sample data
│   ├── step_04_streams_tasks.sql        # Stream CDC + Task routing
│   ├── step_06_dynamic_tables.sql       # 10 Dynamic Tables (Silver→Platinum)
│   ├── step_07_data_quality.sql         # 6 Data Metric Functions
│   ├── step_08_dbt_project.sql          # dbt integration docs
│   ├── step_09_streamlit_dashboard.sql  # Streamlit deployment
│   ├── step_10_semantic_view.sql        # Cortex Analyst semantic model
│   ├── step_11_cortex_agent.sql         # Conversational AI agent
│   ├── step_12_demo_execution.sql       # Verification queries
│   │
│   │  # Incremental Data Batches (for live demos)
│   ├── data_batch_01_initial.sql        # Initial load: 10K fans, 15 titles
│   ├── data_batch_02_incremental.sql    # Day 2: +5K fans, +10 theaters
│   ├── data_batch_03_release.sql        # Day 3: Karate Kid release weekend!
│   └── demo_execution_incremental.sql   # Step-by-step demo guide
│
├── semantic_view_creation/          # Cortex Analyst configuration
│   └── sony_entertainment_analytics_semantic_model.yaml
│
├── sony_dbt/                        # dbt project for testing Dynamic Tables
│   └── sony_spe/
│       ├── models/
│       │   ├── staging/             # Ephemeral models over DTs
│       │   ├── intermediate/
│       │   └── marts/
│       ├── tests/                   # Custom data quality tests
│       ├── snapshots/               # SCD Type 2 tracking
│       └── dbt_project.yml
│
├── streamlit_app/                   # Pipeline monitoring dashboard
│   ├── app_sis.py                   # Streamlit in Snowflake version
│   └── environment.yml
│
├── SONY_DE_ANALYTICS_sony_entertainment_agent/  # Cortex Agent spec
│   └── versions/v20260218-1903/agent_spec.json
│
├── rules/                           # Coding rules (for AI assistants)
├── skills/                          # Custom skills (for AI assistants)
│
├── DEMO_README.md                   # Detailed technical documentation
└── README.md                        # This file
```

## Deployment Guide

### Prerequisites
- Snowflake account with SYSADMIN and SECURITYADMIN access
- Warehouse with sufficient credits for demo

### Step 1: Initial Setup
Run these scripts in Snowflake in order:

```sql
-- 1. Create database, schemas, warehouse
@demo_scripts/step_01_environment_setup.sql

-- 2. Set up RBAC (roles and grants)
@demo_scripts/step_02_rbac_setup.sql

-- 3. Create Bronze layer tables
@demo_scripts/step_03_bronze_layer.sql

-- 4. Create Stream and Tasks
@demo_scripts/step_04_streams_tasks.sql

-- 5. Create Dynamic Tables (Silver → Gold → Platinum)
@demo_scripts/step_06_dynamic_tables.sql

-- 6. Set up Data Quality metrics
@demo_scripts/step_07_data_quality.sql
```

### Step 2: Load Initial Data
```sql
@demo_scripts/data_batch_01_initial.sql
```

### Step 3: Run the Pipeline
```sql
-- Execute task to route fan data through stream
EXECUTE TASK SONY_DE.BRONZE.TASK_ROUTE_FAN_DATA;

-- Dynamic Tables will auto-refresh (1-5 min lag)
-- Or force refresh:
ALTER DYNAMIC TABLE SONY_DE.SILVER.DT_STG_FANS_UNIFIED REFRESH;
```

### Step 4: Verify
```sql
@demo_scripts/step_12_demo_execution.sql
```

## Live Demo Flow

For demonstrating incremental data processing:

| Step | Script | What Happens |
|------|--------|--------------|
| 1 | `data_batch_01_initial.sql` | Load 10K fans, 15 titles, 30 days box office |
| 2 | Execute Task | Stream routes data → Staging tables populate |
| 3 | Wait 1-2 min | Dynamic Tables cascade refresh |
| 4 | `data_batch_02_incremental.sql` | Add 5K more fans, 10 new theaters |
| 5 | Execute Task | Watch row counts grow |
| 6 | `data_batch_03_release.sql` | Karate Kid release + surge traffic |
| 7 | Check Platinum | Aggregates reflect new release performance |

Use `demo_execution_incremental.sql` for guided queries between each step.

## Key Components Created

| Component | Name | Purpose |
|-----------|------|---------|
| Database | `SONY_DE` | Main data warehouse |
| Schemas | BRONZE, SILVER, GOLD, PLATINUM, GOVERNANCE, SECURE, ANALYTICS | Medallion layers |
| Stream | `STREAM_FAN_INTERACTIONS` | CDC on fan interactions |
| Tasks | `TASK_ROUTE_FAN_DATA` + DAG | ETL orchestration |
| Dynamic Tables | 10 DTs | Auto-refreshing transformations |
| DMFs | 6 functions | Data quality monitoring |
| Semantic Model | Stage file | Cortex Analyst text-to-SQL |
| Cortex Agent | `SONY_ENTERTAINMENT_AGENT` | Conversational analytics |

## Key Patterns Demonstrated

### 1. INSERT ALL Fan-Out
```sql
INSERT ALL
    WHEN account_type = 'VERIFIED' THEN INTO STG_FAN_VERIFIED...
    ELSE INTO STG_FAN_GUEST...
SELECT * FROM STREAM_FAN_INTERACTIONS;
```

### 2. DOWNSTREAM Cascading Refresh
```sql
CREATE DYNAMIC TABLE GOLD.DT_DIM_FANS 
    LAG = 'DOWNSTREAM' AS ...
-- Auto-refreshes when upstream Silver DTs change
```

### 3. Data Metric Functions
```sql
CREATE DATA METRIC FUNCTION VALID_IMDB_SCORE(ARG_T TABLE(imdb_score FLOAT))
RETURNS NUMBER AS 
$$ SELECT COUNT_IF(imdb_score < 0 OR imdb_score > 10) FROM ARG_T $$;
```

### 4. Data Lineage Tracking
```sql
-- Each batch tracked via SOURCE_FILE column
SELECT SOURCE_FILE, COUNT(*) FROM RAW_FAN_INTERACTIONS GROUP BY 1;
-- fan_interactions_batch_001.json: 30,000
-- fan_interactions_batch_002.json: 10,000
-- fan_interactions_batch_003.json: 12,000
```

## Optional Components

### Streamlit Dashboard
```sql
@demo_scripts/step_09_streamlit_dashboard.sql
```

### Cortex Analyst (Text-to-SQL)
```sql
@demo_scripts/step_10_semantic_view.sql
```

### Cortex Agent (Conversational AI)
```sql
@demo_scripts/step_11_cortex_agent.sql
```

### dbt Testing
```bash
cd sony_dbt/sony_spe
dbt deps
dbt test
```

## Support

Demo created for Sony Pictures Entertainment Data Engineering team.

For detailed technical documentation, see [DEMO_README.md](DEMO_README.md).
