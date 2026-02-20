-- =============================================================================
-- STEP 11: CORTEX AGENT
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- 
-- Cortex Agent provides conversational AI over data using tools like
-- Cortex Analyst (text-to-SQL). This creates an intelligent assistant
-- for Sony entertainment data analytics.
--
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE SCHEMA ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 11.1 AGENT SPECIFICATION (agent_spec.json)
-- =============================================================================
/*
Agent Configuration:
{
  "models": {
    "orchestration": "auto"
  },
  "orchestration": {
    "budget": {
      "seconds": 900,
      "tokens": 400000
    }
  },
  "instructions": {
    "orchestration": "You are the Sony Pictures Entertainment data analytics agent...",
    "response": "Provide clear, data-driven insights about Sony Pictures entertainment data..."
  },
  "tools": [
    {
      "tool_spec": {
        "type": "cortex_analyst_text_to_sql",
        "name": "sony_entertainment_analytics",
        "description": "Query Sony Pictures entertainment data..."
      }
    }
  ],
  "tool_resources": {
    "sony_entertainment_analytics": {
      "execution_environment": {
        "query_timeout": 299,
        "type": "warehouse",
        "warehouse": "COMPUTE_WH"
      },
      "semantic_model_file": "@SONY_DE.ANALYTICS.SEMANTIC_STAGE/sony_entertainment_analytics_semantic_model.yaml"
    }
  }
}
*/

-- =============================================================================
-- 11.2 CREATE AGENT STAGE
-- =============================================================================

CREATE STAGE IF NOT EXISTS AGENT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Cortex Agent specifications';

-- =============================================================================
-- 11.3 UPLOAD AGENT SPEC
-- =============================================================================

-- From CLI (run in project root):
-- PUT file://SONY_DE_ANALYTICS_sony_entertainment_agent/versions/v20260218-1903/agent_spec.json @SONY_DE.ANALYTICS.AGENT_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Verify upload
LIST @SONY_DE.ANALYTICS.AGENT_STAGE;

-- =============================================================================
-- 11.4 CREATE CORTEX AGENT
-- =============================================================================

CREATE OR REPLACE CORTEX AGENT SONY_ENTERTAINMENT_AGENT
    COMMENT = 'Sony Pictures Entertainment data analytics agent'
    WAREHOUSE = COMPUTE_WH
    AGENT_SPECIFICATION_FILE = '@SONY_DE.ANALYTICS.AGENT_STAGE/agent_spec.json';

-- Grant access
GRANT USAGE ON CORTEX AGENT SONY_ENTERTAINMENT_AGENT TO ROLE SONY_DE_ANALYST;
GRANT USAGE ON CORTEX AGENT SONY_ENTERTAINMENT_AGENT TO ROLE SONY_DE_DATA_ENGINEER;

-- =============================================================================
-- 11.5 QUERY THE AGENT
-- =============================================================================

-- Example: Direct query via SQL function
/*
SELECT SNOWFLAKE.CORTEX.AGENT(
    'SONY_DE.ANALYTICS.SONY_ENTERTAINMENT_AGENT',
    PARSE_JSON('{
        "query": "What is the total box office revenue by franchise?"
    }')
) AS response;
*/

-- Example: Python SDK
/*
```python
from snowflake.core import Root

root = Root(session)
agent = root.databases["SONY_DE"].schemas["ANALYTICS"].cortex_agents["SONY_ENTERTAINMENT_AGENT"]

response = agent.run(
    messages=[{
        "role": "user",
        "content": "What is the total box office revenue by franchise?"
    }]
)

print(response.messages[-1].content)
```
*/

-- Example: Streamlit integration
/*
```python
import streamlit as st
from snowflake.core import Root

st.title("Sony Entertainment Analytics Assistant")

query = st.text_input("Ask a question about Sony entertainment data:")

if query:
    root = Root(session)
    agent = root.databases["SONY_DE"].schemas["ANALYTICS"].cortex_agents["SONY_ENTERTAINMENT_AGENT"]
    
    response = agent.run(messages=[{"role": "user", "content": query}])
    st.write(response.messages[-1].content)
```
*/

-- =============================================================================
-- 11.6 SAMPLE QUERIES FOR THE AGENT
-- =============================================================================
/*
Test questions to validate the agent:

1. Franchise Analysis:
   - "What is the total box office revenue by franchise?"
   - "Which franchise has the most titles?"
   - "Compare Spider-Man vs Ghostbusters performance"

2. Fan Engagement:
   - "How many fans are in each region?"
   - "What's the breakdown of verified vs guest fans?"
   - "Which region has the highest average engagement?"

3. Title Catalog:
   - "What are the top 5 highest rated movies?"
   - "Show me all PG-13 rated movies"
   - "Which movies have budget over $100 million?"

4. Performance Metrics:
   - "What was the total ticket sales last month?"
   - "Show me daily revenue trends for Spider-Man"
   - "Which theaters have the highest revenue?"

5. Combined Analysis:
   - "What's the ROI for action movies?"
   - "How does fan engagement correlate with box office?"
   - "Which franchises have the most active fans?"
*/

-- =============================================================================
-- 11.7 VERIFICATION
-- =============================================================================

-- Check agent exists
SHOW CORTEX AGENTS IN SCHEMA SONY_DE.ANALYTICS;

-- Check agent stage
LIST @SONY_DE.ANALYTICS.AGENT_STAGE;

-- Test agent (uncomment to run)
/*
SELECT SNOWFLAKE.CORTEX.AGENT(
    'SONY_DE.ANALYTICS.SONY_ENTERTAINMENT_AGENT',
    PARSE_JSON('{"query": "How many franchises do we have?"}')
) AS response;
*/

-- Expected:
-- Agent SONY_ENTERTAINMENT_AGENT exists
-- Agent stage contains agent_spec.json
-- Agent responds to natural language queries
