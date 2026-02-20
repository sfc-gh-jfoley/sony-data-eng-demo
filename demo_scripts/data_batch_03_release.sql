-- =============================================================================
-- DATA BATCH 3: INCREMENTAL LOAD (Day 3) + TITLE UPDATES
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- Run AFTER batch_02 to show:
-- - Continued data growth
-- - Title metadata updates (new release!)
-- - Track changes through DTs
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE SCHEMA BRONZE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 3.1 TITLE METADATA UPDATE - Karate Kid Released!
-- =============================================================================

-- Add new titles and update existing
INSERT INTO RAW_TITLE_METADATA (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
SELECT PARSE_JSON(value), 'IMDB_FEED', 'titles_batch_002.json'
FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[
    {"title_id":"SPE015","title_name":"Karate Kid: Legends","franchise":"Karate Kid","genre":"Action","sub_genre":"Drama","release_year":2025,"runtime_minutes":118,"imdb_score":7.4,"rating":"PG-13","budget_usd":65000000,"studio":"Sony Pictures Entertainment","director":"Jonathan Entwistle","production_status":"RELEASED"},
    {"title_id":"SPE016","title_name":"Spider-Man: Beyond the Spider-Verse","franchise":"Spider-Man","genre":"Animation","sub_genre":"Superhero","release_year":2026,"runtime_minutes":null,"imdb_score":null,"rating":"PG","budget_usd":180000000,"studio":"Sony Pictures Animation","director":"Joaquim Dos Santos","production_status":"PRE_PRODUCTION"},
    {"title_id":"SPE017","title_name":"Venom 3: Last Dance","franchise":"Spider-Man","genre":"Action","sub_genre":"Superhero","release_year":2025,"runtime_minutes":120,"imdb_score":null,"rating":"PG-13","budget_usd":120000000,"studio":"Sony Pictures Entertainment","director":"Kelly Marcel","production_status":"POST_PRODUCTION"}
]')));

-- =============================================================================
-- 3.2 FAN INTERACTIONS - BATCH 3 (Karate Kid Release Weekend Surge!)
-- =============================================================================

-- Heavy activity for Karate Kid + general traffic
INSERT INTO RAW_FAN_INTERACTIONS (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
WITH 
-- 4,000 new fans (IDs 13000-16999) attracted by new release
new_fans AS (
    SELECT 
        13000 + SEQ4() AS fan_num,
        'FAN-' || LPAD((13000 + SEQ4())::VARCHAR, 8, '0') AS fan_id,
        CASE MOD(SEQ4(), 4) WHEN 0 THEN 'VERIFIED' ELSE 'GUEST' END AS account_type,
        ARRAY_CONSTRUCT('Kevin','Jennifer','Brian','Michelle','Steven','Amanda','Joshua','Stephanie','Andrew','Heather')[MOD(SEQ4(), 10)] AS first_name,
        ARRAY_CONSTRUCT('Lee','Kim','Park','Chen','Wang','Liu','Nguyen','Patel','Singh','Wong')[MOD(SEQ4(), 10)] AS last_name,
        ARRAY_CONSTRUCT('Asia Pacific','North America','Europe','Asia Pacific')[MOD(SEQ4(), 4)] AS region,
        ARRAY_CONSTRUCT('JPN','USA','KOR','CHN','TWN','HKG','SGP','MYS','PHL','THA')[MOD(SEQ4(), 10)] AS country_code,
        ARRAY_CONSTRUCT('iOS','Android','Web','Smart TV','Console')[MOD(SEQ4(), 5)] AS device_type
    FROM TABLE(GENERATOR(ROWCOUNT => 4000))
),
interactions AS (
    SELECT 
        f.*,
        UUID_STRING() AS interaction_id,
        UUID_STRING() AS session_id,
        -- 60% of interactions are for Karate Kid (SPE015)
        CASE WHEN MOD(s.SEQ4(), 10) < 6 THEN 'SPE015' 
             ELSE 'SPE' || LPAD((MOD(s.SEQ4(), 14) + 1)::VARCHAR, 3, '0') 
        END AS title_id,
        ARRAY_CONSTRUCT('STREAM','PURCHASE','REVIEW','TRAILER_VIEW','TRAILER_VIEW')[MOD(s.SEQ4(), 5)] AS event_type,
        DATEADD('minute', -MOD(s.SEQ4() * 13, 2880), CURRENT_TIMESTAMP()) AS event_timestamp,  -- Last 48 hours
        CONCAT(MOD(s.SEQ4(), 256)::VARCHAR, '.', MOD(s.SEQ4() * 7, 256)::VARCHAR, '.', MOD(s.SEQ4() * 13, 256)::VARCHAR, '.', MOD(s.SEQ4() * 19, 256)::VARCHAR) AS ip_address
    FROM new_fans f
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 3)) s  -- 3 interactions per fan
)
SELECT 
    OBJECT_CONSTRUCT(
        'interaction_id', interaction_id,
        'fan_id', fan_id,
        'account_type', account_type,
        'first_name', first_name,
        'last_name', last_name,
        'email', LOWER(first_name) || fan_num || '@' || ARRAY_CONSTRUCT('gmail.com','yahoo.com','naver.com','qq.com')[MOD(fan_num, 4)],
        'region', region,
        'country_code', country_code,
        'device_type', device_type,
        'title_id', title_id,
        'event_type', event_type,
        'event_timestamp', event_timestamp::VARCHAR,
        'session_id', session_id,
        'ip_address', ip_address
    ),
    'SONY_REWARDS_APP',
    'fan_interactions_batch_003.json'
FROM interactions;

-- =============================================================================
-- 3.3 BOX OFFICE - BATCH 3 (Karate Kid Opening Weekend!)
-- =============================================================================

INSERT INTO RAW_BOX_OFFICE (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
WITH
all_theaters AS (
    SELECT 
        SEQ4() AS theater_num,
        'THR-' || LPAD(SEQ4()::VARCHAR, 6, '0') AS theater_id,
        ARRAY_CONSTRUCT('AMC Downtown','Regal Cinemas','Cinemark Plaza','CGV Gangnam','IMAX Sydney')[MOD(SEQ4(), 5)] AS theater_name,
        ARRAY_CONSTRUCT('North America','Europe','Asia Pacific','Asia Pacific','Asia Pacific')[MOD(SEQ4(), 5)] AS region,
        ARRAY_CONSTRUCT('USA','GBR','JPN','KOR','AUS')[MOD(SEQ4(), 5)] AS country,
        ARRAY_CONSTRUCT('USD','GBP','JPY','KRW','AUD')[MOD(SEQ4(), 5)] AS currency,
        ARRAY_CONSTRUCT(1.0,1.27,0.0067,0.00075,0.65)[MOD(SEQ4(), 5)] AS exchange_rate
    FROM TABLE(GENERATOR(ROWCOUNT => 60))  -- All 60 theaters
),
-- Opening weekend reports - KARATE KID ONLY (big release!)
weekend_reports AS (
    SELECT 
        t.*,
        d.report_date,
        'SPE015' AS title_id,  -- Karate Kid only for opening
        GREATEST(800, FLOOR(RANDOM() * 3500)) AS tickets_sold,  -- HUGE numbers!
        GREATEST(10, FLOOR(RANDOM() * 25)) AS screen_count,
        GREATEST(20, FLOOR(RANDOM() * 40)) AS showtime_count
    FROM all_theaters t
    CROSS JOIN (
        SELECT DATEADD('day', -SEQ4(), CURRENT_DATE()) AS report_date
        FROM TABLE(GENERATOR(ROWCOUNT => 3))  -- 3 days
    ) d
)
SELECT 
    OBJECT_CONSTRUCT(
        'record_id', UUID_STRING(),
        'title_id', title_id,
        'theater_id', theater_id,
        'theater_name', theater_name,
        'theater_region', region,
        'theater_country', country,
        'report_date', report_date::VARCHAR,
        'tickets_sold', tickets_sold,
        'gross_revenue_local', tickets_sold * (18 + MOD(theater_num, 5)),  -- Premium pricing
        'gross_revenue_usd', ROUND(tickets_sold * (18 + MOD(theater_num, 5)) * exchange_rate, 2),
        'local_currency', currency,
        'exchange_rate_usd', exchange_rate,
        'screen_count', screen_count,
        'showtime_count', showtime_count
    ),
    'BOX_OFFICE_MOJO',
    'box_office_batch_003.csv'
FROM weekend_reports;

-- =============================================================================
-- 3.4 VERIFICATION - Show Growth Over Time
-- =============================================================================

-- Data lineage by batch
SELECT 
    'FAN_INTERACTIONS' AS data_type,
    SOURCE_FILE,
    COUNT(*) AS records,
    MIN(INGESTED_AT) AS first_loaded,
    MAX(INGESTED_AT) AS last_loaded
FROM RAW_FAN_INTERACTIONS
GROUP BY SOURCE_FILE
UNION ALL
SELECT 
    'BOX_OFFICE' AS data_type,
    SOURCE_FILE,
    COUNT(*),
    MIN(INGESTED_AT),
    MAX(INGESTED_AT)
FROM RAW_BOX_OFFICE
GROUP BY SOURCE_FILE
UNION ALL
SELECT 
    'TITLE_METADATA' AS data_type,
    SOURCE_FILE,
    COUNT(*),
    MIN(INGESTED_AT),
    MAX(INGESTED_AT)
FROM RAW_TITLE_METADATA
GROUP BY SOURCE_FILE
ORDER BY data_type, SOURCE_FILE;

-- Total pipeline volumes
SELECT 
    'RAW_FAN_INTERACTIONS' AS table_name, COUNT(*) AS total_rows FROM RAW_FAN_INTERACTIONS UNION ALL
SELECT 'RAW_BOX_OFFICE', COUNT(*) FROM RAW_BOX_OFFICE UNION ALL
SELECT 'RAW_TITLE_METADATA', COUNT(*) FROM RAW_TITLE_METADATA;

-- Expected totals after Batch 3:
-- RAW_FAN_INTERACTIONS: ~52,000 (30K + 10K + 12K)
-- RAW_BOX_OFFICE: ~1,740 (1,500 + 60 + 180)
-- RAW_TITLE_METADATA: 18 rows (15 + 3)
