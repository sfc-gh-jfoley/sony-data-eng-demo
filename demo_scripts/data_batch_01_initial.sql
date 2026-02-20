-- =============================================================================
-- DATA BATCH 1: INITIAL LOAD
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- Run this FIRST to establish baseline data
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE SCHEMA BRONZE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 1.1 TITLE METADATA (15 Sony Titles - Full Catalog)
-- =============================================================================

TRUNCATE TABLE RAW_TITLE_METADATA;

INSERT INTO RAW_TITLE_METADATA (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
SELECT PARSE_JSON(value), 'IMDB_FEED', 'titles_batch_001.json'
FROM TABLE(FLATTEN(INPUT => PARSE_JSON('[
    {"title_id":"SPE001","title_name":"Spider-Man: No Way Home","franchise":"Spider-Man","genre":"Action","sub_genre":"Superhero","release_year":2021,"runtime_minutes":148,"imdb_score":8.3,"rating":"PG-13","budget_usd":200000000,"studio":"Sony Pictures Entertainment","director":"Jon Watts","production_status":"RELEASED"},
    {"title_id":"SPE002","title_name":"Spider-Man: Across the Spider-Verse","franchise":"Spider-Man","genre":"Animation","sub_genre":"Superhero","release_year":2023,"runtime_minutes":140,"imdb_score":8.6,"rating":"PG","budget_usd":150000000,"studio":"Sony Pictures Animation","director":"Joaquim Dos Santos","production_status":"RELEASED"},
    {"title_id":"SPE003","title_name":"Spider-Man: Into the Spider-Verse","franchise":"Spider-Man","genre":"Animation","sub_genre":"Superhero","release_year":2018,"runtime_minutes":117,"imdb_score":8.4,"rating":"PG","budget_usd":90000000,"studio":"Sony Pictures Animation","director":"Bob Persichetti","production_status":"RELEASED"},
    {"title_id":"SPE004","title_name":"Venom: Let There Be Carnage","franchise":"Spider-Man","genre":"Action","sub_genre":"Superhero","release_year":2021,"runtime_minutes":97,"imdb_score":5.9,"rating":"PG-13","budget_usd":110000000,"studio":"Sony Pictures Entertainment","director":"Andy Serkis","production_status":"RELEASED"},
    {"title_id":"SPE005","title_name":"Ghostbusters: Frozen Empire","franchise":"Ghostbusters","genre":"Comedy","sub_genre":"Supernatural","release_year":2024,"runtime_minutes":115,"imdb_score":6.2,"rating":"PG-13","budget_usd":100000000,"studio":"Sony Pictures Entertainment","director":"Gil Kenan","production_status":"RELEASED"},
    {"title_id":"SPE006","title_name":"Ghostbusters: Afterlife","franchise":"Ghostbusters","genre":"Comedy","sub_genre":"Supernatural","release_year":2021,"runtime_minutes":124,"imdb_score":7.0,"rating":"PG-13","budget_usd":75000000,"studio":"Sony Pictures Entertainment","director":"Jason Reitman","production_status":"RELEASED"},
    {"title_id":"SPE007","title_name":"Bad Boys: Ride or Die","franchise":"Bad Boys","genre":"Action","sub_genre":"Comedy","release_year":2024,"runtime_minutes":115,"imdb_score":6.5,"rating":"R","budget_usd":100000000,"studio":"Sony Pictures Entertainment","director":"Adil El Arbi","production_status":"RELEASED"},
    {"title_id":"SPE008","title_name":"Jumanji: The Next Level","franchise":"Jumanji","genre":"Adventure","sub_genre":"Fantasy","release_year":2019,"runtime_minutes":123,"imdb_score":6.7,"rating":"PG-13","budget_usd":125000000,"studio":"Sony Pictures Entertainment","director":"Jake Kasdan","production_status":"RELEASED"},
    {"title_id":"SPE009","title_name":"Uncharted","franchise":"Uncharted","genre":"Action","sub_genre":"Adventure","release_year":2022,"runtime_minutes":116,"imdb_score":6.3,"rating":"PG-13","budget_usd":120000000,"studio":"Sony Pictures Entertainment","director":"Ruben Fleischer","production_status":"RELEASED"},
    {"title_id":"SPE010","title_name":"Bullet Train","franchise":"Standalone","genre":"Action","sub_genre":"Thriller","release_year":2022,"runtime_minutes":127,"imdb_score":7.3,"rating":"R","budget_usd":90000000,"studio":"Sony Pictures Entertainment","director":"David Leitch","production_status":"RELEASED"},
    {"title_id":"SPE011","title_name":"Morbius","franchise":"Spider-Man","genre":"Action","sub_genre":"Horror","release_year":2022,"runtime_minutes":104,"imdb_score":5.2,"rating":"PG-13","budget_usd":75000000,"studio":"Sony Pictures Entertainment","director":"Daniel Espinosa","production_status":"RELEASED"},
    {"title_id":"SPE012","title_name":"Madame Web","franchise":"Spider-Man","genre":"Action","sub_genre":"Thriller","release_year":2024,"runtime_minutes":116,"imdb_score":4.0,"rating":"PG-13","budget_usd":80000000,"studio":"Sony Pictures Entertainment","director":"S.J. Clarkson","production_status":"RELEASED"},
    {"title_id":"SPE013","title_name":"Kraven the Hunter","franchise":"Spider-Man","genre":"Action","sub_genre":"Thriller","release_year":2024,"runtime_minutes":127,"imdb_score":5.5,"rating":"R","budget_usd":110000000,"studio":"Sony Pictures Entertainment","director":"J.C. Chandor","production_status":"RELEASED"},
    {"title_id":"SPE014","title_name":"Gran Turismo","franchise":"Standalone","genre":"Action","sub_genre":"Sports","release_year":2023,"runtime_minutes":134,"imdb_score":7.1,"rating":"PG-13","budget_usd":60000000,"studio":"Sony Pictures Entertainment","director":"Neill Blomkamp","production_status":"RELEASED"},
    {"title_id":"SPE015","title_name":"Karate Kid: Legends","franchise":"Karate Kid","genre":"Action","sub_genre":"Drama","release_year":2025,"runtime_minutes":120,"imdb_score":null,"rating":"PG-13","budget_usd":65000000,"studio":"Sony Pictures Entertainment","director":"Jonathan Entwistle","production_status":"POST_PRODUCTION"}
]')));

-- =============================================================================
-- 1.2 FAN INTERACTIONS - BATCH 1 (10,000 fans, Week 1 data)
-- =============================================================================

-- Clear existing data for clean demo
TRUNCATE TABLE RAW_FAN_INTERACTIONS;

INSERT INTO RAW_FAN_INTERACTIONS (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
WITH 
fans AS (
    SELECT 
        SEQ4() AS fan_num,
        'FAN-' || LPAD(SEQ4()::VARCHAR, 8, '0') AS fan_id,
        CASE MOD(SEQ4(), 3) WHEN 0 THEN 'VERIFIED' ELSE 'GUEST' END AS account_type,
        ARRAY_CONSTRUCT('James','Emma','Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason','Isabella')[MOD(SEQ4(), 10)] AS first_name,
        ARRAY_CONSTRUCT('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez')[MOD(SEQ4(), 10)] AS last_name,
        ARRAY_CONSTRUCT('North America','Europe','Asia Pacific','Latin America')[MOD(SEQ4(), 4)] AS region,
        ARRAY_CONSTRUCT('USA','GBR','JPN','BRA','DEU','FRA','AUS','MEX','IND','CAN')[MOD(SEQ4(), 10)] AS country_code,
        ARRAY_CONSTRUCT('iOS','Android','Web','Smart TV','Console')[MOD(SEQ4(), 5)] AS device_type
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
),
interactions AS (
    SELECT 
        f.*,
        UUID_STRING() AS interaction_id,
        UUID_STRING() AS session_id,
        'SPE' || LPAD((MOD(s.SEQ4(), 14) + 1)::VARCHAR, 3, '0') AS title_id,
        ARRAY_CONSTRUCT('STREAM','PURCHASE','REVIEW','WISHLIST','TRAILER_VIEW')[MOD(s.SEQ4(), 5)] AS event_type,
        DATEADD('hour', -MOD(s.SEQ4() * 3, 168), CURRENT_TIMESTAMP()) AS event_timestamp,  -- Last 7 days
        CONCAT(MOD(s.SEQ4(), 256)::VARCHAR, '.', MOD(s.SEQ4() * 7, 256)::VARCHAR, '.', MOD(s.SEQ4() * 13, 256)::VARCHAR, '.', MOD(s.SEQ4() * 19, 256)::VARCHAR) AS ip_address
    FROM fans f
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 3)) s  -- 3 interactions per fan
)
SELECT 
    OBJECT_CONSTRUCT(
        'interaction_id', interaction_id,
        'fan_id', fan_id,
        'account_type', account_type,
        'first_name', first_name,
        'last_name', last_name,
        'email', LOWER(first_name) || fan_num || '@' || ARRAY_CONSTRUCT('gmail.com','yahoo.com','outlook.com','icloud.com')[MOD(fan_num, 4)],
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
    'fan_interactions_batch_001.json'
FROM interactions;

-- =============================================================================
-- 1.3 BOX OFFICE - BATCH 1 (50 theaters, 30 days history)
-- =============================================================================

TRUNCATE TABLE RAW_BOX_OFFICE;

INSERT INTO RAW_BOX_OFFICE (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
WITH
theaters AS (
    SELECT 
        SEQ4() AS theater_num,
        'THR-' || LPAD(SEQ4()::VARCHAR, 6, '0') AS theater_id,
        ARRAY_CONSTRUCT('AMC Downtown','Regal Cinemas','Cinemark Plaza','Odeon Leicester','Vue West End')[MOD(SEQ4(), 5)] AS theater_name,
        ARRAY_CONSTRUCT('North America','Europe','Asia Pacific','Latin America')[MOD(SEQ4(), 4)] AS region,
        ARRAY_CONSTRUCT('USA','GBR','JPN','BRA','DEU')[MOD(SEQ4(), 5)] AS country,
        ARRAY_CONSTRUCT('USD','GBP','JPY','BRL','EUR')[MOD(SEQ4(), 5)] AS currency,
        ARRAY_CONSTRUCT(1.0,1.27,0.0067,0.20,1.08)[MOD(SEQ4(), 5)] AS exchange_rate
    FROM TABLE(GENERATOR(ROWCOUNT => 50))
),
daily_reports AS (
    SELECT 
        t.*,
        DATEADD('day', -s.SEQ4(), CURRENT_DATE()) AS report_date,
        'SPE' || LPAD((MOD(s.SEQ4() + t.theater_num, 10) + 1)::VARCHAR, 3, '0') AS title_id,  -- Focus on top 10 titles
        GREATEST(100, FLOOR(RANDOM() * 1500)) AS tickets_sold,
        GREATEST(5, FLOOR(RANDOM() * 15)) AS screen_count,
        GREATEST(10, FLOOR(RANDOM() * 25)) AS showtime_count
    FROM theaters t
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 30)) s  -- 30 days
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
        'gross_revenue_local', tickets_sold * (12 + MOD(theater_num, 8)),
        'gross_revenue_usd', ROUND(tickets_sold * (12 + MOD(theater_num, 8)) * exchange_rate, 2),
        'local_currency', currency,
        'exchange_rate_usd', exchange_rate,
        'screen_count', screen_count,
        'showtime_count', showtime_count
    ),
    'BOX_OFFICE_MOJO',
    'box_office_batch_001.csv'
FROM daily_reports;

-- =============================================================================
-- 1.4 VERIFICATION
-- =============================================================================

SELECT 'BATCH 1 LOADED' AS status, 
       'RAW_FAN_INTERACTIONS' AS table_name, 
       COUNT(*) AS row_count,
       'fan_interactions_batch_001.json' AS source_file
FROM RAW_FAN_INTERACTIONS WHERE SOURCE_FILE = 'fan_interactions_batch_001.json'
UNION ALL
SELECT 'BATCH 1 LOADED', 'RAW_TITLE_METADATA', COUNT(*), 'titles_batch_001.json'
FROM RAW_TITLE_METADATA WHERE SOURCE_FILE = 'titles_batch_001.json'
UNION ALL
SELECT 'BATCH 1 LOADED', 'RAW_BOX_OFFICE', COUNT(*), 'box_office_batch_001.csv'
FROM RAW_BOX_OFFICE WHERE SOURCE_FILE = 'box_office_batch_001.csv';

-- Expected:
-- RAW_FAN_INTERACTIONS: ~30,000 rows (10K fans × 3 interactions)
-- RAW_TITLE_METADATA: 15 rows
-- RAW_BOX_OFFICE: ~1,500 rows (50 theaters × 30 days)
