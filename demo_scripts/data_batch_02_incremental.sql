-- =============================================================================
-- DATA BATCH 2: INCREMENTAL LOAD (Day 2)
-- Sony Pictures Entertainment Data Engineering Demo
-- =============================================================================
-- Run AFTER batch_01 to show incremental processing
-- Watch: Stream captures changes → Task routes → DTs refresh
-- =============================================================================

USE ROLE SYSADMIN;
USE DATABASE SONY_DE;
USE SCHEMA BRONZE;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 2.1 CHECK STREAM STATUS BEFORE INSERT
-- =============================================================================

SELECT 'BEFORE INSERT' AS timing, 
       SYSTEM$STREAM_HAS_DATA('SONY_DE.BRONZE.STREAM_FAN_INTERACTIONS') AS stream_has_data;

-- =============================================================================
-- 2.2 NEW FAN INTERACTIONS - BATCH 2 (5,000 new fans + returning fans)
-- =============================================================================

INSERT INTO RAW_FAN_INTERACTIONS (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
WITH 
-- 3,000 brand new fans (IDs 10000-12999)
new_fans AS (
    SELECT 
        10000 + SEQ4() AS fan_num,
        'FAN-' || LPAD((10000 + SEQ4())::VARCHAR, 8, '0') AS fan_id,
        CASE MOD(SEQ4(), 3) WHEN 0 THEN 'VERIFIED' ELSE 'GUEST' END AS account_type,
        ARRAY_CONSTRUCT('Michael','Emily','Daniel','Jessica','David','Ashley','Chris','Sarah','Ryan','Nicole')[MOD(SEQ4(), 10)] AS first_name,
        ARRAY_CONSTRUCT('Anderson','Thomas','Jackson','White','Harris','Martin','Thompson','Moore','Young','Allen')[MOD(SEQ4(), 10)] AS last_name,
        ARRAY_CONSTRUCT('North America','Europe','Asia Pacific','Latin America')[MOD(SEQ4(), 4)] AS region,
        ARRAY_CONSTRUCT('USA','GBR','JPN','BRA','DEU','FRA','AUS','MEX','IND','CAN')[MOD(SEQ4(), 10)] AS country_code,
        ARRAY_CONSTRUCT('iOS','Android','Web','Smart TV','Console')[MOD(SEQ4(), 5)] AS device_type
    FROM TABLE(GENERATOR(ROWCOUNT => 3000))
),
-- 2,000 returning fans (random from existing 0-9999)
returning_fans AS (
    SELECT 
        FLOOR(RANDOM() * 10000) AS fan_num,
        'FAN-' || LPAD(FLOOR(RANDOM() * 10000)::VARCHAR, 8, '0') AS fan_id,
        'VERIFIED' AS account_type,  -- Returning fans are verified
        ARRAY_CONSTRUCT('James','Emma','Liam','Olivia','Noah','Ava','Ethan','Sophia','Mason','Isabella')[MOD(SEQ4(), 10)] AS first_name,
        ARRAY_CONSTRUCT('Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis','Rodriguez','Martinez')[MOD(SEQ4(), 10)] AS last_name,
        ARRAY_CONSTRUCT('North America','Europe','Asia Pacific','Latin America')[MOD(SEQ4(), 4)] AS region,
        ARRAY_CONSTRUCT('USA','GBR','JPN','BRA','DEU','FRA','AUS','MEX','IND','CAN')[MOD(SEQ4(), 10)] AS country_code,
        ARRAY_CONSTRUCT('iOS','Android','Web','Smart TV','Console')[MOD(SEQ4(), 5)] AS device_type
    FROM TABLE(GENERATOR(ROWCOUNT => 2000))
),
all_fans AS (
    SELECT * FROM new_fans UNION ALL SELECT * FROM returning_fans
),
interactions AS (
    SELECT 
        f.*,
        UUID_STRING() AS interaction_id,
        UUID_STRING() AS session_id,
        'SPE' || LPAD((MOD(s.SEQ4(), 14) + 1)::VARCHAR, 3, '0') AS title_id,
        ARRAY_CONSTRUCT('STREAM','PURCHASE','REVIEW','WISHLIST','TRAILER_VIEW')[MOD(s.SEQ4(), 5)] AS event_type,
        DATEADD('minute', -MOD(s.SEQ4() * 17, 1440), CURRENT_TIMESTAMP()) AS event_timestamp,  -- Last 24 hours
        CONCAT(MOD(s.SEQ4(), 256)::VARCHAR, '.', MOD(s.SEQ4() * 7, 256)::VARCHAR, '.', MOD(s.SEQ4() * 13, 256)::VARCHAR, '.', MOD(s.SEQ4() * 19, 256)::VARCHAR) AS ip_address
    FROM all_fans f
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => 2)) s  -- 2 interactions per fan
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
    'fan_interactions_batch_002.json'
FROM interactions;

-- =============================================================================
-- 2.3 NEW BOX OFFICE DATA - BATCH 2 (Today's data + 10 new theaters)
-- =============================================================================

INSERT INTO RAW_BOX_OFFICE (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
WITH
-- Existing theaters (50) with today's data
existing_theaters AS (
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
-- 10 NEW theaters (expansion)
new_theaters AS (
    SELECT 
        50 + SEQ4() AS theater_num,
        'THR-' || LPAD((50 + SEQ4())::VARCHAR, 6, '0') AS theater_id,
        ARRAY_CONSTRUCT('CGV Gangnam','IMAX Sydney','PVR Phoenix','Cineplex Toronto','Event Cinemas')[MOD(SEQ4(), 5)] AS theater_name,
        ARRAY_CONSTRUCT('Asia Pacific','Asia Pacific','Asia Pacific','North America','Asia Pacific')[MOD(SEQ4(), 5)] AS region,
        ARRAY_CONSTRUCT('KOR','AUS','IND','CAN','NZL')[MOD(SEQ4(), 5)] AS country,
        ARRAY_CONSTRUCT('KRW','AUD','INR','CAD','NZD')[MOD(SEQ4(), 5)] AS currency,
        ARRAY_CONSTRUCT(0.00075,0.65,0.012,0.74,0.60)[MOD(SEQ4(), 5)] AS exchange_rate
    FROM TABLE(GENERATOR(ROWCOUNT => 10))
),
all_theaters AS (
    SELECT * FROM existing_theaters UNION ALL SELECT * FROM new_theaters
),
daily_reports AS (
    SELECT 
        t.*,
        CURRENT_DATE() AS report_date,  -- Today only
        'SPE' || LPAD((MOD(t.theater_num, 10) + 1)::VARCHAR, 3, '0') AS title_id,
        GREATEST(200, FLOOR(RANDOM() * 2000)) AS tickets_sold,  -- Higher weekend numbers
        GREATEST(8, FLOOR(RANDOM() * 18)) AS screen_count,
        GREATEST(15, FLOOR(RANDOM() * 30)) AS showtime_count
    FROM all_theaters t
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
        'gross_revenue_local', tickets_sold * (14 + MOD(theater_num, 6)),
        'gross_revenue_usd', ROUND(tickets_sold * (14 + MOD(theater_num, 6)) * exchange_rate, 2),
        'local_currency', currency,
        'exchange_rate_usd', exchange_rate,
        'screen_count', screen_count,
        'showtime_count', showtime_count
    ),
    'BOX_OFFICE_MOJO',
    'box_office_batch_002.csv'
FROM daily_reports;

-- =============================================================================
-- 2.4 CHECK STREAM STATUS AFTER INSERT
-- =============================================================================

SELECT 'AFTER INSERT' AS timing, 
       SYSTEM$STREAM_HAS_DATA('SONY_DE.BRONZE.STREAM_FAN_INTERACTIONS') AS stream_has_data;

-- =============================================================================
-- 2.5 MANUALLY TRIGGER TASK (for demo - normally runs on schedule)
-- =============================================================================

-- Uncomment to manually trigger:
-- EXECUTE TASK SONY_DE.BRONZE.TASK_ROUTE_FAN_DATA;

-- =============================================================================
-- 2.6 VERIFICATION
-- =============================================================================

-- Check batch counts
SELECT SOURCE_FILE, COUNT(*) AS row_count
FROM RAW_FAN_INTERACTIONS
GROUP BY SOURCE_FILE
ORDER BY SOURCE_FILE;

SELECT SOURCE_FILE, COUNT(*) AS row_count
FROM RAW_BOX_OFFICE
GROUP BY SOURCE_FILE
ORDER BY SOURCE_FILE;

-- Check stream consumed data (should show TRUE before task runs)
SELECT SYSTEM$STREAM_HAS_DATA('SONY_DE.BRONZE.STREAM_FAN_INTERACTIONS') AS pending_data;

-- Expected:
-- fan_interactions_batch_001.json: ~30,000
-- fan_interactions_batch_002.json: ~10,000 (5K fans × 2 interactions)
-- box_office_batch_001.csv: ~1,500
-- box_office_batch_002.csv: ~60 (60 theaters × 1 day)
