import json
import random
import uuid
from datetime import datetime, timedelta
import snowflake.connector
import os
import time

TITLE_IDS = [f"SPE{str(i).zfill(3)}" for i in range(1, 16)]
REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America"]
COUNTRIES = {"North America": ["USA", "CAN", "MEX"], "Europe": ["GBR", "DEU", "FRA", "ESP"], 
             "Asia Pacific": ["JPN", "AUS", "KOR", "IND"], "Latin America": ["BRA", "ARG", "COL"]}
DEVICES = ["iOS", "Android", "Web", "SmartTV", "PlayStation", "Xbox"]
EVENT_TYPES = ["STREAM", "BROWSE", "PURCHASE", "REVIEW", "SHARE", "WISHLIST"]
ACCOUNT_TYPES = ["VERIFIED_LINKED", "GUEST"]
FIRST_NAMES = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "Oliver", "Sophia", "Elijah", "Isabella",
               "Lucas", "Mia", "Mason", "Charlotte", "Ethan", "Amelia", "Aiden", "Harper", "Logan", "Evelyn"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Wilson"]
THEATERS = [("AMC Downtown", "THR-100"), ("Regal Mall Plaza", "THR-101"), ("Cinemark Central", "THR-102"),
            ("Vue Cinema", "THR-103"), ("Odeon Luxe", "THR-104"), ("TOHO Cinemas", "THR-105")]

def generate_fan_interaction(batch_num):
    region = random.choice(REGIONS)
    country = random.choice(COUNTRIES[region])
    account_type = random.choice(ACCOUNT_TYPES)
    fan_id = f"FAN-BATCH{batch_num}-{str(uuid.uuid4())[:8]}"
    
    return {
        "interaction_id": str(uuid.uuid4()),
        "fan_id": fan_id,
        "session_id": str(uuid.uuid4()),
        "account_type": account_type,
        "email": f"{fan_id.lower()}@example.com" if account_type == "VERIFIED_LINKED" else None,
        "first_name": random.choice(FIRST_NAMES) if account_type == "VERIFIED_LINKED" else None,
        "last_name": random.choice(LAST_NAMES) if account_type == "VERIFIED_LINKED" else None,
        "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        "region": region,
        "country_code": country,
        "event_type": random.choice(EVENT_TYPES),
        "event_timestamp": (datetime.now() - timedelta(hours=random.randint(0, 48))).strftime("%Y-%m-%d %H:%M:%S"),
        "title_id": random.choice(TITLE_IDS),
        "device_type": random.choice(DEVICES)
    }

def generate_box_office(batch_num):
    region = random.choice(REGIONS)
    country = random.choice(COUNTRIES[region])
    theater = random.choice(THEATERS)
    tickets = random.randint(50, 500)
    price = random.uniform(8, 18)
    
    currencies = {"USA": ("USD", 1.0), "CAN": ("CAD", 0.74), "MEX": ("MXN", 0.058), 
                  "GBR": ("GBP", 1.27), "DEU": ("EUR", 1.08), "FRA": ("EUR", 1.08), "ESP": ("EUR", 1.08),
                  "JPN": ("JPY", 0.0067), "AUS": ("AUD", 0.65), "KOR": ("KRW", 0.00075), "IND": ("INR", 0.012),
                  "BRA": ("BRL", 0.20), "ARG": ("ARS", 0.0012), "COL": ("COP", 0.00025)}
    
    currency, rate = currencies.get(country, ("USD", 1.0))
    gross_local = tickets * price
    
    return {
        "record_id": str(uuid.uuid4()),
        "title_id": random.choice(TITLE_IDS),
        "theater_id": f"{theater[1]}-B{batch_num}",
        "theater_name": theater[0],
        "theater_region": region,
        "theater_country": country,
        "report_date": (datetime.now() - timedelta(days=random.randint(0, 7))).strftime("%Y-%m-%d"),
        "tickets_sold": tickets,
        "gross_revenue_local": round(gross_local, 2),
        "local_currency": currency,
        "exchange_rate_usd": rate,
        "gross_revenue_usd": round(gross_local * rate, 2),
        "screen_count": random.randint(1, 10),
        "showtime_count": random.randint(3, 15)
    }

def load_batch(conn, batch_num, fan_count=100, box_office_count=50):
    cursor = conn.cursor()
    
    print(f"\n{'='*60}")
    print(f"BATCH {batch_num}: Loading {fan_count} fan interactions + {box_office_count} box office records")
    print(f"{'='*60}")
    
    fan_data = [generate_fan_interaction(batch_num) for _ in range(fan_count)]
    for record in fan_data:
        cursor.execute("""
            INSERT INTO SONY_DE.BRONZE.RAW_FAN_INTERACTIONS (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
            SELECT PARSE_JSON(%s), 'INCREMENTAL_LOAD', %s
        """, (json.dumps(record), f"batch_{batch_num}_fans.json"))
    
    box_data = [generate_box_office(batch_num) for _ in range(box_office_count)]
    for record in box_data:
        cursor.execute("""
            INSERT INTO SONY_DE.BRONZE.RAW_BOX_OFFICE (RAW_DATA, SOURCE_SYSTEM, SOURCE_FILE)
            SELECT PARSE_JSON(%s), 'INCREMENTAL_LOAD', %s
        """, (json.dumps(record), f"batch_{batch_num}_box_office.json"))
    
    print(f"  ✓ Inserted {fan_count} fan interactions")
    print(f"  ✓ Inserted {box_office_count} box office records")
    
    print(f"\n  Refreshing Dynamic Table DAG...")
    
    refresh_order = [
        "SONY_DE.SILVER.DT_STG_FANS_UNIFIED",
        "SONY_DE.SILVER.DT_STG_BOX_OFFICE_DEDUP",
        "SONY_DE.SILVER.DT_INT_FANS_ENRICHED",
        "SONY_DE.SILVER.DT_INT_DAILY_PERFORMANCE",
        "SONY_DE.GOLD.DT_DIM_FANS",
        "SONY_DE.GOLD.DT_DIM_TITLES",
        "SONY_DE.GOLD.DT_FACT_DAILY_PERFORMANCE",
        "SONY_DE.PLATINUM.AGG_FAN_LIFETIME_VALUE",
        "SONY_DE.PLATINUM.AGG_FRANCHISE_PERFORMANCE"
    ]
    
    for dt in refresh_order:
        result = cursor.execute(f"ALTER DYNAMIC TABLE {dt} REFRESH").fetchone()
        stats = result[1] if result else "refreshed"
        dt_name = dt.split('.')[-1]
        print(f"    ↳ {dt_name}: {stats}")
    
    cursor.execute("SELECT COUNT(*) FROM SONY_DE.GOLD.DT_DIM_FANS")
    fan_count_total = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM SONY_DE.GOLD.DT_FACT_DAILY_PERFORMANCE")
    fact_count = cursor.fetchone()[0]
    
    print(f"\n  📊 Current totals: {fan_count_total:,} fans | {fact_count:,} daily performance records")
    
    cursor.close()
    return fan_count_total, fact_count

def main():
    print("🎬 Sony DE Incremental Load - 10 Batches")
    print("=" * 60)
    
    conn = snowflake.connector.connect(
        connection_name=os.getenv("SNOWFLAKE_CONNECTION_NAME", "default")
    )
    
    for batch in range(1, 11):
        load_batch(conn, batch, fan_count=100, box_office_count=50)
        
        if batch < 10:
            print(f"\n  ⏳ Waiting 3 seconds before next batch...")
            time.sleep(3)
    
    print("\n" + "=" * 60)
    print("✅ All 10 batches loaded successfully!")
    print("=" * 60)
    
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM SONY_DE.GOVERNANCE.V_LAYER_ROW_COUNTS ORDER BY LAYER")
    print("\n📊 Final Row Counts:")
    for row in cursor.fetchall():
        print(f"  {row[0]:10} | {row[1]:40} | {row[2]:,}")
    
    conn.close()

if __name__ == "__main__":
    main()
