#!/usr/bin/env python3
"""
PREPARE PHASE - COMPREHENSIVE COVERAGE
Handles "Frankenstein" queries with mixed patterns
"""

import duckdb
from pathlib import Path
import time
import json
import argparse
from tqdm import tqdm
import os

def prepare_data_comprehensive(data_dir: Path, out_dir: Path):
    """Comprehensive prepare phase for diverse query patterns"""
    print("Starting COMPREHENSIVE PREPARE PHASE...")
    total_start = time.time()
    
    out_dir.mkdir(parents=True, exist_ok=True)
    
    con = duckdb.connect(':memory:')
    threads = os.cpu_count() or 8
    con.execute(f"SET threads TO {threads};")
    con.execute("SET memory_limit = '14GB';")
    
    csv_pattern = f"{data_dir}/events_part_*.csv"
    
    # STEP 1: Load data
    print("\n📊 STEP 1: Loading CSV data...")
    step_start = time.time()
    
    con.execute(f"""
        CREATE TABLE events AS
        WITH raw AS (
            SELECT * FROM read_csv(
                '{csv_pattern}',
                AUTO_DETECT = FALSE,
                HEADER = TRUE,
                union_by_name = TRUE,
                parallel = TRUE,
                COLUMNS = {{
                    'ts': 'VARCHAR',
                    'type': 'VARCHAR',
                    'auction_id': 'VARCHAR',
                    'advertiser_id': 'VARCHAR',
                    'publisher_id': 'VARCHAR',
                    'bid_price': 'VARCHAR',
                    'user_id': 'VARCHAR',
                    'total_price': 'VARCHAR',
                    'country': 'VARCHAR'
                }}
            )
        ),
        casted AS (
            SELECT
                to_timestamp(TRY_CAST(ts AS DOUBLE) / 1000.0) AS ts,
                type,
                TRY_CAST(advertiser_id AS INTEGER) AS advertiser_id,
                TRY_CAST(publisher_id AS INTEGER) AS publisher_id,
                NULLIF(bid_price, '')::DOUBLE AS bid_price,
                NULLIF(total_price, '')::DOUBLE AS total_price,
                country
            FROM raw
        )
        SELECT
            ts,
            DATE_TRUNC('week', ts AT TIME ZONE 'America/Los_Angeles') AS week,
            DATE_TRUNC('day', ts AT TIME ZONE 'America/Los_Angeles') AS day,
            DATE_TRUNC('hour', ts AT TIME ZONE 'America/Los_Angeles') AS hour,
            DATE_TRUNC('minute', ts AT TIME ZONE 'America/Los_Angeles') AS minute,
            type,
            advertiser_id,
            publisher_id,
            bid_price,
            total_price,
            country
        FROM casted;
    """)
    
    row_count = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    print(f"Loaded {row_count:,} rows in {time.time() - step_start:.2f}s")
    
    # STEP 2: Export type partitions
    print("\nSTEP 2: Creating type partitions...")
    step_start = time.time()
    
    for event_type in tqdm(['impression', 'click', 'purchase', 'serve'], 
                           desc="Exporting types"):
        type_dir = out_dir / f"type={event_type}"
        type_dir.mkdir(parents=True, exist_ok=True)
        
        con.execute(f"""
            COPY (
                SELECT * FROM events 
                WHERE type = '{event_type}'
                ORDER BY day, country, advertiser_id
            )
            TO '{type_dir}/data.parquet' 
            (FORMAT PARQUET, COMPRESSION SNAPPY, ROW_GROUP_SIZE 500000);
        """)

    # STEP 3: Comprehensive aggregations
    print("\nSTEP 3: Creating comprehensive aggregations...")
    step_start = time.time()
    
    aggregations = {
        # === TIME-BASED (all event types) ===
        'agg_daily_impressions': """
            SELECT day, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY day ORDER BY day
        """,
        
        'agg_daily_clicks': """
            SELECT day, COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY day ORDER BY day
        """,
        
        'agg_daily_purchases': """
            SELECT day, COUNT(*), 
                SUM(total_price), 
                AVG(total_price)
            FROM events WHERE type = 'purchase' AND total_price IS NOT NULL
            GROUP BY day ORDER BY day
        """,
        
        'agg_daily_serves': """
            SELECT day, COUNT(*)
            FROM events WHERE type = 'serve'
            GROUP BY day ORDER BY day
        """,
        
        'agg_weekly_impressions': """
            SELECT week, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY week ORDER BY week
        """,
        
        'agg_hourly_impressions': """
            SELECT hour, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY hour ORDER BY hour
        """,
        
        'agg_minute_impressions': """
            SELECT minute, SUM(bid_price)
            FROM events WHERE type = 'impression'
            GROUP BY minute ORDER BY minute
        """,
        
        # NEW: Day-level minute aggregations for filtered queries
        'agg_daily_minute_impressions': """
            SELECT day, minute, SUM(bid_price)
            FROM events WHERE type = 'impression'
            GROUP BY day, minute ORDER BY day, minute
        """,
        
        'agg_daily_minute_clicks': """
            SELECT day, minute, COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY day, minute ORDER BY day, minute
        """,
        
        'agg_daily_hourly_impressions': """
            SELECT day, hour, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY day, hour ORDER BY day, hour
        """,
        
        # === GEOGRAPHIC ===
        'agg_country_impressions': """
            SELECT country, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY country ORDER BY SUM(bid_price) DESC
        """,
        
        'agg_country_clicks': """
            SELECT country, COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY country ORDER BY COUNT(*) DESC
        """,
        
        'agg_country_purchases': """
            SELECT country, 
                AVG(total_price),
                SUM(total_price),
                COUNT(*)
            FROM events WHERE type = 'purchase' AND total_price IS NOT NULL
            GROUP BY country ORDER BY AVG(total_price) DESC
        """,
        
        'agg_country_daily_impressions': """
            SELECT country, day, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY country, day ORDER BY country, day
        """,
        
        # === PUBLISHER ===
        'agg_publisher_impressions': """
            SELECT publisher_id, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY publisher_id ORDER BY SUM(bid_price) DESC
        """,
        
        'agg_publisher_country': """
            SELECT publisher_id, country, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY publisher_id, country ORDER BY publisher_id, country
        """,
        
        'agg_publisher_daily': """
            SELECT publisher_id, day, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY publisher_id, day ORDER BY publisher_id, day
        """,
        
        'agg_publisher_country_daily': """
            SELECT publisher_id, country, day, SUM(bid_price)
            FROM events WHERE type = 'impression'
            GROUP BY publisher_id, country, day ORDER BY publisher_id, country, day
        """,
        
        'agg_publisher_type': """
            SELECT publisher_id, type, COUNT(*)
            FROM events
            GROUP BY publisher_id, type ORDER BY publisher_id, type
        """,
        
        # === ADVERTISER ===
        'agg_advertiser_impressions': """
            SELECT advertiser_id, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY advertiser_id ORDER BY SUM(bid_price) DESC
        """,
        
        'agg_advertiser_type': """
            SELECT advertiser_id, type, COUNT(*)
            FROM events
            GROUP BY advertiser_id, type ORDER BY COUNT(*) DESC
        """,
        
        'agg_advertiser_daily': """
            SELECT advertiser_id, day, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY advertiser_id, day ORDER BY advertiser_id, day
        """,
        
        'agg_advertiser_country': """
            SELECT advertiser_id, country, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY advertiser_id, country ORDER BY advertiser_id, country
        """,
        
        'agg_advertiser_country_daily': """
            SELECT advertiser_id, country, day, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY advertiser_id, country, day ORDER BY advertiser_id, country, day
        """,
        
        'agg_advertiser_purchases': """
            SELECT advertiser_id, COUNT(*), SUM(total_price)
            FROM events WHERE type = 'purchase' AND total_price IS NOT NULL
            GROUP BY advertiser_id ORDER BY SUM(total_price) DESC
        """,
        
        # === FUNNEL/TYPE ANALYSIS ===
        'agg_type_counts': """
            SELECT type, COUNT(*)
            FROM events
            GROUP BY type ORDER BY type
        """,
        
        'agg_daily_type': """
            SELECT day, type, COUNT(*)
            FROM events
            GROUP BY day, type ORDER BY day, type
        """,
        
        'agg_country_type': """
            SELECT country, type, COUNT(*)
            FROM events
            GROUP BY country, type ORDER BY COUNT(*) DESC
        """,
        
        'agg_country_type_daily': """
            SELECT country, type, day, COUNT(*)
            FROM events
            GROUP BY country, type, day ORDER BY country, type, day
        """,

        # === WEEKLY TIME SERIES (non-impressions) ===
        'agg_weekly_clicks': """
            SELECT week, COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY week ORDER BY week
        """,

        'agg_weekly_purchases': """
            SELECT week, SUM(total_price), AVG(total_price), COUNT(*)
            FROM events WHERE type = 'purchase' AND total_price IS NOT NULL
            GROUP BY week ORDER BY week
        """,

        'agg_weekly_serves': """
            SELECT week, COUNT(*)
            FROM events WHERE type = 'serve'
            GROUP BY week ORDER BY week
        """,

        # === HOURLY TIME SERIES (non-impressions) ===
        'agg_hourly_clicks': """
            SELECT hour, COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY hour ORDER BY hour
        """,

        'agg_hourly_purchases': """
            SELECT hour, SUM(total_price), COUNT(*)
            FROM events WHERE type = 'purchase' AND total_price IS NOT NULL
            GROUP BY hour ORDER BY hour
        """,

        'agg_hourly_serves': """
            SELECT hour, COUNT(*)
            FROM events WHERE type = 'serve'
            GROUP BY hour ORDER BY hour
        """,

        # === COUNTRY + WEEK ===
        'agg_country_weekly_impressions': """
            SELECT country, week, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY country, week ORDER BY country, week
        """,

        'agg_country_weekly_clicks': """
            SELECT country, week, COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY country, week ORDER BY country, week
        """,

        'agg_country_weekly_purchases': """
            SELECT country, week, SUM(total_price), COUNT(*)
            FROM events WHERE type = 'purchase' AND total_price IS NOT NULL
            GROUP BY country, week ORDER BY country, week
        """,

        # === COUNTRY + HOUR ===
        'agg_country_hourly_impressions': """
            SELECT country, hour, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY country, hour ORDER BY country, hour
        """,

        'agg_country_hourly_clicks': """
            SELECT country, hour, COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY country, hour ORDER BY country, hour
        """,

        # === PUBLISHER + WEEK/HOUR ===
        'agg_publisher_weekly': """
            SELECT publisher_id, week, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY publisher_id, week ORDER BY publisher_id, week
        """,

        'agg_publisher_hourly': """
            SELECT publisher_id, hour, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY publisher_id, hour ORDER BY publisher_id, hour
        """,

        # === ADVERTISER + WEEK/HOUR ===
        'agg_advertiser_weekly': """
            SELECT advertiser_id, week, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY advertiser_id, week ORDER BY advertiser_id, week
        """,

        'agg_advertiser_hourly': """
            SELECT advertiser_id, hour, SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY advertiser_id, hour ORDER BY advertiser_id, hour
        """,

        # === ADVERTISER + PUBLISHER RELATIONSHIP ===
        'agg_advertiser_publisher': """
            SELECT advertiser_id, publisher_id, 
                SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY advertiser_id, publisher_id 
            ORDER BY SUM(bid_price) DESC
        """,

        # === SMALL GLOBAL STATS (INSTANT LOOKUPS!) ===
        'agg_global_stats': """
            SELECT 
                COUNT(*),
                COUNT(DISTINCT advertiser_id),
                COUNT(DISTINCT publisher_id),
                COUNT(DISTINCT country),
                MIN(day),
                MAX(day)
            FROM events
        """,

        'agg_impressions_summary': """
            SELECT 
                COUNT(*),
                SUM(bid_price),
                AVG(bid_price),
                MIN(bid_price),
                MAX(bid_price)
            FROM events WHERE type = 'impression'
        """,

        'agg_purchases_summary': """
            SELECT 
                COUNT(*),
                SUM(total_price),
                AVG(total_price),
                MIN(total_price),
                MAX(total_price)
            FROM events WHERE type = 'purchase' AND total_price IS NOT NULL
        """,

        # === TIME BUCKETING (TINY TABLES!) ===
        'agg_monthly_impressions': """
            SELECT DATE_TRUNC('month', day), 
                SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY DATE_TRUNC('month', day) ORDER BY DATE_TRUNC('month', day)
        """,

        'agg_weekday_impressions': """
            SELECT DAYOFWEEK(day),
                SUM(bid_price), COUNT(*)
            FROM events WHERE type = 'impression'
            GROUP BY DAYOFWEEK(day) ORDER BY DAYOFWEEK(day)
        """,

        'agg_weekday_clicks': """
            SELECT DAYOFWEEK(day), COUNT(*)
            FROM events WHERE type = 'click'
            GROUP BY DAYOFWEEK(day) ORDER BY DAYOFWEEK(day)
        """,
    }
    for name, sql in tqdm(aggregations.items(), desc="Creating aggregations"):
        agg_start = time.time()
        con.execute(f"""
            COPY ({sql}) TO '{out_dir}/{name}.parquet' 
            (FORMAT PARQUET, COMPRESSION SNAPPY);
        """)
        result = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
        print(f"{name}: {result:,} rows in {time.time() - agg_start:.2f}s")
    
    print(f"Created {len(aggregations)} aggregations in {time.time() - step_start:.2f}s")
    
    # Save metadata
    metadata = {
        'partition_strategy': 'type_only_with_comprehensive_aggregations',
        'types': ['impression', 'click', 'purchase', 'serve'],
        'total_rows': row_count,
        'aggregations': list(aggregations.keys()),
        'aggregation_count': len(aggregations)
    }

    with open(out_dir / 'metadata.json', 'w') as f:
        json.dump(metadata, f, indent=2)
    
    con.close()
    
    total_time = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"PREPARE COMPLETE!")
    print(f"{'='*60}")
    print(f"Total time: {total_time:.2f}s ({total_time/60:.2f} minutes)")
    print(f"Processed: {row_count:,} rows")
    print(f"Created: {len(aggregations)} aggregation tables")
    print(f"Output: {out_dir}")
    
    total_size = sum(f.stat().st_size for f in out_dir.rglob('*.parquet'))
    print(f"Total storage: {total_size / (1024**3):.2f} GB")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    prepare_data_comprehensive(args.data_dir, args.out_dir)