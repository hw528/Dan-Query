
# Technical Architecture Document

## System Overview

Columnar database achieving **740-2028x speedup** through three core strategies:

1.  **Pre-aggregation** - 51 tables covering 90% of query patterns
2.  **Sorted storage** - Enables fast I/O reduction via zone maps
3.  **Smart routing** - Hierarchical query planner picks optimal execution path

**Final stats:** 3.07 GB storage, <100ms queries, 9-minute one-time setup

----------

## 1. Storage Architecture

### Type Partitions (2.2 GB)

```
(sorted by day→country→advertiser_id)
type=impression/data.parquet  
type=click/data.parquet
type=purchase/data.parquet
type=serve/data.parquet
```

**Why partition by type?**

-   a lot of queries filter by event type
-   Reduces scan size by 75% (read 1 of 4 files instead of all, but impression has most data)

**Why sort before writing?**

_Problem:_ Unsorted Parquet has useless statistics

```
Row Group 1 (random order): day spans [Jan 1 to Dec 31]
Query WHERE day='June 1' → Must read ALL row groups (5 GB) 
```

_Solution:_ Sort by `(day, country, advertiser_id)`

```
Row Group 1:  day=[Jan 1 to Jan 1]
Row Group 20: day=[June 1 to June 1]  ← Only this matches!
Row Group 50: day=[Dec 31 to Dec 31]

Query WHERE day='June 1' → Read 1 row group 
```

**Additional benefits:**

-   **Compression:** Sorted data (e.g., 500k consecutive "JP") compresses 2-3x better
-   **Format:** Parquet columnar + SNAPPY compression (outperforms ZSTD) + 500k row groups

### Pre-Aggregations (51 tables, 0.87 GB)

**Time-based** (instant lookups for time-series queries):

-   Daily: 366 rows per event type
-   Weekly: 53 rows
-   Hourly: 8,760 rows
-   Minute: 525k rows (impressions/clicks only)

**Dimensional** (entity-level analysis):

-   Country: 12 countries
-   Publisher: 1,114 publishers
-   Advertiser: 1,654 advertisers

**Multi-dimensional** (complex queries):

-   `agg_publisher_country_daily`: 4.87M rows
-   `agg_advertiser_hourly`: 14.43M rows
-   `agg_advertiser_publisher`: 1.84M rows

**Why 51 tables?** Covers ~90% of queries. Trade-off: 6 min build time for 1000x query speedup.

### Query Cache (runtime)
-   In-memory MD5-hashed results
-   LRU eviction after 100 entries
----------

## 2. Query Execution

### Smart Router

**Algorithm:** Score each aggregation table, pick highest

```python
def  _score_aggregation(query) -> int
def  find_best_aggregation(query) -> (table_name, execution_plan)
```

**Example:**

```
Query: country + week + SUM(bid_price) WHERE type='impression'

Scoring:
  agg_country_weekly_impressions   → 150 (exact match)
  agg_country_daily_impressions    → 90  (can re-group daily→weekly)
  type=impression partition        → 10  (fallback)

Selected: agg_country_weekly_impressions 

```

### Execution Paths

**Path A: Direct hit** (80% of queries, 0~10ms)

1.  Read small aggregation (e.g., 366 rows)
2.  DuckDB filters during Parquet read (predicate pushdown)
3.  Return results

**Path B: Re-aggregation** (15% of queries, 10-50ms)

1.  Read detailed table (e.g., daily aggregation)
2.  Filter + re-group to broader level (e.g., sum daily→weekly)
3.  Return results

**Path C: Partition scan** (5% of queries, 50-500ms)

1.  Read sorted type partition
2.  Full DuckDB SQL execution
3.  Return results

----------

## 3. Critical Optimizations

### Native Datetime Types

```python
# BEFORE: String comparisons
WHERE day = '2024-06-01'  # 800ms (character-by-character matching)

# AFTER: Binary comparisons  
WHERE day = DATE '2024-06-01'  # 234ms (integer comparison)
```

All temporal columns stored as datetime, not strings.


### Adaptive Sorting

```python
if len(df) > 10000:
    df = duckdb.execute("SELECT * FROM df ORDER BY col").df()  # < 20ms
else:
    df = df.sort_values(col)  # Pandas faster for small data

```

DuckDB's external merge-sort handles 10k+ rows efficiently. Pandas struggles at 10k+.

### Skip Redundant Filters

```python
# Table agg_daily_impressions is already type='impression'
# Don't add WHERE type='impression' again (saves 10-15ms)
```

----------

## 4. Development Process

### Testing Strategy

1.  Built 51 aggregation tables
2.  Generated 70+ diverse test queries (not just 5 samples)
3.  Profiled each query: `planning, file_read, filtering, aggregation, sorting`
4.  Fixed bottlenecks iteratively

### Example: Query Optimization

```
Q31: 86.095 ms total
├─ Planning: 0.020 ms
├─ File Read:  48.058 ms    <- bottleneck
├─ Filtering:  0.032 ms
├─ Aggregation:0.000 ms
└─ Sorting:  34.354 ms      <- bottleneck

Root causes:
1. Date columns stored as strings, the string comparison is char-by-char too slow

Fixes:
1. Store all pre-calculated time as INT type instead of STR

Q31: 17.208 ms total
├─ Planning: 0.019 ms
├─ File Read:  3.499 ms     <- way better
├─ Filtering:  0.021 ms
├─ Aggregation:0.000 ms
└─ Sorting:  13.447 ms      <- way better
```
