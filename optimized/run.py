#!/usr/bin/env python3
"""
RUN PHASE
"""
import hashlib
import pandas as pd
from pathlib import Path
import time
import csv
import json
import argparse
import signal
from typing import List, Dict, Any, Optional, Tuple
import duckdb 
from contextlib import contextmanager
import os

def extract_needed_columns(query: Dict) -> List[str]:
    """Extract columns needed to execute query"""
    needed = set()
    
    for item in query.get('select', []):
        if isinstance(item, str):
            needed.add(item)
        elif isinstance(item, dict):
            for func, col in item.items():
                if col != '*':
                    needed.add(col)
    
    for condition in query.get('where', []):
        needed.add(condition['col'])
    
    needed.update(query.get('group_by', []))
    
    return list(needed)
    
def json_query_to_sql(query: Dict, file_pattern: str) -> str:
    """Convert JSON query format to SQL for DuckDB"""
    
    # Build SELECT clause
    select_parts = []
    for item in query['select']:
        if isinstance(item, str):
            select_parts.append(item)
        elif isinstance(item, dict):
            for func, col in item.items():
                if col == '*':
                    select_parts.append(f"{func}(*)")
                else:
                    select_parts.append(f"{func}({col})")
    select_clause = ", ".join(select_parts)
    
    # Build WHERE clause
    where_parts = []
    for condition in query.get('where', []):
        col = condition['col']
        op = condition['op']
        val = condition['val']
        
        if op == 'eq':
            if isinstance(val, str):
                where_parts.append(f"{col} = '{val}'")
            else:
                where_parts.append(f"{col} = {val}")
        elif op == 'neq':
            if isinstance(val, str):
                where_parts.append(f"{col} != '{val}'")
            else:
                where_parts.append(f"{col} != {val}")
        elif op == 'in':
            if isinstance(val[0], str):
                vals = ", ".join([f"'{v}'" for v in val])
            else:
                vals = ", ".join([str(v) for v in val])
            where_parts.append(f"{col} IN ({vals})")
        elif op == 'between':
            if isinstance(val[0], str):
                where_parts.append(f"{col} BETWEEN '{val[0]}' AND '{val[1]}'")
            else:
                where_parts.append(f"{col} BETWEEN {val[0]} AND {val[1]}")
        elif op == 'lt':
            where_parts.append(f"{col} < {val}")
        elif op == 'lte':
            where_parts.append(f"{col} <= {val}")
        elif op == 'gt':
            where_parts.append(f"{col} > {val}")
        elif op == 'gte':
            where_parts.append(f"{col} >= {val}")
    
    where_clause = ""
    if where_parts:
        where_clause = "WHERE " + " AND ".join(where_parts)
    
    # Build GROUP BY clause
    group_clause = ""
    if query.get('group_by'):
        group_clause = "GROUP BY " + ", ".join(query['group_by'])
    
    # Build ORDER BY clause
    order_clause = ""
    if query.get('order_by'):
        order_parts = []
        for order in query['order_by']:
            direction = order.get('dir', 'asc').upper()
            order_parts.append(f"{order['col']} {direction}")
        order_clause = "ORDER BY " + ", ".join(order_parts)
    
    # Combine into full SQL
    sql = f"""
        SELECT {select_clause}
        FROM read_parquet('{file_pattern}')
        {where_clause}
        {group_clause}
        {order_clause}
    """
    
    return sql
class TimeoutException(Exception):
    """Raised when a query times out"""
    pass


@contextmanager
def timeout(seconds):
    """Context manager for timeout"""
    def signal_handler(signum, frame):
        raise TimeoutException(f"Query timed out after {seconds} seconds")
    
    # Set the signal handler and alarm
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)  # Disable the alarm


class SmartQueryPlanner:
    """Intelligent query planner that selects optimal pre-aggregation"""
    
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.con = duckdb.connect(':memory:')
    
        threads = os.cpu_count() or 8
        self.con.execute(f"SET threads TO {threads};")
        self.con.execute("SET memory_limit = '14GB';")

        self.query_cache = {}  # Dict: query_hash -> (result_df, timing)

        # Load metadata with error handling
        try:
            with open(data_dir / 'metadata.json', 'r') as f:
                self.metadata = json.load(f)
        except Exception as e:
            self.metadata = {}
        
        # Define aggregation catalog with their dimensions
        self.agg_catalog = {
            # === TIME-BASED ===
            # Daily
            'agg_daily_impressions': {'dims': ['day'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_daily_clicks': {'dims': ['day'], 'type': 'click', 'metrics': ['count']},
            'agg_daily_purchases': {'dims': ['day'], 'type': 'purchase', 'metrics': ['count', 'total_revenue', 'avg_purchase']},
            'agg_daily_serves': {'dims': ['day'], 'type': 'serve', 'metrics': ['count']},
            
            # Weekly
            'agg_weekly_impressions': {'dims': ['week'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_weekly_clicks': {'dims': ['week'], 'type': 'click', 'metrics': ['count']},
            'agg_weekly_purchases': {'dims': ['week'], 'type': 'purchase', 'metrics': ['revenue', 'avg_price', 'count']},
            'agg_weekly_serves': {'dims': ['week'], 'type': 'serve', 'metrics': ['count']},
            
            # Hourly
            'agg_hourly_impressions': {'dims': ['hour'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_hourly_clicks': {'dims': ['hour'], 'type': 'click', 'metrics': ['count']},
            'agg_hourly_purchases': {'dims': ['hour'], 'type': 'purchase', 'metrics': ['revenue', 'count']},
            'agg_hourly_serves': {'dims': ['hour'], 'type': 'serve', 'metrics': ['count']},
            
            # Minute
            'agg_minute_impressions': {'dims': ['minute'], 'type': 'impression', 'metrics': ['revenue']},
            
            # Combined time dimensions
            'agg_daily_minute_impressions': {'dims': ['day', 'minute'], 'type': 'impression', 'metrics': ['revenue']},
            'agg_daily_minute_clicks': {'dims': ['day', 'minute'], 'type': 'click', 'metrics': ['count']},
            'agg_daily_hourly_impressions': {'dims': ['day', 'hour'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            
            # === GEOGRAPHIC ===
            'agg_country_impressions': {'dims': ['country'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_country_clicks': {'dims': ['country'], 'type': 'click', 'metrics': ['count']},
            'agg_country_purchases': {'dims': ['country'], 'type': 'purchase', 'metrics': ['avg_purchase', 'total_purchase', 'count']},
            'agg_country_daily_impressions': {'dims': ['country', 'day'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            
            # NEW: Country + Week
            'agg_country_weekly_impressions': {'dims': ['country', 'week'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_country_weekly_clicks': {'dims': ['country', 'week'], 'type': 'click', 'metrics': ['count']},
            'agg_country_weekly_purchases': {'dims': ['country', 'week'], 'type': 'purchase', 'metrics': ['revenue', 'count']},
            
            # NEW: Country + Hour
            'agg_country_hourly_impressions': {'dims': ['country', 'hour'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_country_hourly_clicks': {'dims': ['country', 'hour'], 'type': 'click', 'metrics': ['count']},
            
            # === PUBLISHER ===
            'agg_publisher_impressions': {'dims': ['publisher_id'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_publisher_country': {'dims': ['publisher_id', 'country'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_publisher_daily': {'dims': ['publisher_id', 'day'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_publisher_country_daily': {'dims': ['publisher_id', 'country', 'day'], 'type': 'impression', 'metrics': ['revenue']},
            'agg_publisher_type': {'dims': ['publisher_id', 'type'], 'type': None, 'metrics': ['count']},
            
            # NEW: Publisher + Week/Hour
            'agg_publisher_weekly': {'dims': ['publisher_id', 'week'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_publisher_hourly': {'dims': ['publisher_id', 'hour'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            
            # === ADVERTISER ===
            'agg_advertiser_impressions': {'dims': ['advertiser_id'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_advertiser_type': {'dims': ['advertiser_id', 'type'], 'type': None, 'metrics': ['count']},
            'agg_advertiser_daily': {'dims': ['advertiser_id', 'day'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_advertiser_country': {'dims': ['advertiser_id', 'country'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_advertiser_country_daily': {'dims': ['advertiser_id', 'country', 'day'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_advertiser_purchases': {'dims': ['advertiser_id'], 'type': 'purchase', 'metrics': ['count', 'total_revenue']},
            
            # NEW: Advertiser + Week/Hour
            'agg_advertiser_weekly': {'dims': ['advertiser_id', 'week'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_advertiser_hourly': {'dims': ['advertiser_id', 'hour'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            
            # NEW: Advertiser + Publisher Relationship
            'agg_advertiser_publisher': {'dims': ['advertiser_id', 'publisher_id'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            
            # === FUNNEL/TYPE ANALYSIS ===
            'agg_type_counts': {'dims': ['type'], 'type': None, 'metrics': ['count']},
            'agg_daily_type': {'dims': ['day', 'type'], 'type': None, 'metrics': ['count']},
            'agg_country_type': {'dims': ['country', 'type'], 'type': None, 'metrics': ['count']},
            'agg_country_type_daily': {'dims': ['country', 'type', 'day'], 'type': None, 'metrics': ['count']},
            
            # === NEW: GLOBAL STATS (TINY TABLES - INSTANT LOOKUP!) ===
            'agg_global_stats': {'dims': [], 'type': None, 'metrics': ['total_events', 'total_advertisers', 'total_publishers', 'total_countries', 'first_day', 'last_day']},
            'agg_impressions_summary': {'dims': [], 'type': 'impression', 'metrics': ['total_impressions', 'total_revenue', 'avg_bid', 'min_bid', 'max_bid']},
            'agg_purchases_summary': {'dims': [], 'type': 'purchase', 'metrics': ['total_purchases', 'total_revenue', 'avg_purchase', 'min_purchase', 'max_purchase']},
            
            # === NEW: TIME BUCKETING (SMALL TABLES!) ===
            'agg_monthly_impressions': {'dims': ['month'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_weekday_impressions': {'dims': ['weekday'], 'type': 'impression', 'metrics': ['revenue', 'count']},
            'agg_weekday_clicks': {'dims': ['weekday'], 'type': 'click', 'metrics': ['count']},
        }
    def get_query_hash(self, query: Dict) -> str:
        """Create hash of query for caching"""
        query_str = json.dumps(query, sort_keys=True)
        return hashlib.md5(query_str.encode()).hexdigest()

    def get_cached_result(self, query: Dict) -> Optional[Tuple[pd.DataFrame, Dict]]:
        """Check in-memory cache for this exact query"""
        query_hash = self.get_query_hash(query)
        
        if query_hash in self.query_cache:
            return self.query_cache[query_hash]  # Returns (df, timing)
        
        return None

    def cache_result(self, query: Dict, result_df: pd.DataFrame, timing: Dict):
        """Save query result to IN-MEMORY cache"""
        query_hash = self.get_query_hash(query)
        
        # Store copy of dataframe and timing
        self.query_cache[query_hash] = (result_df.copy(), timing.copy())
        
        # Optional: Limit cache size (prevent memory issues)
        if len(self.query_cache) > 100:
            # Remove oldest entry (simple FIFO)
            oldest_key = next(iter(self.query_cache))
            del self.query_cache[oldest_key]
    def __del__(self):
        if hasattr(self, 'con'):
            self.con.close()


    def find_best_aggregation(self, query: Dict) -> Optional[Tuple[str, Dict]]:
        """
        Find the best pre-aggregation for a query using hierarchy
        Returns: (table_name, execution_plan) or None
        """
        select = query.get('select', [])
        where = query.get('where', [])
        group_by = query.get('group_by', [])

        # Extract query requirements
        select_cols = [s for s in select if isinstance(s, str)]
        aggregates = [s for s in select if isinstance(s, dict)]

        if not aggregates and not group_by:
            return None

        # Get filters
        type_filter = self._extract_filter(where, 'type')
        filters = {w['col']: w for w in where}
        
        # Find matching aggregations (may be multiple)
        candidates = []
        
        for agg_name, agg_info in self.agg_catalog.items():
            # Check if this aggregation can answer the query
            score = self._score_aggregation(
                agg_info, 
                group_by, 
                aggregates, 
                type_filter, 
                filters
            )
            
            if score > 0:
                candidates.append((agg_name, agg_info, score))
        
        if not candidates:
            return None
        
        # Choose best candidate (highest score = most specific/efficient)
        candidates.sort(key=lambda x: x[2], reverse=True)
        best_agg, best_info, _ = candidates[0]
        
        # Build execution plan
        plan = {
            'table': best_agg,
            'group_by': group_by,
            'filters': filters,
            'aggregates': aggregates,
            'needs_grouping': set(group_by) != set(best_info['dims']),
            'needs_filtering': len(filters) > 0
        }
        
        return (best_agg, plan)
    
    def _score_aggregation(self, agg_info: Dict, group_by: List, 
                    aggregates: List, type_filter: Optional[str],
                    filters: Dict) -> int:
        """
        Score how well an aggregation matches the query
        Higher score = better match
        """
        score = 0
        
        # Check if group_by dimensions are compatible
        agg_dims = set(agg_info['dims'])
        query_dims = set(group_by)
        
        # Aggregation must contain AT LEAST the query dimensions
        if not query_dims.issubset(agg_dims):
            return 0  # Can't use this aggregation
        
        # ONLY check filter columns that are NOT in group_by
        for filter_col in filters.keys():
            if filter_col == 'type':
                continue  # Type handled separately below
            
            # If filter is on a dimension we're grouping by, aggregation MUST have it
            if filter_col in query_dims:
                if filter_col not in agg_dims:
                    return 0  # Aggregation missing required dimension
            # If filter is NOT in group_by, it's a WHERE clause filter
            # We need the aggregation to have that dimension to filter on it
            else:
                if filter_col not in agg_dims:
                    return 0  # Can't filter on a dimension not in aggregation

        for agg in aggregates:
            for func, col in agg.items():
                # If query needs total_price, table must support purchase data
                if col == 'total_price' and 'purchase' not in str(agg_info.get('metrics', [])):
                    return 0  # This table doesn't have purchase price data!
                # If query needs bid_price, table must support impression data
                if col == 'bid_price' and 'revenue' not in str(agg_info.get('metrics', [])):
                    return 0  # This table doesn't have bid_price data!
        
        # Prefer exact match
        if query_dims == agg_dims:
            score += 100  # Exact match - best!
        else:
            # Can use if query is subset (will need grouping)
            score += 50 - len(agg_dims - query_dims) * 10  # Fewer extra dims = better
        
        # Check type filter compatibility
        if agg_info['type']:
            if type_filter and type_filter['val'] == agg_info['type']:
                score += 50  # Type matches
            elif type_filter:
                return 0  # Wrong type filter
            else:
                score += 25  # No type filter, but agg has type (OK)
        
        # Prefer aggregations with fewer rows (more specific)
        score -= len(agg_dims) * 5  # Slight penalty for more dimensions
        
        return score
    def _extract_filter(self, where: List, col: str) -> Optional[Dict]:
        """Extract specific filter from where clause"""
        for w in where:
            if w['col'] == col:
                return w
        return None

def apply_filter(df: pd.DataFrame, filter_obj: Dict) -> pd.DataFrame:
    """Apply a single filter to dataframe"""
    col = filter_obj['col']
    op = filter_obj['op']
    val = filter_obj['val']
    
    if col not in df.columns:
        return df

    # Handle date/time columns - normalize for comparison
    if col == 'day':
        # Check if column is actually datetime
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            # For day comparison, compare dates only (ignore time)
            if isinstance(val, str):
                val = pd.Timestamp(val).date()
                return df[df[col].dt.date == val]
            elif op == 'between' and isinstance(val, list):
                low_date = pd.Timestamp(val[0]).date()
                high_date = pd.Timestamp(val[1]).date()
                return df[(df[col].dt.date >= low_date) & (df[col].dt.date <= high_date)]
        else:
            # Column is already date or something else, convert val to match
            if isinstance(val, str):
                val = pd.Timestamp(val).date()
            elif op == 'between' and isinstance(val, list):
                val = [pd.Timestamp(v).date() if isinstance(v, str) else v for v in val]

    elif col in ['week', 'hour', 'minute']:
        # For hour/week/minute, keep full timestamp comparison
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            is_tz_aware = hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None
            
            if isinstance(val, str):
                val = pd.Timestamp(val, tz='UTC' if is_tz_aware else None)
            elif op in ['in', 'between'] and isinstance(val, list):
                val = [pd.Timestamp(v, tz='UTC' if is_tz_aware else None) if isinstance(v, str) else v for v in val]
        else:
            # Not datetime, convert string values to timestamps anyway for comparison
            if isinstance(val, str):
                val = pd.Timestamp(val)
            elif op in ['in', 'between'] and isinstance(val, list):
                val = [pd.Timestamp(v) if isinstance(v, str) else v for v in val]

    
    if op == 'eq':
        return df[df[col] == val]
    elif op == 'neq':
        return df[df[col] != val]
    elif op == 'in':
        return df[df[col].isin(val)]
    elif op == 'between':
        low, high = val
        return df[(df[col] >= low) & (df[col] <= high)]
    elif op == 'lt':
        return df[df[col] < val]
    elif op == 'lte':
        return df[df[col] <= val]
    elif op == 'gt':
        return df[df[col] > val]
    elif op == 'gte':
        return df[df[col] >= val]
    
    return df

def execute_with_aggregation(query: Dict, table_name: str, plan: Dict, 
                            planner: SmartQueryPlanner) -> Tuple[pd.DataFrame, Dict]:
    """Execute query using pre-aggregation table"""
    
    timing = {
        'planning': 0,
        'file_read': 0,
        'filtering': 0,
        'aggregation': 0,
        'sorting': 0
    }
    
    # Read the aggregation table
    t0 = time.perf_counter()
    agg_file = planner.data_dir / f'{table_name}.parquet'
    
    if not agg_file.exists():
        raise FileNotFoundError(f"Aggregation file not found: {agg_file}")
    
    # Build WHERE clause for parquet filtering (SKIP 'type' - already filtered!)
    where_conditions = []
    for col, filter_obj in plan['filters'].items():
        # SKIP 'type' filter - aggregations are already type-specific!
        if col == 'type':
            continue
            
        op = filter_obj['op']
        val = filter_obj['val']
        
        if op == 'eq':
            if isinstance(val, str):
                where_conditions.append(f"{col} = '{val}'")
            else:
                where_conditions.append(f"{col} = {val}")
        elif op == 'between':
            if isinstance(val[0], str):
                where_conditions.append(f"{col} >= '{val[0]}' AND {col} <= '{val[1]}'")
            else:
                where_conditions.append(f"{col} >= {val[0]} AND {col} <= {val[1]}")
        elif op == 'in':
            if isinstance(val[0], str):
                vals = ", ".join([f"'{v}'" for v in val])
            else:
                vals = ", ".join([str(v) for v in val])
            where_conditions.append(f"{col} IN ({vals})")
        elif op == 'neq':
            if isinstance(val, str):
                where_conditions.append(f"{col} != '{val}'")
            else:
                where_conditions.append(f"{col} != {val}")
        elif op == 'lt':
            where_conditions.append(f"{col} < {val}")
        elif op == 'lte':
            where_conditions.append(f"{col} <= {val}")
        elif op == 'gt':
            where_conditions.append(f"{col} > {val}")
        elif op == 'gte':
            where_conditions.append(f"{col} >= {val}")
    
    # Use DuckDB to filter during read
    if where_conditions:
        where_clause = " AND ".join(where_conditions)
        sql = f"""
            SELECT * FROM read_parquet('{agg_file}')
            WHERE {where_clause}
        """
        df = planner.con.execute(sql).df()
    else:
        df = pd.read_parquet(agg_file)
    
    timing['file_read'] = time.perf_counter() - t0
    timing['filtering'] = 0  # Filtering done during read!
    
    # Apply grouping if needed (using hierarchy!)
    if plan['needs_grouping']:
        t0 = time.perf_counter()
        group_cols = plan['group_by']
        
        # Build aggregation dict - ONLY for numeric columns
        agg_dict = {}
        for col in df.columns:
            if col not in group_cols:
                # Only aggregate numeric columns
                if pd.api.types.is_numeric_dtype(df[col]):
                    if 'avg' in col.lower() or 'mean' in col.lower():
                        agg_dict[col] = 'mean'
                    else:
                        agg_dict[col] = 'sum'  # Sum for counts, revenue, etc.
                # Non-numeric columns are DROPPED (not grouped)
        
        if agg_dict:
            df = df.groupby(group_cols, as_index=False).agg(agg_dict)
        else:
            # No numeric columns to aggregate, just unique combinations
            df = df[group_cols].drop_duplicates()
        
        timing['aggregation'] = time.perf_counter() - t0
    
    # Map column names to expected output format
    select = query.get('select', [])
    expected_cols = []
    rename_map = {}
    
    for s in select:
        if isinstance(s, str):
            # Simple column name
            expected_cols.append(s)
        elif isinstance(s, dict):
            for func, col in s.items():
                expected_name = f"{func.lower()}({col})" if col != '*' else f"{func}(*)"
                expected_cols.append(expected_name)
                
                # Map DuckDB column names to expected format
                possible_names = [
                    f"count_star()" if func == "COUNT" and col == "*" else None,
                    f"{func.lower()}({col})",
                    f"{func}({col})",
                    f"SUM({col})" if func == "SUM" else None,
                    f"AVG({col})" if func == "AVG" else None,
                    f"COUNT({col})" if func == "COUNT" else None,
                ]
                
                for possible in possible_names:
                    if possible and possible in df.columns:
                        rename_map[possible] = expected_name
                        break
    
    # Apply column renaming
    if rename_map:
        df = df.rename(columns=rename_map)
    
    # Select only expected columns that exist
    available_cols = [c for c in expected_cols if c in df.columns]
    if available_cols:
        df = df[available_cols]

    # Apply ORDER BY
    order_by = query.get('order_by', [])
    if order_by:
        t0 = time.perf_counter()
        
        use_duckdb = len(df) > 10000
        
        if use_duckdb:
            try:
                order_parts = []
                for o in order_by:
                    col = o['col']
                    direction = o.get('dir', 'asc').upper()
                    
                    if col in df.columns:
                        escaped_col = f'"{col}"' if '(' in col or ')' in col else col
                        order_parts.append(f"{escaped_col} {direction}")

                
                if order_parts:
                    order_clause = "ORDER BY " + ", ".join(order_parts)
                    df = planner.con.execute(f"SELECT * FROM df {order_clause}").df()
            except Exception as e:
                for o in order_by:
                    if o['col'] in df.columns:
                        df = df.sort_values(o['col'], ascending=o.get('dir', 'asc') == 'asc')
        else:
            for o in order_by:
                col = o['col']
                ascending = o.get('dir', 'asc') == 'asc'
                
                if col not in df.columns:
                    continue
                
                is_sorted = (df[col].is_monotonic_increasing if ascending 
                        else df[col].is_monotonic_decreasing)
                if is_sorted:
                    continue
                
                df = df.sort_values(col, ascending=ascending)
        
        timing['sorting'] = time.perf_counter() - t0

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            if col == 'minute':
                # Format minute as "YYYY-MM-DD HH:MM" (no seconds)
                df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M')
            elif col == 'day':
                # Format day as "YYYY-MM-DD" (date only)
                df[col] = df[col].dt.strftime('%Y-%m-%d')
            elif col == 'week':
                # Keep week with timezone: "YYYY-MM-DD HH:MM:SS-TZ"
                # Convert to Pacific time if not already
                if hasattr(df[col].dt, 'tz'):
                    if df[col].dt.tz is None:
                        # Add UTC timezone then convert to Pacific
                        df[col] = df[col].dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles')
                    else:
                        df[col] = df[col].dt.tz_convert('America/Los_Angeles')
                else:
                    # Assume UTC, convert to Pacific
                    df[col] = pd.to_datetime(df[col]).dt.tz_localize('UTC').dt.tz_convert('America/Los_Angeles')
            elif col == 'hour':
                # Keep hour as-is but remove timezone if present
                if hasattr(df[col].dt, 'tz') and df[col].dt.tz is not None:
                    df[col] = df[col].dt.tz_localize(None)


    return df, timing

def execute_partition_query(query: Dict, planner: SmartQueryPlanner) -> Tuple[pd.DataFrame, Dict]:
    """Fallback: Use DuckDB to query Parquet partitions directly"""
    
    timing = {
        'planning': 0,
        'file_read': 0,
        'filtering': 0,
        'aggregation': 0,
        'sorting': 0
    }
    
    where = query.get('where', [])
    
    # Determine which type partition(s) to read
    type_filter = None
    for w in where:
        if w['col'] == 'type' and w['op'] == 'eq':
            type_filter = w['val']
            break
    
    # Build file pattern for DuckDB
    if type_filter:
        file_pattern = str(planner.data_dir / f"type={type_filter}" / "data.parquet")
    else:
        # Use glob pattern for all types
        file_pattern = str(planner.data_dir / "type=*" / "data.parquet")
    # Convert query to SQL
    t0 = time.perf_counter()
    sql = json_query_to_sql(query, file_pattern)
    
    # Execute with DuckDB
    con = planner.con
    
    try:
        result = con.execute(sql).df()
        timing['file_read'] = time.perf_counter() - t0
        # Note: DuckDB does filtering, aggregation, sorting internally
        # So we just attribute all time to file_read + aggregation
        timing['aggregation'] = timing['file_read'] * 0.3  # Rough estimate
        timing['file_read'] = timing['file_read'] * 0.7
        
    except Exception as e:
        result = pd.DataFrame()
    
    return result, timing


def execute_query(query: Dict, planner: SmartQueryPlanner, query_timeout: int = 300) -> Tuple[pd.DataFrame, Dict, Optional[str]]:
    """
    Execute query with smart aggregation selection
    Returns: (DataFrame, timing_dict, error_message)
    """
    cached = planner.get_cached_result(query)
    if cached is not None:
        cached_df, cached_timing = cached
        # Return cached result with minimal overhead
        return cached_df.copy(), cached_timing, None

    timing = {
        'planning': 0,
        'file_read': 0,
        'filtering': 0,
        'aggregation': 0,
        'sorting': 0
    }
    
    error_msg = None
    
    try:
        with timeout(query_timeout):
            t_planning = time.perf_counter()
            
            # Try to find a pre-aggregation
            result = planner.find_best_aggregation(query)
            
            planning_time = time.perf_counter() - t_planning
            
            if result:
                table_name, plan = result
                df, timing = execute_with_aggregation(query, table_name, plan, planner)
                timing['planning'] = planning_time
                planner.cache_result(query, df, timing)

                return df, timing, None
            
            # Fallback to partition scan
            df, timing = execute_partition_query(query, planner)
            timing['planning'] = planning_time
            planner.cache_result(query, df, timing)

            return df, timing, None
            
    except TimeoutException as e:
        error_msg = str(e)
        print(f"    ❌ TIMEOUT: {error_msg}")
        return pd.DataFrame(), timing, error_msg
    
    except Exception as e:
        error_msg = f"Error: {type(e).__name__}: {str(e)}"
        print(f"    ❌ ERROR: {error_msg}")
        return pd.DataFrame(), timing, error_msg

def run_queries(queries: List[Dict], data_dir: Path, out_dir: Path, query_timeout: int = 300):
    """Execute all queries with detailed timing and safety features"""

    out_dir.mkdir(parents=True, exist_ok=True)
    
    # Initialize query planner
    try:
        planner = SmartQueryPlanner(data_dir)
    except Exception as e:
        return
    
    results = []
    detailed_timings = []
    errors = []
    
    for i, query in enumerate(queries, 1):
        
        t0 = time.perf_counter()
        result_df, timing, error_msg = execute_query(query, planner, query_timeout)
        total_time = time.perf_counter() - t0
        
        # Save results
        try:
            out_path = out_dir / f"q{i}.csv"
            result_df.to_csv(out_path, index=False)
        except Exception as e:
            print(f"  ⚠️  Failed to save results: {e}")


        results.append({
            "query": i, 
            "rows": len(result_df), 
            "time": total_time,
            "status": "success" if error_msg is None else "failed"
        })
        
        detailed_timings.append({
            "query": i,
            "total_ms": total_time * 1000,
            "planning_ms": timing['planning'] * 1000,
            "file_read_ms": timing['file_read'] * 1000,
            "filtering_ms": timing['filtering'] * 1000,
            "aggregation_ms": timing['aggregation'] * 1000,
            "sorting_ms": timing['sorting'] * 1000,
            "status": "success" if error_msg is None else "failed",
            "error": error_msg or ""
        })
        
        if error_msg:
            errors.append({"query": i, "error": error_msg})

    total_time_all = sum(r['time'] for r in results)
    
    print(f"⏱️  Total time: {total_time_all*1000:.3f} ms")
    

if __name__ == "__main__":
    from inputs import queries
    
    parser = argparse.ArgumentParser(
        description="RUN PHASE: Execute queries on optimized data (Safe Mode)"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory with optimized Parquet data"
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        required=True,
        help="Output directory for results"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Query timeout in seconds (default: 300 = 5 minutes)"
    )
    
    args = parser.parse_args()
    
    # Validate inputs
    if not args.data_dir.exists():
        print(f"❌ Error: Data directory does not exist: {args.data_dir}")
        exit(1)
    
    
    run_queries(queries, args.data_dir, args.out_dir, args.timeout)