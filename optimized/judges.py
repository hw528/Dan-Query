#!/usr/bin/env python3

"""
Source of queries to test
"""

queries = [
    {
        "select": [{"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-07-01", "2024-08-01"]}
        ]
    },
    {
        "select": [{"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "eq", "val": "CA"},
            {"col": "day", "op": "eq", "val": "2024-07-01"}
        ]
    },
    {
        "select": ["publisher_id", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"},
            {"col": "day", "op": "between", "val": ["2024-01-01", "2025-01-01"]}
        ],
        "group_by": ["publisher_id"],
        "order_by": [{"col": "COUNT(*)", "dir": "desc"}]
    },
    {
        "select": ["minute", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-03-01"}
        ],
        "group_by": ["minute"],
        "order_by": [{"col": "minute", "dir": "asc"}]
    },
    {
        "select": ["minute", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "eq", "val": "2024-03-01"}
        ],
        "group_by": ["minute"],
        "order_by": [{"col": "minute", "dir": "asc"}]
    },
    {
        "select": [{"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
        ]
    },
    {
        "select": [{"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "advertiser_id", "op": "eq", "val": 1234},
            {"col": "type", "op": "eq", "val": "purchase"}
        ]
    },
    {
        "select": ["advertiser_id", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"}
        ],
        "group_by": ["advertiser_id"],
        "order_by": [{"col": "advertiser_id", "dir": "asc"}]
    },
    {
        "select": ["country", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"}
        ],
        "group_by": ["country"],
        "order_by": [{"col": "country", "dir": "asc"}]
    },
    {
        "select": ["country", {"SUM": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"}
        ],
        "group_by": ["country"],
        "order_by": [{"col": "country", "dir": "asc"}]
    },
    {
        "select": ["advertiser_id", "country", {"AVG": "total_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "purchase"},
            {"col": "day", "op": "eq", "val": "2024-03-01"}
        ],
        "group_by": ["advertiser_id", "country"],
        "order_by": [{"col": "advertiser_id", "dir": "asc"}]
    },
    {
        "select": ["week", {"SUM": "bid_price"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "day", "op": "between", "val": ["2024-01-01", "2025-01-01"]}
        ],
        "group_by": ["week"],
        "order_by": [{"col": "week", "dir": "asc"}]
    },
    {
        "select": ["day", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "country", "op": "eq", "val": "GB"},
            {"col": "type", "op": "eq", "val": "click"}
        ],
        "group_by": ["day"],
        "order_by": [{"col": "day", "dir": "asc"}]
    },
    {
        "select": ["country", {"COUNT": "*"}],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "click"}
        ],
        "group_by": ["country"],
        "order_by": [{"col": "country", "dir": "asc"}]
    },
    {
        "select": ["publisher_id"],
        "from": "events",
        "where": [
            {"col": "type", "op": "eq", "val": "impression"},
            {"col": "country", "op": "neq", "val": "US"},
            {"col": "day", "op": "eq", "val": "2024-01-01"}
        ],
        "group_by": ["publisher_id"],
        "order_by": [{"col": "publisher_id", "dir": "asc"}]
    }
]