"""
Query Router - Intelligent query planning with pre-aggregation matching

Attempts to rewrite queries to use pre-aggregated tables when possible.
Falls back to partitioned Parquet data for queries that don't match.
"""
from pathlib import Path


DATA_STORE = Path("data_store")


def try_q1_pattern(q):
    """
    Q1 Pattern: Daily revenue totals
    SELECT day, SUM(bid_price) FROM events WHERE type='impression' GROUP BY day
    """
    # Check structure
    select = q.get('select', [])
    where = q.get('where', [])
    group_by = q.get('group_by', [])

    # Must group by day only
    if group_by != ['day']:
        return None

    # Must have day and SUM(bid_price) in select
    if len(select) != 2:
        return None

    has_day = 'day' in select
    has_sum_bid = any(isinstance(s, dict) and s.get('SUM') == 'bid_price' for s in select)

    if not (has_day and has_sum_bid):
        return None

    # Must filter by type='impression'
    has_impression_filter = any(
        w.get('col') == 'type' and w.get('op') == 'eq' and w.get('val') == 'impression'
        for w in where
    )

    if not has_impression_filter:
        return None

    # Check if there are other filters (if so, can't use pre-agg)
    if len(where) > 1:
        return None

    # Match! Use pre-agg table and aggregate from minute to day
    print(f"[ROUTER] Q1 pattern matched - using revenue pre-aggregation")
    return f"""
        SELECT
            day,
            SUM(total_revenue) AS "sum(bid_price)"
        FROM read_parquet('{DATA_STORE}/agg_revenue_by_minute_publisher_country.parquet')
        GROUP BY day
    """


def try_q2_pattern(q):
    """
    Q2 Pattern: Publisher revenue by country and date range
    SELECT publisher_id, SUM(bid_price)
    FROM events
    WHERE type='impression' AND country='JP' AND day BETWEEN '2024-10-20' AND '2024-10-23'
    GROUP BY publisher_id
    """
    select = q.get('select', [])
    where = q.get('where', [])
    group_by = q.get('group_by', [])

    # Must group by publisher_id only
    if group_by != ['publisher_id']:
        return None

    # Must have publisher_id and SUM(bid_price)
    if len(select) != 2:
        return None

    has_publisher = 'publisher_id' in select
    has_sum_bid = any(isinstance(s, dict) and s.get('SUM') == 'bid_price' for s in select)

    if not (has_publisher and has_sum_bid):
        return None

    # Parse WHERE conditions
    type_filter = None
    country_filter = None
    day_filter = None

    for w in where:
        col = w.get('col')
        op = w.get('op')
        val = w.get('val')

        if col == 'type' and op == 'eq':
            type_filter = val
        elif col == 'country' and op == 'eq':
            country_filter = val
        elif col == 'day' and op == 'between':
            day_filter = val

    # Must have type='impression'
    if type_filter != 'impression':
        return None

    # Must have country and day filters
    if not country_filter or not day_filter:
        return None

    # Match! Use pre-agg table
    print(f"[ROUTER] Q2 pattern matched - using revenue pre-aggregation")
    day_start, day_end = day_filter
    return f"""
        SELECT
            publisher_id,
            SUM(total_revenue) AS "sum(bid_price)"
        FROM read_parquet('{DATA_STORE}/agg_revenue_by_minute_publisher_country.parquet')
        WHERE country = '{country_filter}'
          AND day BETWEEN '{day_start}' AND '{day_end}'
        GROUP BY publisher_id
    """


def try_q3_pattern(q):
    """
    Q3 Pattern: Average purchase value by country
    SELECT country, AVG(total_price)
    FROM events
    WHERE type='purchase'
    GROUP BY country
    ORDER BY AVG(total_price) DESC
    """
    select = q.get('select', [])
    where = q.get('where', [])
    group_by = q.get('group_by', [])
    order_by = q.get('order_by', [])

    # Must group by country only
    if group_by != ['country']:
        return None

    # Must have country and AVG(total_price)
    if len(select) != 2:
        return None

    has_country = 'country' in select
    has_avg_price = any(isinstance(s, dict) and s.get('AVG') == 'total_price' for s in select)

    if not (has_country and has_avg_price):
        return None

    # Must filter by type='purchase'
    has_purchase_filter = any(
        w.get('col') == 'type' and w.get('op') == 'eq' and w.get('val') == 'purchase'
        for w in where
    )

    if not has_purchase_filter or len(where) > 1:
        return None

    # Match! Use pre-agg table and compute AVG from SUM/COUNT
    print(f"[ROUTER] Q3 pattern matched - using purchase pre-aggregation")

    # Build ORDER BY clause if present
    order_clause = ""
    if order_by:
        order_parts = []
        for o in order_by:
            col = o.get('col')
            direction = o.get('dir', 'asc').upper()
            # Map AVG(total_price) to reference the SELECT alias
            if col == 'AVG(total_price)':
                order_parts.append(f'"avg(total_price)" {direction}')
            else:
                order_parts.append(f"{col} {direction}")
        order_clause = "ORDER BY " + ", ".join(order_parts)

    return f"""
        SELECT
            country,
            sum_of_price / count_of_purchases AS "avg(total_price)"
        FROM read_parquet('{DATA_STORE}/agg_purchase_summary.parquet')
        {order_clause}
    """


def try_q4_pattern(q):
    """
    Q4 Pattern: Event counts by advertiser and type
    SELECT advertiser_id, type, COUNT(*)
    FROM events
    GROUP BY advertiser_id, type
    ORDER BY COUNT(*) DESC
    """
    select = q.get('select', [])
    where = q.get('where', [])
    group_by = q.get('group_by', [])
    order_by = q.get('order_by', [])

    # Must group by advertiser_id and type
    if set(group_by) != {'advertiser_id', 'type'}:
        return None

    # Must have advertiser_id, type, and COUNT(*)
    if len(select) != 3:
        return None

    has_advertiser = 'advertiser_id' in select
    has_type = 'type' in select
    has_count = any(isinstance(s, dict) and s.get('COUNT') == '*' for s in select)

    if not (has_advertiser and has_type and has_count):
        return None

    # Should have no WHERE clause (or we can't use pre-agg safely)
    if where:
        return None

    # Match! Use pre-agg table directly
    print(f"[ROUTER] Q4 pattern matched - using advertiser counts pre-aggregation")

    # Build ORDER BY clause
    order_clause = ""
    if order_by:
        order_parts = []
        for o in order_by:
            col = o.get('col')
            direction = o.get('dir', 'asc').upper()
            # Map COUNT(*) to our column alias
            if col == 'COUNT(*)':
                order_parts.append(f'"count_star()" {direction}')
            else:
                order_parts.append(f"{col} {direction}")
        order_clause = "ORDER BY " + ", ".join(order_parts)

    return f"""
        SELECT
            advertiser_id,
            type,
            event_count AS "count_star()"
        FROM read_parquet('{DATA_STORE}/agg_counts_by_advertiser_type.parquet')
        {order_clause}
    """


def try_q5_pattern(q):
    """
    Q5 Pattern: Revenue by minute for a specific day
    SELECT minute, SUM(bid_price)
    FROM events
    WHERE type='impression' AND day='2024-06-01'
    GROUP BY minute
    ORDER BY minute ASC
    """
    select = q.get('select', [])
    where = q.get('where', [])
    group_by = q.get('group_by', [])
    order_by = q.get('order_by', [])

    # Must group by minute only
    if group_by != ['minute']:
        return None

    # Must have minute and SUM(bid_price)
    if len(select) != 2:
        return None

    has_minute = 'minute' in select
    has_sum_bid = any(isinstance(s, dict) and s.get('SUM') == 'bid_price' for s in select)

    if not (has_minute and has_sum_bid):
        return None

    # Parse WHERE conditions
    type_filter = None
    day_filter = None

    for w in where:
        col = w.get('col')
        op = w.get('op')
        val = w.get('val')

        if col == 'type' and op == 'eq':
            type_filter = val
        elif col == 'day' and op == 'eq':
            day_filter = val

    # Must have type='impression' and day filter
    if type_filter != 'impression' or not day_filter:
        return None

    # Check no other filters
    if len(where) > 2:
        return None

    # Match! Use pre-agg table
    print(f"[ROUTER] Q5 pattern matched - using revenue pre-aggregation")

    # Build ORDER BY clause
    order_clause = ""
    if order_by:
        order_parts = []
        for o in order_by:
            col = o.get('col')
            direction = o.get('dir', 'asc').upper()
            order_parts.append(f"{col} {direction}")
        order_clause = "ORDER BY " + ", ".join(order_parts)

    return f"""
        SELECT
            minute,
            SUM(total_revenue) AS "sum(bid_price)"
        FROM read_parquet('{DATA_STORE}/agg_revenue_by_minute_publisher_country.parquet')
        WHERE day = '{day_filter}'
        GROUP BY minute
        {order_clause}
    """


def route_query(q):
    """
    Try to match query to a pre-aggregation pattern.
    Returns SQL string if match found, None otherwise.
    """
    # Try each pattern in order
    sql = (try_q1_pattern(q) or
           try_q2_pattern(q) or
           try_q3_pattern(q) or
           try_q4_pattern(q) or
           try_q5_pattern(q))

    return sql
