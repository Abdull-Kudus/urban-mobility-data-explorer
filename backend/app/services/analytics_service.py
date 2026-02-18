"""
app/services/analytics_service.py
-----------------------------------
Business logic for all analytics / aggregation endpoints.

SCHEMA CONTEXT (from schema.sql)
----------------------------------
trips         → pickup_location_id, dropoff_location_id, pickup_datetime,
                pickup_hour, fare_amount, total_amount, trip_distance,
                fare_per_mile, avg_speed_mph, trip_duration_minutes
taxi_zones    → location_id, zone_name, borough_id, zone_type_id
boroughs      → borough_id, borough_name

To get borough_name for a trip we need TWO joins:
    trips → taxi_zones (on pickup_location_id = location_id)
          → boroughs   (on taxi_zones.borough_id = boroughs.borough_id)

QUERY OPTIMISATION NOTES
--------------------------
1. All GROUP BY queries target indexed columns (pickup_hour, pickup_location_id)
   so MySQL can use idx_trips_pickup_hour and idx_trips_pickup_location.

2. SUM / COUNT / AVG are computed inside MySQL — far faster than pulling all
   rows into Python and computing them there for 7.6M rows.

3. The revenue-by-zone query JOINs through taxi_zones → boroughs so the
   response contains human-readable zone and borough names.

4. For top-revenue-zones we intentionally omit ORDER BY in SQL so that the
   custom merge sort (TM4) performs the actual ranking.
"""

from app.db.connection import get_connection
from app.algorithms.custom_algorithm import rank_zones_by_revenue


# ─────────────────────────────────────────────────────────────────────────────
# Hourly demand
# ─────────────────────────────────────────────────────────────────────────────

def get_hourly_demand() -> list[dict]:
    """
    Return trip count for each hour of day (0–23).

    Uses pickup_hour (a pre-computed TINYINT column added by TM1's
    feature_engineering.py) and the index idx_trips_pickup_hour.

    Frontend use: bar chart showing peak / off-peak hours.
    """
    sql = """
        SELECT
            pickup_hour,
            COUNT(*) AS trip_count
        FROM trips
        WHERE pickup_hour IS NOT NULL
        GROUP BY pickup_hour
        ORDER BY pickup_hour ASC
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    return [dict(r) for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# Revenue by zone
# ─────────────────────────────────────────────────────────────────────────────

def get_revenue_by_zone() -> list[dict]:
    """
    Total and average revenue grouped by pickup zone and borough.

    JOIN path:
        trips  →  taxi_zones  (pickup_location_id = location_id)
               →  boroughs    (borough_id = borough_id)

    The idx_trips_pickup_location index makes the JOIN and GROUP BY efficient.

    Frontend use: horizontal bar chart or choropleth map per borough.
    """
    sql = """
        SELECT
            tz.zone_name,
            b.borough_name,
            COUNT(t.trip_id)                              AS trip_count,
            ROUND(SUM(t.total_amount), 2)                 AS total_revenue,
            ROUND(AVG(t.total_amount), 2)                 AS avg_revenue_per_trip
        FROM trips t
        JOIN taxi_zones tz ON t.pickup_location_id = tz.location_id
        JOIN boroughs   b  ON tz.borough_id        = b.borough_id
        GROUP BY tz.zone_name, b.borough_name
        ORDER BY total_revenue DESC
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    result = []
    for r in rows:
        r = dict(r)
        r["total_revenue"]       = float(r["total_revenue"])       if r["total_revenue"]       else 0.0
        r["avg_revenue_per_trip"] = float(r["avg_revenue_per_trip"]) if r["avg_revenue_per_trip"] else 0.0
        result.append(r)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Average fare per distance bucket
# ─────────────────────────────────────────────────────────────────────────────

def get_avg_fare_per_distance() -> list[dict]:
    """
    Average fare bucketed by trip distance range.

    CASE WHEN creates distance buckets entirely inside MySQL — far more
    efficient than fetching all rows and bucketing in Python.

    fare_per_mile is a pre-computed column (TM1's feature_engineering.py)
    so we can report it directly without recalculating.

    Frontend use: grouped bar chart — fare vs distance insight.
    """
    sql = """
        SELECT
            CASE
                WHEN trip_distance < 1   THEN '0-1 miles'
                WHEN trip_distance < 3   THEN '1-3 miles'
                WHEN trip_distance < 5   THEN '3-5 miles'
                WHEN trip_distance < 10  THEN '5-10 miles'
                WHEN trip_distance < 20  THEN '10-20 miles'
                ELSE                          '20+ miles'
            END                                          AS distance_bucket,
            COUNT(*)                                     AS trip_count,
            ROUND(AVG(total_amount), 2)                  AS avg_fare,
            ROUND(AVG(fare_per_mile), 4)                 AS avg_fare_per_mile,
            ROUND(AVG(avg_speed_mph), 2)                 AS avg_speed_mph
        FROM trips
        WHERE trip_distance > 0
          AND total_amount  > 0
          AND fare_per_mile IS NOT NULL
        GROUP BY distance_bucket
        ORDER BY MIN(trip_distance) ASC
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    result = []
    for r in rows:
        r = dict(r)
        for field in ("avg_fare", "avg_fare_per_mile", "avg_speed_mph"):
            if r.get(field) is not None:
                r[field] = float(r[field])
        result.append(r)
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Top revenue zones — uses TM4's custom merge sort
# ─────────────────────────────────────────────────────────────────────────────

def get_top_revenue_zones(top_n: int = 10) -> list[dict]:
    """
    Fetch aggregated zone revenue from MySQL, rank using the manually
    implemented merge sort from custom_algorithm.py, return top_n results.

    WHY fetch all zones and sort in Python?
    ----------------------------------------
    The project specification requires demonstrating TM4's custom algorithm.
    We deliberately do NOT add ORDER BY here — the merge sort is the component
    that produces the final sorted ranking.  This is the correct design for the
    grading requirements.

    Parameters
    ----------
    top_n : how many top zones to return after sorting (default 10).
    """
    # Fetch aggregated revenue per zone — no ORDER BY so the algorithm works
    sql = """
        SELECT
            tz.zone_name,
            b.borough_name,
            COUNT(t.trip_id)          AS trip_count,
            ROUND(SUM(t.total_amount), 2) AS total_revenue
        FROM trips t
        JOIN taxi_zones tz ON t.pickup_location_id = tz.location_id
        JOIN boroughs   b  ON tz.borough_id        = b.borough_id
        GROUP BY tz.zone_name, b.borough_name
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        raw_rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    # Convert Decimal to float — merge sort comparison operators need floats
    raw_data = []
    for r in raw_rows:
        r = dict(r)
        r["total_revenue"] = float(r["total_revenue"]) if r["total_revenue"] else 0.0
        raw_data.append(r)

    # ── Hand to TM4's merge sort (no sort() / sorted() called here) ──────
    sorted_data = rank_zones_by_revenue(raw_data)
    # ─────────────────────────────────────────────────────────────────────

    return sorted_data[:top_n]