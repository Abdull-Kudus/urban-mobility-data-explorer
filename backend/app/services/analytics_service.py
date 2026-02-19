from app.db.connection import get_connection
from app.algorithms.custom_algorithm import (
    rank_zones_by_revenue,
    get_top_pickup_hours,
    group_trips_by_key,
    detect_anomalies,
    detect_multiple_anomalies
)


def get_hourly_demand() -> list[dict]:
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


def get_revenue_by_zone() -> list[dict]:
    sql = """
        SELECT
            tz.zone_name,
            b.borough_name,
            COUNT(t.trip_id)              AS trip_count,
            ROUND(SUM(t.total_amount), 2) AS total_revenue,
            ROUND(AVG(t.total_amount), 2) AS avg_revenue_per_trip
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
        r["total_revenue"]        = float(r["total_revenue"])        if r["total_revenue"]        else 0.0
        r["avg_revenue_per_trip"] = float(r["avg_revenue_per_trip"]) if r["avg_revenue_per_trip"] else 0.0
        result.append(r)
    return result


def get_avg_fare_per_distance() -> list[dict]:
    sql = """
        SELECT
            CASE
                WHEN trip_distance < 1  THEN '0-1 miles'
                WHEN trip_distance < 3  THEN '1-3 miles'
                WHEN trip_distance < 5  THEN '3-5 miles'
                WHEN trip_distance < 10 THEN '5-10 miles'
                WHEN trip_distance < 20 THEN '10-20 miles'
                ELSE                         '20+ miles'
            END                              AS distance_bucket,
            COUNT(*)                         AS trip_count,
            ROUND(AVG(total_amount), 2)      AS avg_fare,
            ROUND(AVG(fare_per_mile), 4)     AS avg_fare_per_mile,
            ROUND(AVG(avg_speed_mph), 2)     AS avg_speed_mph
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


def get_top_revenue_zones(top_n: int = 10) -> list[dict]:
    sql = """
        SELECT
            tz.zone_name,
            b.borough_name,
            COUNT(t.trip_id)              AS trip_count,
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

    raw_data = []
    for r in raw_rows:
        r = dict(r)
        r["total_revenue"] = float(r["total_revenue"]) if r["total_revenue"] else 0.0
        raw_data.append(r)

    sorted_data = rank_zones_by_revenue(raw_data)

    return sorted_data[:top_n]


def get_top_pickup_hours_manual(top_n: int = 5) -> list[dict]:
    """
    Get top N pickup hours using manual top-k selection algorithm.
    This demonstrates the top-k selection algorithm without using built-in sorting.
    """
    sql = """
        SELECT
            pickup_hour,
            COUNT(*) AS trip_count
        FROM trips
        WHERE pickup_hour IS NOT NULL
        GROUP BY pickup_hour
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    hourly_data = []
    for r in rows:
        r = dict(r)
        r["trip_count"] = int(r["trip_count"])
        hourly_data.append(r)

    # Use manual top-k selection instead of SQL ORDER BY
    top_hours = get_top_pickup_hours(hourly_data, top_n)
    
    return top_hours


def get_trips_grouped_by_zone_manual(limit: int = 1000) -> list[dict]:
    """
    Group trips by zone using custom hash map implementation.
    This demonstrates manual grouping without SQL GROUP BY.
    """
    sql = """
        SELECT
            tz.zone_name,
            b.borough_name,
            t.total_amount,
            t.trip_distance,
            t.trip_duration_minutes
        FROM trips t
        JOIN taxi_zones tz ON t.pickup_location_id = tz.location_id
        JOIN boroughs   b  ON tz.borough_id        = b.borough_id
        LIMIT %s
    """
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    trips = []
    for r in rows:
        trips.append({
            'zone_name': r['zone_name'],
            'borough_name': r['borough_name'],
            'total_amount': float(r['total_amount']) if r['total_amount'] else 0.0,
            'trip_distance': float(r['trip_distance']) if r['trip_distance'] else 0.0,
            'trip_duration_minutes': float(r['trip_duration_minutes']) if r['trip_duration_minutes'] else 0.0
        })

    # Use custom hash map for grouping
    grouped = group_trips_by_key(
        trips, 
        group_key='zone_name',
        aggregate_key='total_amount',
        operation='sum'
    )

    return grouped


def get_anomalous_trips(field: str = 'total_amount', 
                        threshold: float = 3.0,
                        limit: int = 10000) -> list[dict]:
    """
    Detect anomalous trips using manual Z-score calculation.
    This demonstrates anomaly detection without using statistical libraries.
    """
    # Validate field name to prevent SQL injection
    valid_fields = {
        'total_amount': 'total_amount',
        'trip_distance': 'trip_distance',
        'trip_duration_minutes': 'trip_duration_minutes',
        'fare_per_mile': 'fare_per_mile',
        'avg_speed_mph': 'avg_speed_mph'
    }
    
    if field not in valid_fields:
        raise ValueError(f"Invalid field: {field}")
    
    safe_field = valid_fields[field]
    
    sql = f"""
        SELECT
            trip_id,
            pickup_datetime,
            tz.zone_name,
            b.borough_name,
            total_amount,
            trip_distance,
            trip_duration_minutes,
            fare_per_mile,
            avg_speed_mph
        FROM trips t
        JOIN taxi_zones tz ON t.pickup_location_id = tz.location_id
        JOIN boroughs   b  ON tz.borough_id        = b.borough_id
        WHERE {safe_field} IS NOT NULL
        LIMIT %s
    """
    
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, (limit,))
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    trips = []
    for r in rows:
        trips.append({
            'trip_id': r['trip_id'],
            'pickup_datetime': str(r['pickup_datetime']) if r['pickup_datetime'] else None,
            'zone_name': r['zone_name'],
            'borough_name': r['borough_name'],
            'total_amount': float(r['total_amount']) if r['total_amount'] else None,
            'trip_distance': float(r['trip_distance']) if r['trip_distance'] else None,
            'trip_duration_minutes': float(r['trip_duration_minutes']) if r['trip_duration_minutes'] else None,
            'fare_per_mile': float(r['fare_per_mile']) if r['fare_per_mile'] else None,
            'avg_speed_mph': float(r['avg_speed_mph']) if r['avg_speed_mph'] else None
        })

    # Use manual anomaly detection
    anomalies = detect_anomalies(trips, field, threshold)
    
    return anomalies