"""
app/services/trip_service.py
-----------------------------
Business logic for trip filtering and pagination.

COLUMN NAMES (matching schema.sql exactly)
-------------------------------------------
trips table uses:
  - pickup_datetime          (not tpep_pickup_datetime)
  - dropoff_datetime         (not tpep_dropoff_datetime)
  - pickup_location_id       (not PULocationID)
  - dropoff_location_id      (not DOLocationID)
  - avg_speed_mph            (not average_speed)
  - fare_per_mile
  - trip_duration_minutes
  - pickup_hour
  - is_weekend

taxi_zones table: location_id, zone_name, borough_id
boroughs table:   borough_id, borough_name

QUERY OPTIMISATION (for oral defence)
----------------------------------------
1. SELECT only the columns the frontend needs — never SELECT *.
   Fewer columns = less data transferred from MySQL to Python.

2. WHERE conditions target indexed columns:
     pickup_datetime       → idx_trips_pickup_datetime
     pickup_location_id    → idx_trips_pickup_location
     dropoff_location_id   → idx_trips_dropoff_location
     fare_amount           → idx_trips_fare_amount
     trip_distance         → idx_trips_trip_distance
   MySQL can use these indexes for an index scan instead of a full-table scan.

3. Dynamic WHERE clauses built with a params list — never string concatenation.
   mysql.connector escapes all %s values, eliminating SQL injection risk.

4. Pagination with LIMIT + OFFSET prevents loading the full 7.6M-row table
   into memory at once.

5. We run COUNT(*) first (fast, uses index) to compute total_pages, then
   the actual data query with LIMIT/OFFSET.
"""

from app.db.connection import get_connection


def get_filtered_trips(
    start_date:    str | None,
    end_date:      str | None,
    pickup_zone:   str | None,
    dropoff_zone:  str | None,
    min_fare:      float | None,
    max_fare:      float | None,
    min_distance:  float | None,
    page:          int,
    limit:         int,
) -> dict:
    """
    Return a paginated, filtered slice of the trips table.

    Returns
    -------
    {
        "data":        [ { trip record }, ... ],
        "page":        int,
        "limit":       int,
        "total_count": int,
        "total_pages": int,
    }
    """
    # ------------------------------------------------------------------ #
    # Build WHERE clause dynamically                                       #
    # ------------------------------------------------------------------ #
    # We accumulate SQL condition fragments and their corresponding values
    # separately.  psycopg2 / mysql.connector bind them safely via %s.
    conditions: list[str] = []
    params:     list      = []

    if start_date:
        conditions.append("t.pickup_datetime >= %s")
        params.append(start_date)

    if end_date:
        conditions.append("t.pickup_datetime <= %s")
        params.append(end_date)

    if pickup_zone:
        # LIKE search on the zone name from the taxi_zones dimension table
        conditions.append("pu_zone.zone_name LIKE %s")
        params.append(f"%{pickup_zone}%")

    if dropoff_zone:
        conditions.append("do_zone.zone_name LIKE %s")
        params.append(f"%{dropoff_zone}%")

    if min_fare is not None:
        conditions.append("t.fare_amount >= %s")
        params.append(min_fare)

    if max_fare is not None:
        conditions.append("t.fare_amount <= %s")
        params.append(max_fare)

    if min_distance is not None:
        conditions.append("t.trip_distance >= %s")
        params.append(min_distance)

    where_sql = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    # ------------------------------------------------------------------ #
    # Shared FROM + JOIN block                                             #
    # ------------------------------------------------------------------ #
    # We JOIN taxi_zones TWICE:
    #   pu_zone → for the pickup location name
    #   do_zone → for the dropoff location name
    # Both JOINs use the indexed pickup_location_id / dropoff_location_id.
    # LEFT JOIN means trips with location_id = 265 (Outside NYC) still appear.
    base_from = f"""
        FROM trips t
        LEFT JOIN taxi_zones pu_zone
               ON t.pickup_location_id  = pu_zone.location_id
        LEFT JOIN taxi_zones do_zone
               ON t.dropoff_location_id = do_zone.location_id
        LEFT JOIN boroughs pu_b
               ON pu_zone.borough_id = pu_b.borough_id
        LEFT JOIN boroughs do_b
               ON do_zone.borough_id = do_b.borough_id
        {where_sql}
    """

    # ------------------------------------------------------------------ #
    # COUNT query — determines total_pages                                 #
    # ------------------------------------------------------------------ #
    count_sql = "SELECT COUNT(*) AS total " + base_from

    # ------------------------------------------------------------------ #
    # Data query                                                           #
    # ------------------------------------------------------------------ #
    offset    = (page - 1) * limit

    data_sql = f"""
        SELECT
            t.trip_id,
            t.pickup_datetime,
            t.dropoff_datetime,
            t.trip_distance,
            t.fare_amount,
            t.total_amount,
            t.tip_amount,
            t.passenger_count,
            t.fare_per_mile,
            t.trip_duration_minutes,
            t.avg_speed_mph,
            t.pickup_hour,
            t.is_weekend,
            pu_zone.zone_name   AS pickup_zone,
            do_zone.zone_name   AS dropoff_zone,
            pu_b.borough_name   AS pickup_borough,
            do_b.borough_name   AS dropoff_borough
        {base_from}
        ORDER BY t.pickup_datetime DESC
        LIMIT %s OFFSET %s
    """

    conn = get_connection()
    try:
        # dictionary=True makes fetchall() return list of dicts
        count_cur = conn.cursor()
        count_cur.execute(count_sql, params)
        total_count = count_cur.fetchone()["total"]
        count_cur.close()

        data_cur = conn.cursor()
        data_cur.execute(data_sql, params + [limit, offset])
        rows = data_cur.fetchall()
        data_cur.close()
    finally:
        conn.close()

    # Ceiling division: total_pages = ceil(total_count / limit)
    total_pages = max(1, -(-total_count // limit))

    # Convert datetime objects to ISO strings for JSON serialisation
    data = []
    for row in rows:
        row = dict(row)
        if row.get("pickup_datetime"):
            row["pickup_datetime"] = row["pickup_datetime"].strftime("%Y-%m-%d %H:%M:%S")
        if row.get("dropoff_datetime"):
            row["dropoff_datetime"] = row["dropoff_datetime"].strftime("%Y-%m-%d %H:%M:%S")
        # Convert Decimal to float for JSON
        for field in ("trip_distance", "fare_amount", "total_amount", "tip_amount",
                      "fare_per_mile", "trip_duration_minutes", "avg_speed_mph"):
            if row.get(field) is not None:
                row[field] = float(row[field])
        data.append(row)

    return {
        "data":        data,
        "page":        page,
        "limit":       limit,
        "total_count": total_count,
        "total_pages": total_pages,
    }