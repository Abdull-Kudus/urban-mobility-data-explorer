"""
app/routes/analytics.py
------------------------
Blueprint for all /api/analytics/* endpoints.

Registered with url_prefix="/api/analytics" in app/__init__.py, so:
  /hourly-demand       → GET /api/analytics/hourly-demand
  /revenue-by-zone     → GET /api/analytics/revenue-by-zone
  /average-fare-per-mile → GET /api/analytics/average-fare-per-mile
  /top-revenue-zones   → GET /api/analytics/top-revenue-zones
"""

from flask import Blueprint, request, jsonify, current_app
from app.services.analytics_service import (
    get_hourly_demand,
    get_revenue_by_zone,
    get_avg_fare_per_distance,
    get_top_revenue_zones,
)

analytics_bp = Blueprint("analytics", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/analytics/hourly-demand
# ─────────────────────────────────────────────────────────────────────────────

@analytics_bp.route("/hourly-demand", methods=["GET"])
def hourly_demand():
    """
    Trip count grouped by hour of day (0–23).

    Uses the pickup_hour column (pre-computed by TM1's feature_engineering.py)
    and the idx_trips_pickup_hour index for fast GROUP BY.

    Response (200)
    --------------
    {
        "data": [
            {"pickup_hour": 0,  "trip_count": 3421},
            {"pickup_hour": 1,  "trip_count": 2103},
            ...
            {"pickup_hour": 23, "trip_count": 7812}
        ]
    }

    Frontend use: bar chart — peak vs off-peak demand hours.
    """
    try:
        return jsonify({"data": get_hourly_demand()}), 200
    except Exception as e:
        current_app.logger.error(f"[/hourly-demand] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/analytics/revenue-by-zone
# ─────────────────────────────────────────────────────────────────────────────

@analytics_bp.route("/revenue-by-zone", methods=["GET"])
def revenue_by_zone():
    """
    Total and average revenue grouped by pickup zone (all zones, sorted by
    total revenue descending).

    JOIN path: trips → taxi_zones → boroughs (from schema.sql)

    Response (200)
    --------------
    {
        "data": [
            {
                "zone_name":            "Upper East Side North",
                "borough_name":         "Manhattan",
                "trip_count":           45032,
                "total_revenue":        612045.80,
                "avg_revenue_per_trip": 13.59
            },
            ...
        ]
    }

    Frontend use: revenue chart per zone / borough, or map overlay.
    """
    try:
        return jsonify({"data": get_revenue_by_zone()}), 200
    except Exception as e:
        current_app.logger.error(f"[/revenue-by-zone] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/analytics/average-fare-per-mile
# ─────────────────────────────────────────────────────────────────────────────

@analytics_bp.route("/average-fare-per-mile", methods=["GET"])
def average_fare_per_mile():
    """
    Average fare bucketed by trip distance range.
    Buckets: 0-1 mi, 1-3 mi, 3-5 mi, 5-10 mi, 10-20 mi, 20+ mi.

    Uses fare_per_mile and avg_speed_mph — both pre-computed by TM1.
    Bucketing is done inside MySQL with CASE WHEN (no Python loops over rows).

    Response (200)
    --------------
    {
        "data": [
            {
                "distance_bucket":  "0-1 miles",
                "trip_count":       84321,
                "avg_fare":         9.25,
                "avg_fare_per_mile": 11.40,
                "avg_speed_mph":    8.3
            },
            ...
        ]
    }

    Frontend use: grouped bar chart — fare efficiency vs trip length.
    """
    try:
        return jsonify({"data": get_avg_fare_per_distance()}), 200
    except Exception as e:
        current_app.logger.error(f"[/average-fare-per-mile] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/analytics/top-revenue-zones
# ─────────────────────────────────────────────────────────────────────────────

@analytics_bp.route("/top-revenue-zones", methods=["GET"])
def top_revenue_zones():
    """
    Top N pickup zones by total revenue.
    Sorted using TM4's custom merge sort — Python sort() is never called.

    Query Parameters
    ----------------
    n : int  Number of top zones to return. Default 10, max 50.

    Response (200)
    --------------
    {
        "algorithm": "merge_sort",
        "sorted_by": "total_revenue (descending)",
        "data": [
            {
                "zone_name":     "Upper East Side North",
                "borough_name":  "Manhattan",
                "trip_count":    45032,
                "total_revenue": 612045.80
            },
            ...
        ]
    }

    HOW THE ALGORITHM INTEGRATES
    ------------------------------
    analytics_service.get_top_revenue_zones() fetches ALL zone revenue rows
    from MySQL with no ORDER BY clause (raw, unsorted).
    It then passes the list to rank_zones_by_revenue() in custom_algorithm.py,
    which calls the manual merge_sort() function.
    The result is sliced to top_n before being returned here.
    """
    try:
        raw_n = request.args.get("n", "10")
        n = int(raw_n)
        if n < 1 or n > 50:
            return jsonify({"error": "Parameter 'n' must be between 1 and 50."}), 400
    except ValueError:
        return jsonify({"error": "Parameter 'n' must be an integer."}), 400

    try:
        data = get_top_revenue_zones(top_n=n)
        return jsonify({
            "algorithm": "merge_sort",
            "sorted_by": "total_revenue (descending)",
            "data":      data,
        }), 200
    except Exception as e:
        current_app.logger.error(f"[/top-revenue-zones] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500