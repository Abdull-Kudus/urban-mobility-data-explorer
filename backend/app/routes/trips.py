"""
app/routes/trips.py
--------------------
Blueprint for GET /api/trips.

A route's ONLY jobs:
  1. Parse and validate query-string parameters.
  2. Call the service layer with clean, typed values.
  3. Return a structured JSON response or a clear error.

No SQL lives here. No algorithm logic lives here.
"""

from flask import Blueprint, request, jsonify, current_app
from app.services.trip_service import get_filtered_trips

trips_bp = Blueprint("trips", __name__)


# ─────────────────────────────────────────────────────────────────────────────
# Parameter parsing helpers
# ─────────────────────────────────────────────────────────────────────────────

def _parse_float(value: str | None, name: str) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"Parameter '{name}' must be a number. Got: '{value}'")


def _parse_int(value: str | None, name: str, default: int) -> int:
    if value is None:
        return default
    try:
        result = int(value)
        if result < 1:
            raise ValueError()
        return result
    except ValueError:
        raise ValueError(f"Parameter '{name}' must be a positive integer. Got: '{value}'")


# ─────────────────────────────────────────────────────────────────────────────
# GET /api/trips
# ─────────────────────────────────────────────────────────────────────────────

@trips_bp.route("/trips", methods=["GET"])
def list_trips():
    """
    Paginated, filtered trip list.

    Query Parameters
    ----------------
    start_date    : str   ISO datetime  e.g. 2019-01-01 00:00:00
    end_date      : str   ISO datetime  e.g. 2019-01-31 23:59:59
    pickup_zone   : str   partial zone name  e.g. "JFK" or "Midtown"
    dropoff_zone  : str   partial zone name
    min_fare      : float minimum fare_amount  e.g. 5.0
    max_fare      : float maximum fare_amount  e.g. 100.0
    min_distance  : float minimum trip_distance  e.g. 1.0
    page          : int   page number, 1-indexed  (default 1)
    limit         : int   rows per page           (default 50, max 500)

    Success Response — 200
    -----------------------
    {
        "data": [
            {
                "trip_id":               12345,
                "pickup_datetime":       "2019-01-03 14:22:10",
                "dropoff_datetime":      "2019-01-03 14:55:40",
                "trip_distance":         8.3,
                "fare_amount":           28.50,
                "total_amount":          34.75,
                "tip_amount":            5.00,
                "passenger_count":       1,
                "fare_per_mile":         3.43,
                "trip_duration_minutes": 33.5,
                "avg_speed_mph":         14.87,
                "pickup_hour":           14,
                "is_weekend":            0,
                "pickup_zone":           "JFK Airport",
                "dropoff_zone":          "Upper East Side North",
                "pickup_borough":        "Queens",
                "dropoff_borough":       "Manhattan"
            }
        ],
        "page":        1,
        "limit":       50,
        "total_count": 3842,
        "total_pages": 77
    }

    Error Responses
    ---------------
    400 — invalid parameter value
    500 — database or server error
    """
    try:
        # String parameters (passed through safely to parameterized SQL)
        start_date   = request.args.get("start_date")
        end_date     = request.args.get("end_date")
        pickup_zone  = request.args.get("pickup_zone")
        dropoff_zone = request.args.get("dropoff_zone")

        # Numeric parameters
        min_fare     = _parse_float(request.args.get("min_fare"),     "min_fare")
        max_fare     = _parse_float(request.args.get("max_fare"),     "max_fare")
        min_distance = _parse_float(request.args.get("min_distance"), "min_distance")

        # Pagination
        default_limit = current_app.config["DEFAULT_PAGE_SIZE"]
        max_limit     = current_app.config["MAX_PAGE_SIZE"]
        page          = _parse_int(request.args.get("page"),  "page",  default=1)
        limit         = _parse_int(request.args.get("limit"), "limit", default=default_limit)
        limit         = min(limit, max_limit)   # cap — prevents oversized payloads

        # Cross-field validation
        if min_fare is not None and max_fare is not None and min_fare > max_fare:
            return jsonify({"error": "min_fare cannot be greater than max_fare"}), 400

    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # Delegate all data logic to the service layer
    try:
        result = get_filtered_trips(
            start_date   = start_date,
            end_date     = end_date,
            pickup_zone  = pickup_zone,
            dropoff_zone = dropoff_zone,
            min_fare     = min_fare,
            max_fare     = max_fare,
            min_distance = min_distance,
            page         = page,
            limit        = limit,
        )
        return jsonify(result), 200

    except Exception as e:
        current_app.logger.error(f"[/api/trips] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500