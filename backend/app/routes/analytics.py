from flask import Blueprint, request, jsonify, current_app
from app.services.analytics_service import (
    get_hourly_demand,
    get_revenue_by_zone,
    get_avg_fare_per_distance,
    get_top_revenue_zones,
    get_top_pickup_hours_manual,
    get_trips_grouped_by_zone_manual,
    get_anomalous_trips,
)

analytics_bp = Blueprint("analytics", __name__)


@analytics_bp.route("/hourly-demand", methods=["GET"])
def hourly_demand():
    try:
        return jsonify({"data": get_hourly_demand()}), 200
    except Exception as e:
        current_app.logger.error(f"[/hourly-demand] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


@analytics_bp.route("/revenue-by-zone", methods=["GET"])
def revenue_by_zone():
    try:
        return jsonify({"data": get_revenue_by_zone()}), 200
    except Exception as e:
        current_app.logger.error(f"[/revenue-by-zone] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


@analytics_bp.route("/average-fare-per-mile", methods=["GET"])
def average_fare_per_mile():
    try:
        return jsonify({"data": get_avg_fare_per_distance()}), 200
    except Exception as e:
        current_app.logger.error(f"[/average-fare-per-mile] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


@analytics_bp.route("/top-revenue-zones", methods=["GET"])
def top_revenue_zones():
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


@analytics_bp.route("/top-pickup-hours", methods=["GET"])
def top_pickup_hours():
    """
    Get top N pickup hours using manual top-k selection algorithm.
    Query parameter: n (default: 5)
    """
    try:
        raw_n = request.args.get("n", "5")
        n = int(raw_n)
        if n < 1 or n > 24:
            return jsonify({"error": "Parameter 'n' must be between 1 and 24."}), 400
    except ValueError:
        return jsonify({"error": "Parameter 'n' must be an integer."}), 400

    try:
        data = get_top_pickup_hours_manual(top_n=n)
        return jsonify({
            "algorithm": "top_k_selection (quickselect)",
            "sorted_by": "trip_count (descending)",
            "data": data,
        }), 200
    except Exception as e:
        current_app.logger.error(f"[/top-pickup-hours] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


@analytics_bp.route("/grouped-by-zone", methods=["GET"])
def grouped_by_zone():
    """
    Group trips by zone using custom hash map implementation.
    Query parameter: limit (default: 1000) - number of trips to process
    """
    try:
        raw_limit = request.args.get("limit", "1000")
        limit = int(raw_limit)
        if limit < 1 or limit > 100000:
            return jsonify({"error": "Parameter 'limit' must be between 1 and 100000."}), 400
    except ValueError:
        return jsonify({"error": "Parameter 'limit' must be an integer."}), 400

    try:
        data = get_trips_grouped_by_zone_manual(limit=limit)
        return jsonify({
            "algorithm": "custom_hash_map",
            "grouped_by": "zone_name",
            "aggregation": "sum(total_amount)",
            "data": data,
        }), 200
    except Exception as e:
        current_app.logger.error(f"[/grouped-by-zone] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500


@analytics_bp.route("/anomalies", methods=["GET"])
def anomalies():
    """
    Detect anomalous trips using manual Z-score calculation.
    Query parameters:
        - field: Field to analyze (default: 'total_amount')
        - threshold: Z-score threshold (default: 3.0)
        - limit: Number of trips to analyze (default: 10000)
    """
    try:
        field = request.args.get("field", "total_amount")
        valid_fields = ['total_amount', 'trip_distance', 'trip_duration_minutes', 
                        'fare_per_mile', 'avg_speed_mph']
        if field not in valid_fields:
            return jsonify({
                "error": f"Field must be one of: {', '.join(valid_fields)}"
            }), 400
        
        raw_threshold = request.args.get("threshold", "3.0")
        threshold = float(raw_threshold)
        if threshold < 0:
            return jsonify({"error": "Threshold must be non-negative."}), 400
        
        raw_limit = request.args.get("limit", "10000")
        limit = int(raw_limit)
        if limit < 1 or limit > 100000:
            return jsonify({"error": "Parameter 'limit' must be between 1 and 100000."}), 400
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter value: {str(e)}"}), 400

    try:
        data = get_anomalous_trips(field=field, threshold=threshold, limit=limit)
        return jsonify({
            "algorithm": "z_score_anomaly_detection",
            "field_analyzed": field,
            "threshold": threshold,
            "anomalies_found": len(data),
            "data": data,
        }), 200
    except Exception as e:
        current_app.logger.error(f"[/anomalies] {e}", exc_info=True)
        return jsonify({"error": "An internal server error occurred."}), 500