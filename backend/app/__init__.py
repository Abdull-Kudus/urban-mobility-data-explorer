"""
app/__init__.py
---------------
Application factory.

Flask's "application factory" pattern means the app object is created inside
a function, not at module level. This prevents circular imports and allows
different configurations (dev vs production) to be injected cleanly.
"""

import sys
import os

# Ensure backend/ folder is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify
from flask_cors import CORS

from config import config_map
from app.routes.trips     import trips_bp
from app.routes.analytics import analytics_bp


def create_app(env: str = "development") -> Flask:
    """Create, configure, and return the Flask application."""

    app = Flask(__name__)
    app.config.from_object(config_map.get(env, config_map["development"]))

    # Allow the frontend (Ariane's HTML/JS served on a different port) to
    # call the API without browser CORS errors.
    CORS(app, resources={r"/api/*": {"origins": "*"}})

    # ------------------------------------------------------------------ #
    # Blueprints                                                           #
    # trips_bp     → /api/trips                                           #
    # analytics_bp → /api/analytics/*                                     #
    # ------------------------------------------------------------------ #
    app.register_blueprint(trips_bp,     url_prefix="/api")
    app.register_blueprint(analytics_bp, url_prefix="/api/analytics")

    # ------------------------------------------------------------------ #
    # Global HTTP error handlers                                           #
    # ------------------------------------------------------------------ #
    @app.errorhandler(404)
    def not_found(e):
        return jsonify({"error": "Endpoint not found"}), 404

    @app.errorhandler(405)
    def method_not_allowed(e):
        return jsonify({"error": "Method not allowed on this endpoint"}), 405

    return app