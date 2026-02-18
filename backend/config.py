"""
config.py
---------
Flask application configuration.

Reads database credentials from db_config.py (already used by insert_tripdata.py),
keeping credentials in one place across the whole project.
"""

import sys
import os

# Allow import of db_config.py from the project root
# Project root is one level above backend/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from db_config import DB_CONFIG


class Config:
    # ------------------------------------------------------------------ #
    # Database — pulled directly from the shared db_config.py            #
    # ------------------------------------------------------------------ #
    DB_HOST     = DB_CONFIG["host"]
    DB_PORT     = DB_CONFIG.get("port", 3306)
    DB_NAME     = DB_CONFIG["database"]
    DB_USER     = DB_CONFIG["user"]
    DB_PASSWORD = DB_CONFIG["password"]

    # ------------------------------------------------------------------ #
    # Flask                                                                #
    # ------------------------------------------------------------------ #
    DEBUG   = False
    TESTING = False

    # ------------------------------------------------------------------ #
    # Pagination                                                           #
    # ------------------------------------------------------------------ #
    DEFAULT_PAGE_SIZE = 50
    MAX_PAGE_SIZE     = 500


class DevelopmentConfig(Config):
    DEBUG = True


class ProductionConfig(Config):
    DEBUG = False


config_map = {
    "development": DevelopmentConfig,
    "production":  ProductionConfig,
}