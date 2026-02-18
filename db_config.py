"""
db_config.py
-------------
Database credentials for MySQL.
This file already exists in the project (db_config_cpython-38.pyc is present).
Place this file at the project root (same level as backend/ and database/).

IMPORTANT: Add db_config.py to your .gitignore — never commit credentials.
"""

DB_CONFIG = {
    "host":     "localhost",
    "port":     3306,
    "database": "urban_mobility",
    "user":     "daniel",
    "password": "UbF0TDq0MNDjGgz9ZNTRDA",
    "sslmode": "postgresql://daniel:UbF0TDq0MNDjGgz9ZNTRDA@urban-mobility-12676.jxf.gcp-europe-west1.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full",
}