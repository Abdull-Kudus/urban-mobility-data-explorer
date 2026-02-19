"""
test_db_connection.py
---------------------
Quick test to check if the CockroachDB connection works.

HOW TO RUN (from the project root folder):
    python test_db_connection.py

If it prints "SUCCESS", the database is connected and ready.
If it prints "FAILED", read the error message and check HOW_TO_CONNECT_COCKROACHDB.md
"""

import sys
import os

# Add the backend folder to Python's search path so we can import from it
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

# We need to create a fake Flask app context because connection.py uses current_app
from flask import Flask
from config import config_map

# Create a minimal Flask app just for this test
app = Flask(__name__)
app.config.from_object(config_map["development"])

# Now we can import and use the connection function
from app.db.connection import get_connection

print("Testing CockroachDB connection...")
print("  Host:     " + app.config["DB_HOST"])
print("  Port:     " + str(app.config["DB_PORT"]))
print("  Database: " + app.config["DB_NAME"])
print("  User:     " + app.config["DB_USER"])
print()

# Run the test inside the Flask app context
with app.app_context():
    try:
        conn = get_connection()
        cur  = conn.cursor()

        # Try a simple query to check the connection works
        cur.execute("SELECT 1 AS test_value")
        row = cur.fetchone()

        if row and row["test_value"] == 1:
            print("[OK] Connection successful! The database is reachable.")
        else:
            print("[WARN] Connected but got an unexpected response.")

        # Try counting trips if the table exists
        try:
            cur.execute("SELECT COUNT(*) AS total FROM trips")
            count_row = cur.fetchone()
            print("[OK] Trips table found! Total rows: " + str(count_row["total"]))
        except Exception:
            print("[INFO] The 'trips' table doesn't exist yet -- run your schema SQL first.")

        cur.close()
        conn.close()

    except Exception as e:
        print("[FAILED] Connection failed: " + str(e))
        print()
        print("Things to check:")
        print("  1. Is psycopg installed?  ->  pip install psycopg[binary]")
        print("  2. Are the credentials in db_config.py correct?")
        print("  3. Is your internet connection working?")
        print("  4. See HOW_TO_CONNECT_COCKROACHDB.md for more help.")
