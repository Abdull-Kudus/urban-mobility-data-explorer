"""
db_config.py
------------
Database credentials for CockroachDB (PostgreSQL-compatible cloud database).

HOW TO USE:
  This file is imported by backend/config.py, which passes the values
  to Flask's app.config. The connection is then made in backend/app/db/connection.py.

HOW TO CONNECT TO COCKROACHDB:
  See HOW_TO_CONNECT_COCKROACHDB.md in the project root for a full step-by-step guide.

  Short version:
    1. Install the driver:  pip install psycopg[binary]
    2. Make sure the credentials below are correct.
    3. Run the backend:     cd backend && python run.py
    4. Test the connection: python test_db_connection.py

WHERE TO FIND YOUR CREDENTIALS:
  1. Go to https://cockroachlabs.cloud
  2. Click your cluster (urban-mobility-12676)
  3. Click "Connect" → choose Python → copy the connection string

IMPORTANT: Add db_config.py to your .gitignore — never commit passwords to GitHub.
"""

DB_CONFIG = {
    # ── CockroachDB Cloud connection details ────────────────────────────────
    # Your cluster hostname (from the CockroachDB Cloud dashboard)
    "host":     "urban-mobility-12676.jxf.gcp-europe-west1.cockroachlabs.cloud",

    # CockroachDB always listens on port 26257
    "port":     26257,

    # The database name (defaultdb is the default in CockroachDB)
    "database": "defaultdb",

    # Your CockroachDB username
    "user":     "daniel",

    # Your CockroachDB password
    "password": "UbF0TDq0MNDjGgz9ZNTRDA",

    # sslmode="require" encrypts the connection to CockroachDB Cloud.
    # We use "require" instead of "verify-full" because Windows does not have
    # the CockroachDB root certificate file by default. "require" still encrypts
    # all traffic — it just skips verifying the server's certificate chain.
    # This is fine for development and coursework use.
    "sslmode":  "require",
}
"""
NOTE: The connection.py file (backend/app/db/connection.py) uses psycopg (v3).
Install it with:  pip install psycopg[binary]
"""