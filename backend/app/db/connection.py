"""
app/db/connection.py
---------------------
CockroachDB connection factory using psycopg (v3).
CockroachDB is PostgreSQL-compatible so psycopg works directly.

SSL NOTE:
  We use sslmode="require" which encrypts all traffic to CockroachDB Cloud.
  We do NOT use "verify-full" because that requires a local root.crt certificate
  file which is not present on Windows by default.
  For a production deployment, download the cert from CockroachDB Cloud dashboard
  and switch back to sslmode="verify-full" with sslrootcert="path/to/root.crt".
"""

import psycopg
from psycopg.rows import dict_row
from flask import current_app


def get_connection():
    """
    Opens and returns a new database connection to CockroachDB.
    Called by every service function that needs to query the database.
    """
    return psycopg.connect(
        host        = current_app.config["DB_HOST"],
        port        = current_app.config["DB_PORT"],
        dbname      = current_app.config["DB_NAME"],
        user        = current_app.config["DB_USER"],
        password    = current_app.config["DB_PASSWORD"],
        # "require" encrypts the connection without needing a local certificate file
        sslmode     = "require",
        row_factory = dict_row,
    )