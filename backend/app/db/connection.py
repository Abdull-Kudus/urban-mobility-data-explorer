"""
app/db/connection.py
---------------------
CockroachDB connection factory using psycopg2.
CockroachDB is PostgreSQL-compatible so psycopg2 works directly.
"""

import psycopg
from psycopg.rows import dict_row
from flask import current_app


def get_connection():
    return psycopg.connect(
        host     = current_app.config["DB_HOST"],
        port     = current_app.config["DB_PORT"],
        dbname   = current_app.config["DB_NAME"],
        user     = current_app.config["DB_USER"],
        password = current_app.config["DB_PASSWORD"],
        sslmode  = "verify-full",
        row_factory = dict_row,

    )