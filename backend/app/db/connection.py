"""
app/db/connection.py
---------------------
PostgreSQL connection factory using psycopg (v3).

Works with both local PostgreSQL (sslmode=disable) and
CockroachDB Cloud (sslmode=require), controlled by DB_SSLMODE in .env.
"""

import psycopg
from psycopg.rows import dict_row
from flask import current_app


def get_connection():
    """
    Opens and returns a new database connection.
    DB_SSLMODE from .env controls SSL:
      - Local PostgreSQL  →  DB_SSLMODE=disable
      - CockroachDB Cloud →  DB_SSLMODE=require
    """
    return psycopg.connect(
        host        = current_app.config["DB_HOST"],
        port        = current_app.config["DB_PORT"],
        dbname      = current_app.config["DB_NAME"],
        user        = current_app.config["DB_USER"],
        password    = current_app.config["DB_PASSWORD"],
        sslmode     = current_app.config.get("DB_SSLMODE", "disable"),
        row_factory = dict_row,
    )