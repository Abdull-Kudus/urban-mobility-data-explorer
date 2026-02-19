"""
load_trips.py
-------------
Bulk-loads the cleaned trip data into the local PostgreSQL 'trips' table.

HOW TO RUN (from the project root):
    python load_trips.py

Prerequisites:
  1. Run `python run_schema.py` first to create the tables.
  2. Run `python data/data_pipeline.py` to produce the cleaned CSV.
  3. Make sure your .env DB credentials are correct.

If a previous load was interrupted, that's fine — the script reports
how many rows already exist and skips the load so you don't get duplicates.
To reload from scratch, truncate the trips table first in psql:
    TRUNCATE trips RESTART IDENTITY;
"""

import csv
import io
import os
import sys
import time

# ── Flask app context (needed by get_connection) ──────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from flask import Flask
from config import config_map
from app.db.connection import get_connection

app = Flask(__name__)
app.config.from_object(config_map["development"])

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CLEANED_FILE = os.path.join(BASE_DIR, "data", "yellow_cleaned_tripdata.csv")

# How many rows to INSERT at a time (only used for COPY fallback)
BATCH_SIZE = 5_000


def count_existing(conn):
    """Return how many trip rows are already in the table."""
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS n FROM trips")
    row = cur.fetchone()
    cur.close()
    return int(row["n"]) if row else 0


def load_via_copy(conn, csv_path):
    """
    Ultra-fast bulk load using PostgreSQL COPY FROM STDIN.
    Reads the cleaned CSV and pipes each row directly into the database.
    This is 10-50x faster than individual INSERTs.
    """
    print("  Using PostgreSQL COPY for fast bulk load...")

    # We map the CSV columns to the trips table columns (excluding trip_id which is SERIAL)
    COPY_COLUMNS = (
        "vendor_id, ratecode_id, payment_type_id, "
        "pickup_location_id, dropoff_location_id, "
        "pickup_datetime, dropoff_datetime, "
        "passenger_count, trip_distance, store_and_fwd_flag, "
        "fare_amount, extra, mta_tax, tip_amount, tolls_amount, "
        "improvement_surcharge, congestion_surcharge, total_amount, "
        "trip_duration_minutes, fare_per_mile, pickup_hour, is_weekend, avg_speed_mph"
    )

    cur = conn.cursor()
    total = 0
    start = time.time()

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # COPY expects a tab-separated stream
        with cur.copy(f"COPY trips ({COPY_COLUMNS}) FROM STDIN (FORMAT TEXT, NULL '')") as copy:
            for row in reader:
                total += 1

                def val(key, default=""):
                    v = row.get(key, default)
                    return "" if v == "" or v is None else v

                # Map CSV column names → DB column order; empty string → NULL
                line_parts = [
                    val("VendorID"),
                    val("RatecodeID"),
                    val("payment_type"),
                    val("PULocationID"),
                    val("DOLocationID"),
                    val("tpep_pickup_datetime"),
                    val("tpep_dropoff_datetime"),
                    val("passenger_count", "1"),
                    val("trip_distance", "0"),
                    val("store_and_fwd_flag", "N"),
                    val("fare_amount", "0"),
                    val("extra", "0"),
                    val("mta_tax", "0"),
                    val("tip_amount", "0"),
                    val("tolls_amount", "0"),
                    val("improvement_surcharge", "0"),
                    val("congestion_surcharge", "0"),
                    val("total_amount", "0"),
                    val("trip_duration_minutes"),
                    val("fare_per_mile"),
                    val("pickup_hour"),
                    val("is_weekend"),
                    val("avg_speed_mph"),
                ]

                copy.write("\t".join(line_parts) + "\n")

                if total % 100_000 == 0:
                    elapsed = time.time() - start
                    rate = total / elapsed
                    print(f"  {total:>10,} rows  |  {rate:,.0f} rows/sec")

    conn.commit()
    cur.close()

    elapsed = time.time() - start
    print(f"\n  Done via COPY: {total:,} rows in {int(elapsed // 60)}m {elapsed % 60:.1f}s")
    return total


def load_via_insert(conn, csv_path):
    """
    Fallback batch INSERT method (slower, but works on any PostgreSQL setup).
    Used if COPY fails for any reason.
    """
    print("  Using batched INSERT (fallback)...")

    INSERT_SQL = """
        INSERT INTO trips (
            vendor_id, ratecode_id, payment_type_id,
            pickup_location_id, dropoff_location_id,
            pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, store_and_fwd_flag,
            fare_amount, extra, mta_tax, tip_amount, tolls_amount,
            improvement_surcharge, congestion_surcharge, total_amount,
            trip_duration_minutes, fare_per_mile, pickup_hour, is_weekend, avg_speed_mph
        ) VALUES (
            %(vendor_id)s, %(ratecode_id)s, %(payment_type_id)s,
            %(pickup_location_id)s, %(dropoff_location_id)s,
            %(pickup_datetime)s, %(dropoff_datetime)s,
            %(passenger_count)s, %(trip_distance)s, %(store_and_fwd_flag)s,
            %(fare_amount)s, %(extra)s, %(mta_tax)s, %(tip_amount)s, %(tolls_amount)s,
            %(improvement_surcharge)s, %(congestion_surcharge)s, %(total_amount)s,
            %(trip_duration_minutes)s, %(fare_per_mile)s, %(pickup_hour)s, %(is_weekend)s, %(avg_speed_mph)s
        )
    """

    def to_none(v):
        return None if v == "" else v

    cur = conn.cursor()
    batch = []
    total = 0
    start = time.time()

    with open(csv_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            total += 1
            batch.append({
                "vendor_id":              to_none(row.get("VendorID")),
                "ratecode_id":            to_none(row.get("RatecodeID")),
                "payment_type_id":        to_none(row.get("payment_type")),
                "pickup_location_id":     to_none(row.get("PULocationID")),
                "dropoff_location_id":    to_none(row.get("DOLocationID")),
                "pickup_datetime":        to_none(row.get("tpep_pickup_datetime")),
                "dropoff_datetime":       to_none(row.get("tpep_dropoff_datetime")),
                "passenger_count":        to_none(row.get("passenger_count")) or 1,
                "trip_distance":          to_none(row.get("trip_distance")) or 0,
                "store_and_fwd_flag":     row.get("store_and_fwd_flag") or "N",
                "fare_amount":            to_none(row.get("fare_amount")) or 0,
                "extra":                  to_none(row.get("extra")) or 0,
                "mta_tax":               to_none(row.get("mta_tax")) or 0,
                "tip_amount":             to_none(row.get("tip_amount")) or 0,
                "tolls_amount":           to_none(row.get("tolls_amount")) or 0,
                "improvement_surcharge":  to_none(row.get("improvement_surcharge")) or 0,
                "congestion_surcharge":   to_none(row.get("congestion_surcharge")) or 0,
                "total_amount":           to_none(row.get("total_amount")) or 0,
                "trip_duration_minutes":  to_none(row.get("trip_duration_minutes")),
                "fare_per_mile":          to_none(row.get("fare_per_mile")),
                "pickup_hour":            to_none(row.get("pickup_hour")),
                "is_weekend":             to_none(row.get("is_weekend")),
                "avg_speed_mph":          to_none(row.get("avg_speed_mph")),
            })

            if len(batch) >= BATCH_SIZE:
                cur.executemany(INSERT_SQL, batch)
                conn.commit()
                batch.clear()
                elapsed = time.time() - start
                rate = total / elapsed
                print(f"  {total:>10,} rows  |  {rate:,.0f} rows/sec")

    # flush remaining
    if batch:
        cur.executemany(INSERT_SQL, batch)
        conn.commit()

    cur.close()
    elapsed = time.time() - start
    print(f"\n  Done via INSERT: {total:,} rows in {int(elapsed // 60)}m {elapsed % 60:.1f}s")
    return total


def run():
    print("=" * 55)
    print("  Trip Data Loader")
    print("=" * 55)

    # ── Prerequisite check ────────────────────────────────────────────────────
    if not os.path.exists(CLEANED_FILE):
        print(f"\n[ERROR] Cleaned data file not found:\n  {CLEANED_FILE}")
        print("\nRun the pipeline first:")
        print("  python data/data_pipeline.py")
        return

    size_mb = os.path.getsize(CLEANED_FILE) / (1024 * 1024)
    print(f"\nFile: {os.path.basename(CLEANED_FILE)} ({size_mb:.1f} MB)")

    # ── Connect ───────────────────────────────────────────────────────────────
    print(f"\nConnecting to PostgreSQL at {app.config['DB_HOST']}:{app.config['DB_PORT']} ...")
    with app.app_context():
        conn = get_connection()

        existing = count_existing(conn)
        if existing > 0:
            print(f"\n[INFO] The trips table already has {existing:,} rows.")
            print("  If you want to reload, run this in psql first:")
            print("    TRUNCATE trips RESTART IDENTITY;")
            print("  Then re-run this script.")
            conn.close()
            return

        print("\nLoading trips...")
        try:
            total = load_via_copy(conn, CLEANED_FILE)
        except Exception as e:
            print(f"\n  COPY failed ({e}), falling back to INSERT...")
            conn.rollback()
            total = load_via_insert(conn, CLEANED_FILE)

        # ── Final verification ────────────────────────────────────────────
        verify_count = count_existing(conn)
        conn.close()

    print(f"\n{'=' * 55}")
    print(f"  Load complete!")
    print(f"  Rows inserted: {total:,}")
    print(f"  Rows in DB:    {verify_count:,}")
    print(f"{'=' * 55}")
    print("\nNext: start the backend and open the frontend dashboard.")
    print("  cd backend && python run.py")


if __name__ == "__main__":
    run()
