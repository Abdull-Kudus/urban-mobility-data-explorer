import csv
import psycopg
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

load_dotenv()

# File Path and Validation Parameters
TRIP_DATA_FILE = "data/yellow_cleaned_tripdata.csv"
MIN_DISTANCE    = 0.0          
MAX_DISTANCE    = 200.0
MIN_FARE        = 0.0         
MAX_FARE        = 500.0       
MAX_DURATION_H  = 24           
VALID_YEAR      = 2019         

# Helper Functions
def parse_dt(value):
    """Parse datetime string to Python datetime object."""
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def safe_float(value, default=0.0):
    """Convert string to float, return default on failure."""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=0):
    """Convert string to int, return default on failure."""
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def compute_derived_features(pickup_dt, dropoff_dt, distance, fare):
    """Calculate derived features from raw trip data."""
    features = {
        "trip_duration_minutes": None,
        "fare_per_mile":         None,
        "pickup_hour":           None,
        "is_weekend":            None,
        "avg_speed_mph":         None,
    }

    if pickup_dt is None or dropoff_dt is None:
        return features

    duration_seconds = (dropoff_dt - pickup_dt).total_seconds()
    if duration_seconds < 0:
        return features   
    
    duration_minutes = round(duration_seconds / 60, 2)
    features["trip_duration_minutes"] = duration_minutes

    if distance > 0:
        features["fare_per_mile"] = round(fare / distance, 4)

    features["pickup_hour"] = pickup_dt.hour
    features["is_weekend"] = 1 if pickup_dt.weekday() >= 5 else 0

    duration_hours = duration_seconds / 3600
    if duration_hours > 0 and distance > 0:
        features["avg_speed_mph"] = round(distance / duration_hours, 4)

    return features


def is_valid_row(row, pickup_dt, dropoff_dt, distance, fare):
    """Validate row against quality rules. Returns True if valid."""
    reason = None

    if pickup_dt is None or dropoff_dt is None:
        reason = "unparseable datetime"
    elif pickup_dt.year != VALID_YEAR:
        reason = f"pickup year {pickup_dt.year} out of range"
    elif dropoff_dt < pickup_dt:
        reason = "dropoff before pickup"
    elif (dropoff_dt - pickup_dt).total_seconds() > MAX_DURATION_H * 3600:
        reason = "trip duration > 24 hours"
    elif not (MIN_DISTANCE <= distance <= MAX_DISTANCE):
        reason = f"distance {distance} out of range"
    elif fare < MIN_FARE or fare > MAX_FARE:
        reason = f"fare {fare} out of range"
    elif safe_int(row.get("passenger_count", 1)) < 0:
        reason = "negative passenger count"

    if reason:
        print(f"  [EXCLUDED] {reason} → row: {dict(list(row.items())[:4])}")
        return False
    return True


# Main Insertion Function
def insert_trips():
    """Read CSV and insert cleaned trips into database."""
    conn = psycopg.connect(
        host     = os.getenv("DB_HOST"),
        port     = int(os.getenv("DB_PORT", 26257)),
        dbname   = os.getenv("DB_NAME"),
        user     = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD"),
        sslmode  = os.getenv("DB_SSLMODE", "verify-full"),
    )

    inserted  = 0
    excluded  = 0
    batch     = []
    BATCH_SIZE = 500

    sql = """
        INSERT INTO trips (
            vendor_id, ratecode_id, payment_type_id,
            pickup_location_id, dropoff_location_id,
            pickup_datetime, dropoff_datetime,
            passenger_count, trip_distance, store_and_fwd_flag,
            fare_amount, extra, mta_tax, tip_amount,
            tolls_amount, improvement_surcharge,
            congestion_surcharge, total_amount,
            trip_duration_minutes, fare_per_mile,
            pickup_hour, is_weekend, avg_speed_mph
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        )
        ON CONFLICT DO NOTHING
    """

    print(f"Opening {TRIP_DATA_FILE} ...")
    with open(TRIP_DATA_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            pickup_dt  = parse_dt(row.get("tpep_pickup_datetime", ""))
            dropoff_dt = parse_dt(row.get("tpep_dropoff_datetime", ""))
            distance   = safe_float(row.get("trip_distance", "0"))
            fare       = safe_float(row.get("fare_amount", "0"))

            vendor_id   = safe_int(row.get("VendorID",     1))
            ratecode_id = safe_int(row.get("RatecodeID",   1))
            payment_id  = safe_int(row.get("payment_type", 1))
            pu_loc      = safe_int(row.get("PULocationID", 265))
            do_loc      = safe_int(row.get("DOLocationID", 265))

            if not is_valid_row(row, pickup_dt, dropoff_dt, distance, fare):
                excluded += 1
                continue

            # Enforce valid ranges for foreign keys
            vendor_id   = vendor_id   if vendor_id   in (1, 2)         else 2
            ratecode_id = ratecode_id if ratecode_id  in range(1, 7)    else 1
            payment_id  = payment_id  if payment_id   in range(1, 7)    else 5
            pu_loc      = pu_loc      if 1 <= pu_loc  <= 265            else 265
            do_loc      = do_loc      if 1 <= do_loc  <= 265            else 265

            features = compute_derived_features(pickup_dt, dropoff_dt, distance, fare)

            record = (
                vendor_id,
                ratecode_id,
                payment_id,
                pu_loc,
                do_loc,
                pickup_dt,
                dropoff_dt,
                max(safe_int(row.get("passenger_count", "1")), 0),
                distance,
                row.get("store_and_fwd_flag", "N").strip() or "N",
                fare,
                safe_float(row.get("extra", "0")),
                safe_float(row.get("mta_tax", "0")),
                safe_float(row.get("tip_amount", "0")),
                safe_float(row.get("tolls_amount", "0")),
                safe_float(row.get("improvement_surcharge", "0")),
                safe_float(row.get("congestion_surcharge", "0")),
                safe_float(row.get("total_amount", "0")),
                features["trip_duration_minutes"],
                features["fare_per_mile"],
                features["pickup_hour"],
                features["is_weekend"],
                features["avg_speed_mph"],
            )

            batch.append(record)

            if len(batch) >= BATCH_SIZE:
                with conn.cursor() as cur:
                    cur.executemany(sql, batch)
                conn.commit()
                inserted += len(batch)
                print(f"  Inserted {inserted} rows so far ...")
                batch = []

    # Insert remaining rows
    if batch:
        with conn.cursor() as cur:
            cur.executemany(sql, batch)
        conn.commit()
        inserted += len(batch)

    conn.close()

    print("\nInsertion complete")
    print(f"  Rows inserted : {inserted}")
    print(f"  Rows excluded : {excluded}")
    print(f"  Total processed: {inserted + excluded}")


if __name__ == "__main__":
    insert_trips()