"""
Urban Mobility Data Explorer
clean_data.py — Cleans raw yellow taxi trip data
Run: python3 database/clean_data.py

Input:  data/yellow_tripdata_2019-01.csv  (raw, 1.5M rows)
Output: data/yellow_cleaned_tripdata.csv  (cleaned)
"""

import csv
import os

RAW_FILE     = "data/yellow_tripdata_2019-01.csv"
CLEANED_FILE = "data/yellow_cleaned_tripdata.csv"

# Valid ranges 
VALID_YEAR       = 2019
MAX_DURATION_HRS = 24
MIN_DISTANCE     = 0.0
MAX_DISTANCE     = 200.0
MIN_FARE         = 0.0
MAX_FARE         = 500.0
VALID_VENDORS    = {"1", "2"}
VALID_RATECODES  = {"1", "2", "3", "4", "5", "6"}
VALID_PAYMENTS   = {"1", "2", "3", "4", "5", "6"}

# Exclusion log 
excluded_log = {}

def log(reason):
    excluded_log[reason] = excluded_log.get(reason, 0) + 1

def parse_dt(value):
    from datetime import datetime
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None

def safe_float(value):
    try:
        return float(value)
    except (ValueError, TypeError):
        return None

def safe_int(value):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return None

def clean_row(row):
    """
    Returns cleaned row dict or None if row should be excluded.
    """
    # Parse datetimes 
    pickup_dt  = parse_dt(row.get("tpep_pickup_datetime", ""))
    dropoff_dt = parse_dt(row.get("tpep_dropoff_datetime", ""))

    if pickup_dt is None or dropoff_dt is None:
        log("unparseable datetime")
        return None

    if pickup_dt.year != VALID_YEAR:
        log(f"pickup year not 2019 ({pickup_dt.year})")
        return None

    if dropoff_dt < pickup_dt:
        log("dropoff before pickup")
        return None

    duration_hrs = (dropoff_dt - pickup_dt).total_seconds() / 3600
    if duration_hrs > MAX_DURATION_HRS:
        log("duration > 24 hours")
        return None

    # Parse numeric fields 
    distance = safe_float(row.get("trip_distance", ""))
    fare     = safe_float(row.get("fare_amount", ""))

    if distance is None or fare is None:
        log("missing distance or fare")
        return None

    if not (MIN_DISTANCE <= distance <= MAX_DISTANCE):
        log("distance out of range")
        return None

    if not (MIN_FARE <= fare <= MAX_FARE):
        log("fare out of range")
        return None

    # Passenger count 
    passenger = safe_int(row.get("passenger_count", "1"))
    if passenger is None or passenger < 0:
        log("invalid passenger count")
        return None

    # Validate categorical IDs 
    vendor_id  = str(safe_int(row.get("VendorID", "2")))
    ratecode   = str(safe_int(row.get("RatecodeID", "1")))
    payment    = str(safe_int(row.get("payment_type", "1")))
    pu_loc     = safe_int(row.get("PULocationID", "265"))
    do_loc     = safe_int(row.get("DOLocationID", "265"))

    if vendor_id not in VALID_VENDORS:
        vendor_id = "2"
    if ratecode not in VALID_RATECODES:
        ratecode = "1"
    if payment not in VALID_PAYMENTS:
        payment = "5"
    if pu_loc is None or not (1 <= pu_loc <= 265):
        pu_loc = 265
    if do_loc is None or not (1 <= do_loc <= 265):
        do_loc = 265

    # Normalize numeric fields
    def fn(key, default=0.0):
        v = safe_float(row.get(key, ""))
        return round(v, 2) if v is not None else default

    extra        = fn("extra")
    mta_tax      = fn("mta_tax")
    tip_amount   = fn("tip_amount")
    tolls_amount = fn("tolls_amount")
    improvement  = fn("improvement_surcharge")
    congestion   = fn("congestion_surcharge")
    total        = fn("total_amount")

    flag = row.get("store_and_fwd_flag", "N").strip()
    if flag not in ("Y", "N"):
        flag = "N"

    # Compute derived features 
    duration_mins = round((dropoff_dt - pickup_dt).total_seconds() / 60, 2)
    fare_per_mile = round(fare / distance, 4) if distance > 0 else ""
    pickup_hour   = pickup_dt.hour
    is_weekend    = 1 if pickup_dt.weekday() >= 5 else 0
    duration_h    = duration_mins / 60
    avg_speed     = round(distance / duration_h, 4) if duration_h > 0 and distance > 0 else ""

    return {
        "VendorID":             vendor_id,
        "tpep_pickup_datetime": pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "tpep_dropoff_datetime":dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count":      passenger,
        "trip_distance":        round(distance, 2),
        "RatecodeID":           ratecode,
        "store_and_fwd_flag":   flag,
        "PULocationID":         pu_loc,
        "DOLocationID":         do_loc,
        "payment_type":         payment,
        "fare_amount":          round(fare, 2),
        "extra":                extra,
        "mta_tax":              mta_tax,
        "tip_amount":           tip_amount,
        "tolls_amount":         tolls_amount,
        "improvement_surcharge":improvement,
        "congestion_surcharge": congestion,
        "total_amount":         total,
        "trip_duration_minutes":duration_mins,
        "fare_per_mile":        fare_per_mile,
        "pickup_hour":          pickup_hour,
        "is_weekend":           is_weekend,
        "avg_speed_mph":        avg_speed,
    }

def clean():
    if not os.path.exists(RAW_FILE):
        print(f"ERROR: {RAW_FILE} not found.")
        print("Place the raw file in the data/ folder and try again.")
        return

    print(f"Reading {RAW_FILE} ...")

    total    = 0
    cleaned  = 0
    excluded = 0

    FIELDNAMES = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "congestion_surcharge", "total_amount",
        "trip_duration_minutes", "fare_per_mile",
        "pickup_hour", "is_weekend", "avg_speed_mph",
    ]

    with open(RAW_FILE, newline="", encoding="utf-8") as infile, \
         open(CLEANED_FILE, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        for row in reader:
            total += 1
            result = clean_row(row)

            if result:
                writer.writerow(result)
                cleaned += 1
            else:
                excluded += 1

            if total % 100000 == 0:
                print(f"  Processed {total:,} rows — kept {cleaned:,} ...")

    print("\n Cleaning completed")
    print(f"  Total rows    : {total:,}")
    print(f"  Rows kept     : {cleaned:,}")
    print(f"  Rows excluded : {excluded:,}")
    print(f"\n Exclusion reasons")
    for reason, count in sorted(excluded_log.items(), key=lambda x: -x[1]):
        print(f"  {reason:<35} {count:,}")
    print(f"\n  Saved to: {CLEANED_FILE}")

if __name__ == "__main__":
    clean()