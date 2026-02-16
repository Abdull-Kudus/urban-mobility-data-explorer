"""
Adds derived features to the cleaned trip data.

Reads yellow_cleaned_tripdata.csv, computes 5 analytical features,
and writes the result back to the same file.

Features:
  - trip_duration_minutes: (dropoff - pickup) in minutes
  - fare_per_mile: fare_amount / trip_distance
  - pickup_hour: hour of day extracted from pickup time
  - is_weekend: 1 for Sat/Sun, 0 for weekdays
  - avg_speed_mph: trip_distance / duration in hours

Usage: python3 data/feature_engineering.py
"""

import csv
import os
import shutil
from datetime import datetime

BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
CLEANED_FILE = os.path.join(BASE_DIR, "yellow_cleaned_tripdata.csv")
TEMP_FILE    = os.path.join(BASE_DIR, "_temp_enriched.csv")

DERIVED_FIELDS = [
    "trip_duration_minutes",
    "fare_per_mile",
    "pickup_hour",
    "is_weekend",
    "avg_speed_mph",
]


def parse_dt(value):
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return None


def compute_features(row):
    pickup_dt  = parse_dt(row.get("tpep_pickup_datetime", ""))
    dropoff_dt = parse_dt(row.get("tpep_dropoff_datetime", ""))

    features = {f: "" for f in DERIVED_FIELDS}

    if pickup_dt is None or dropoff_dt is None:
        return features

    duration_secs = (dropoff_dt - pickup_dt).total_seconds()
    if duration_secs < 0:
        return features

    try:
        distance = float(row.get("trip_distance", 0))
        fare = float(row.get("fare_amount", 0))
    except (ValueError, TypeError):
        distance, fare = 0.0, 0.0

    duration_mins = round(duration_secs / 60, 2)
    duration_hrs = duration_secs / 3600

    features["trip_duration_minutes"] = duration_mins
    features["pickup_hour"] = pickup_dt.hour
    features["is_weekend"] = 1 if pickup_dt.weekday() >= 5 else 0

    if distance > 0:
        features["fare_per_mile"] = round(fare / distance, 4)

    if duration_hrs > 0 and distance > 0:
        features["avg_speed_mph"] = round(distance / duration_hrs, 4)

    return features


def engineer():
    if not os.path.exists(CLEANED_FILE):
        print(f"ERROR: {CLEANED_FILE} not found. Run clean_tripdata.py first.")
        return

    # read the header to build output fields
    with open(CLEANED_FILE, newline="", encoding="utf-8") as f:
        input_fields = csv.DictReader(f).fieldnames

    output_fields = list(input_fields) + DERIVED_FIELDS

    total = 0
    print(f"Adding derived features to {os.path.basename(CLEANED_FILE)}...")

    with open(CLEANED_FILE, newline="", encoding="utf-8") as infile, \
         open(TEMP_FILE, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=output_fields)
        writer.writeheader()

        for row in reader:
            total += 1
            row.update(compute_features(row))
            writer.writerow(row)

            if total % 500000 == 0:
                print(f"  {total:,} rows processed...")

    # replace the cleaned file with the enriched version
    shutil.move(TEMP_FILE, CLEANED_FILE)

    print(f"\nDone. {total:,} rows enriched with 5 derived features.")
    print(f"Output: {os.path.basename(CLEANED_FILE)}")


if __name__ == "__main__":
    engineer()
