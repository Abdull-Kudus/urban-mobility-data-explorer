"""
End-to-end data processing pipeline.

Stage 1 (clean_tripdata): validates, deduplicates, removes outliers,
    enriches trips with zone metadata from lookup CSV and GeoJSON.
Stage 2 (feature_engineering): adds 5 derived features to the cleaned data.

Final output: data/yellow_cleaned_tripdata.csv (ready for DB insertion)

Usage: python3 data/data_pipeline.py
"""

import os
import sys
import time
import json
import csv
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_FILE         = os.path.join(BASE_DIR, "yellow_tripdata_2019-01.csv")
ZONE_LOOKUP_FILE = os.path.join(BASE_DIR, "taxi_zone_lookup.csv")
GEOJSON_FILE     = os.path.join(BASE_DIR, "taxi_zones.geojson")
CLEANED_FILE     = os.path.join(BASE_DIR, "yellow_cleaned_tripdata.csv")
EXCLUSION_LOG    = os.path.join(BASE_DIR, "exclusion_log.json")


def check_prerequisites():
    print("\nChecking input files...")
    all_ok = True
    for label, path in [
        ("Raw trip data", RAW_FILE),
        ("Zone lookup",   ZONE_LOOKUP_FILE),
        ("Zones GeoJSON", GEOJSON_FILE),
    ]:
        exists = os.path.exists(path)
        size = ""
        if exists:
            mb = os.path.getsize(path) / (1024 * 1024)
            size = f" ({mb:.1f} MB)" if mb > 1 else f" ({os.path.getsize(path)} bytes)"
        status = "OK" if exists else "MISSING"
        print(f"  [{status}] {label}: {os.path.basename(path)}{size}")
        if not exists:
            all_ok = False
    return all_ok


def run_stage(stage_num, name, module_name):
    print(f"\n{'=' * 50}")
    print(f"  Stage {stage_num}: {name}")
    print(f"{'=' * 50}")

    start = time.time()
    sys.path.insert(0, BASE_DIR)

    try:
        mod = __import__(module_name)
        if hasattr(mod, "clean"):
            mod.clean()
        elif hasattr(mod, "engineer"):
            mod.engineer()
        else:
            print(f"  ERROR: no entry point in {module_name}")
            return False
    except Exception as e:
        print(f"  ERROR in {module_name}: {e}")
        import traceback
        traceback.print_exc()
        return False

    elapsed = time.time() - start
    print(f"\n  Stage {stage_num} done in {int(elapsed // 60)}m {elapsed % 60:.1f}s")
    return True


def validate():
    print(f"\n{'=' * 50}")
    print(f"  Validation")
    print(f"{'=' * 50}")

    issues = []

    if not os.path.exists(CLEANED_FILE):
        issues.append("Cleaned dataset not found")
    else:
        with open(CLEANED_FILE, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            first_row = next(reader, None)

        expected = {
            "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
            "passenger_count", "trip_distance", "RatecodeID",
            "store_and_fwd_flag", "PULocationID", "DOLocationID",
            "payment_type", "fare_amount", "extra", "mta_tax",
            "tip_amount", "tolls_amount", "improvement_surcharge",
            "congestion_surcharge", "total_amount",
            "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
            "trip_duration_minutes", "fare_per_mile", "pickup_hour",
            "is_weekend", "avg_speed_mph",
        }
        missing = expected - set(header)
        if missing:
            issues.append(f"Missing columns: {missing}")
        else:
            print(f"  [OK] All {len(expected)} columns present")

        if first_row:
            row = dict(zip(header, first_row))

            # spot-check derived features
            pickup  = datetime.strptime(row["tpep_pickup_datetime"], "%Y-%m-%d %H:%M:%S")
            dropoff = datetime.strptime(row["tpep_dropoff_datetime"], "%Y-%m-%d %H:%M:%S")

            expected_dur = round((dropoff - pickup).total_seconds() / 60, 2)
            actual_dur = float(row["trip_duration_minutes"])
            if abs(expected_dur - actual_dur) < 0.1:
                print(f"  [OK] trip_duration_minutes: {actual_dur}")
            else:
                issues.append(f"duration mismatch: {expected_dur} vs {actual_dur}")

            if pickup.hour == int(row["pickup_hour"]):
                print(f"  [OK] pickup_hour: {row['pickup_hour']}")

            expected_wknd = 1 if pickup.weekday() >= 5 else 0
            if expected_wknd == int(row["is_weekend"]):
                print(f"  [OK] is_weekend: {row['is_weekend']}")
        else:
            issues.append("No data rows in output")

    if os.path.exists(EXCLUSION_LOG):
        with open(EXCLUSION_LOG) as f:
            log = json.load(f)
        print(f"  [OK] Exclusion log: {log['rows_excluded']:,} excluded ({log['exclusion_rate_pct']}%)")
    else:
        issues.append("Exclusion log not found")

    if issues:
        print("\n  Issues:")
        for i in issues:
            print(f"    - {i}")
        return False

    print("\n  All checks passed.")
    return True


def pipeline():
    start = time.time()
    print("=" * 50)
    print(f"  Data Pipeline - {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)

    if not check_prerequisites():
        print("\nAborted: missing input files.")
        return

    if not run_stage(1, "Cleaning & Integration", "clean_tripdata"):
        print("\nAborted: cleaning failed.")
        return

    if not run_stage(2, "Feature Engineering", "feature_engineering"):
        print("\nAborted: feature engineering failed.")
        return

    validate()

    elapsed = time.time() - start
    print(f"\n{'=' * 50}")
    print(f"  Pipeline complete ({int(elapsed // 60)}m {elapsed % 60:.1f}s)")
    print(f"{'=' * 50}")

    if os.path.exists(CLEANED_FILE):
        mb = os.path.getsize(CLEANED_FILE) / (1024 * 1024)
        print(f"\n  Output: {os.path.basename(CLEANED_FILE)} ({mb:.1f} MB)")
    print()


if __name__ == "__main__":
    pipeline()
