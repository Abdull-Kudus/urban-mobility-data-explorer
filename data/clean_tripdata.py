"""
Cleans raw yellow taxi trip data and associates it with zone metadata.

Reads the raw CSV, validates each row, removes duplicates and outliers,
enriches with borough/zone info from taxi_zone_lookup.csv and taxi_zones.geojson,
then writes the cleaned output. Also saves an exclusion log as JSON.

Usage: python3 data/clean_tripdata.py
"""

import csv
import json
import os
from datetime import datetime

BASE_DIR         = os.path.dirname(os.path.abspath(__file__))
RAW_FILE         = os.path.join(BASE_DIR, "yellow_tripdata_2019-01.csv")
ZONE_LOOKUP_FILE = os.path.join(BASE_DIR, "taxi_zone_lookup.csv")
GEOJSON_FILE     = os.path.join(BASE_DIR, "taxi_zones.geojson")
CLEANED_FILE     = os.path.join(BASE_DIR, "yellow_cleaned_tripdata.csv")
EXCLUSION_LOG    = os.path.join(BASE_DIR, "exclusion_log.json")

VALID_YEAR       = 2019
MAX_DURATION_HRS = 24
MIN_DISTANCE, MAX_DISTANCE = 0.0, 200.0
MIN_FARE, MAX_FARE         = 0.0, 500.0
MAX_SPEED_MPH    = 200
VALID_VENDORS    = {"1", "2"}
VALID_RATECODES  = {"1", "2", "3", "4", "5", "6"}
VALID_PAYMENTS   = {"1", "2", "3", "4", "5", "6"}

excluded_log = {}


def log_exclusion(reason):
    excluded_log[reason] = excluded_log.get(reason, 0) + 1


def parse_dt(value):
    try:
        return datetime.strptime(value.strip(), "%Y-%m-%d %H:%M:%S")
    except (ValueError, AttributeError):
        return None


def safe_float(value, default=None):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def safe_int(value, default=None):
    try:
        return int(float(value))
    except (ValueError, TypeError):
        return default


def load_zone_lookup():
    zones = {}
    if not os.path.exists(ZONE_LOOKUP_FILE):
        print(f"  WARNING: {ZONE_LOOKUP_FILE} not found, skipping zone enrichment")
        return zones

    with open(ZONE_LOOKUP_FILE, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            loc_id = safe_int(row.get("LocationID"))
            if loc_id is not None:
                zones[loc_id] = {
                    "borough":      row.get("Borough", "Unknown").strip(),
                    "zone":         row.get("Zone", "Unknown").strip(),
                    "service_zone": row.get("service_zone", "Unknown").strip(),
                }
    print(f"  Loaded {len(zones)} zones from lookup")
    return zones


def load_geojson_zones():
    if not os.path.exists(GEOJSON_FILE):
        print(f"  WARNING: {GEOJSON_FILE} not found, skipping spatial validation")
        return set()

    with open(GEOJSON_FILE, encoding="utf-8") as f:
        data = json.load(f)

    ids = set()
    for feature in data.get("features", []):
        loc_id = feature.get("properties", {}).get("LocationID")
        if loc_id is not None:
            ids.add(int(loc_id))

    print(f"  Loaded {len(ids)} zone boundaries from GeoJSON")
    return ids


def clean_row(row):
    pickup_dt  = parse_dt(row.get("tpep_pickup_datetime", ""))
    dropoff_dt = parse_dt(row.get("tpep_dropoff_datetime", ""))

    if pickup_dt is None or dropoff_dt is None:
        log_exclusion("unparseable_datetime")
        return None

    if pickup_dt.year != VALID_YEAR:
        log_exclusion(f"pickup_year_not_{VALID_YEAR}")
        return None

    if dropoff_dt < pickup_dt:
        log_exclusion("dropoff_before_pickup")
        return None

    duration_secs = (dropoff_dt - pickup_dt).total_seconds()
    duration_hrs  = duration_secs / 3600

    if duration_hrs > MAX_DURATION_HRS:
        log_exclusion("duration_exceeds_24hrs")
        return None

    distance = safe_float(row.get("trip_distance", ""), default=None)
    fare     = safe_float(row.get("fare_amount", ""),   default=None)

    if distance is None or fare is None:
        log_exclusion("missing_distance_or_fare")
        return None

    if not (MIN_DISTANCE <= distance <= MAX_DISTANCE):
        log_exclusion("distance_out_of_range")
        return None

    if not (MIN_FARE <= fare <= MAX_FARE):
        log_exclusion("fare_out_of_range")
        return None

    if distance == 0 and duration_secs == 0:
        log_exclusion("zero_distance_and_zero_duration")
        return None

    if duration_hrs > 0 and distance > 0:
        speed = distance / duration_hrs
        if speed > MAX_SPEED_MPH:
            log_exclusion("unrealistic_speed_over_200mph")
            return None

    passenger = safe_int(row.get("passenger_count", "1"), default=1)
    if passenger < 0:
        log_exclusion("negative_passenger_count")
        return None

    # normalize categorical IDs, defaulting invalid ones
    vendor_id = str(safe_int(row.get("VendorID", "2"), default=2))
    ratecode  = str(safe_int(row.get("RatecodeID", "1"), default=1))
    payment   = str(safe_int(row.get("payment_type", "1"), default=1))
    pu_loc    = safe_int(row.get("PULocationID", "265"), default=265)
    do_loc    = safe_int(row.get("DOLocationID", "265"), default=265)

    if vendor_id not in VALID_VENDORS:   vendor_id = "2"
    if ratecode not in VALID_RATECODES:  ratecode = "1"
    if payment not in VALID_PAYMENTS:    payment = "5"
    if pu_loc is None or not (1 <= pu_loc <= 265): pu_loc = 265
    if do_loc is None or not (1 <= do_loc <= 265): do_loc = 265

    def norm(key, default=0.0):
        v = safe_float(row.get(key, ""), default=default)
        return round(v, 2)

    extra        = norm("extra")
    mta_tax      = norm("mta_tax")
    tip_amount   = norm("tip_amount")
    tolls_amount = norm("tolls_amount")
    improvement  = norm("improvement_surcharge")
    congestion   = norm("congestion_surcharge")
    total        = norm("total_amount")

    # reject rows with negative financial values
    for name, val in [("extra", extra), ("mta_tax", mta_tax),
                      ("tip_amount", tip_amount), ("tolls_amount", tolls_amount),
                      ("improvement_surcharge", improvement), ("total_amount", total)]:
        if val < 0:
            log_exclusion(f"negative_{name}")
            return None

    if total < fare:
        log_exclusion("total_less_than_fare")
        return None

    flag = row.get("store_and_fwd_flag", "N").strip()
    if flag not in ("Y", "N"):
        flag = "N"

    return {
        "VendorID":              vendor_id,
        "tpep_pickup_datetime":  pickup_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "tpep_dropoff_datetime": dropoff_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "passenger_count":       passenger,
        "trip_distance":         round(distance, 2),
        "RatecodeID":            ratecode,
        "store_and_fwd_flag":    flag,
        "PULocationID":          pu_loc,
        "DOLocationID":          do_loc,
        "payment_type":          payment,
        "fare_amount":           round(fare, 2),
        "extra":                 extra,
        "mta_tax":               mta_tax,
        "tip_amount":            tip_amount,
        "tolls_amount":          tolls_amount,
        "improvement_surcharge": improvement,
        "congestion_surcharge":  congestion,
        "total_amount":          total,
    }


def clean():
    if not os.path.exists(RAW_FILE):
        print(f"ERROR: {RAW_FILE} not found")
        return

    print("Loading dimension tables...")
    zone_lookup = load_zone_lookup()
    geojson_ids = load_geojson_zones()

    if zone_lookup and geojson_ids:
        missing = set(zone_lookup.keys()) - geojson_ids
        if missing:
            print(f"  Note: {len(missing)} zone IDs in lookup but not in GeoJSON: "
                  f"{sorted(missing)[:10]}{'...' if len(missing) > 10 else ''}")

    print(f"Cleaning {os.path.basename(RAW_FILE)}...")

    total = cleaned = excluded = 0
    seen_keys = set()

    fieldnames = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "congestion_surcharge", "total_amount",
        "pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone",
    ]

    with open(RAW_FILE, newline="", encoding="utf-8") as infile, \
         open(CLEANED_FILE, "w", newline="", encoding="utf-8") as outfile:

        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            total += 1
            result = clean_row(row)

            if result is None:
                excluded += 1
                if total % 500000 == 0:
                    print(f"  {total:,} rows processed, {cleaned:,} kept...")
                continue

            # deduplicate on a composite key
            dedup_key = (
                result["tpep_pickup_datetime"],
                result["tpep_dropoff_datetime"],
                str(result["PULocationID"]),
                str(result["DOLocationID"]),
                str(result["trip_distance"]),
                str(result["fare_amount"]),
            )
            if dedup_key in seen_keys:
                log_exclusion("duplicate_row")
                excluded += 1
                continue
            seen_keys.add(dedup_key)

            # enrich with zone metadata
            if zone_lookup:
                pu = zone_lookup.get(int(result["PULocationID"]), {})
                do = zone_lookup.get(int(result["DOLocationID"]), {})
                result["pickup_borough"]  = pu.get("borough", "Unknown")
                result["pickup_zone"]     = pu.get("zone", "Unknown")
                result["dropoff_borough"] = do.get("borough", "Unknown")
                result["dropoff_zone"]    = do.get("zone", "Unknown")
            else:
                result["pickup_borough"]  = ""
                result["pickup_zone"]     = ""
                result["dropoff_borough"] = ""
                result["dropoff_zone"]    = ""

            writer.writerow(result)
            cleaned += 1

            if total % 500000 == 0:
                print(f"  {total:,} rows processed, {cleaned:,} kept...")

    print(f"\nDone. {total:,} total | {cleaned:,} kept | {excluded:,} excluded")
    print(f"Duplicates: {excluded_log.get('duplicate_row', 0):,}")
    print("\nExclusion breakdown:")
    for reason, count in sorted(excluded_log.items(), key=lambda x: -x[1]):
        print(f"  {reason:<40} {count:>10,}")

    # save exclusion log
    log_data = {
        "run_date":     datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "input_file":   os.path.basename(RAW_FILE),
        "total_rows":   total,
        "rows_kept":    cleaned,
        "rows_excluded": excluded,
        "exclusion_rate_pct": round((excluded / total) * 100, 2) if total > 0 else 0,
        "exclusion_reasons": dict(sorted(excluded_log.items(), key=lambda x: -x[1])),
        "assumptions": [
            "Pickup year restricted to 2019",
            "Max trip duration: 24 hours",
            "Distance range: 0-200 miles",
            "Fare range: $0-$500",
            "Speed cap: 200 mph (anything above is physically impossible)",
            "Zero-distance + zero-duration trips excluded (stationary)",
            "Negative financial fields excluded",
            "total_amount < fare_amount excluded (logically inconsistent)",
            "Invalid vendor/ratecode/payment IDs corrected to defaults",
            "Location IDs outside 1-265 mapped to 265 (Outside NYC)",
            "Dedup key: (pickup_time, dropoff_time, PU_loc, DO_loc, distance, fare)",
            "Zone enrichment from taxi_zone_lookup.csv (borough + zone name)",
        ],
        "dimension_tables_loaded": {
            "taxi_zone_lookup": len(zone_lookup),
            "taxi_zones_geojson": len(geojson_ids),
        },
    }

    with open(EXCLUSION_LOG, "w", encoding="utf-8") as f:
        json.dump(log_data, f, indent=2)

    print(f"\nExclusion log: {os.path.basename(EXCLUSION_LOG)}")
    print(f"Cleaned data:  {os.path.basename(CLEANED_FILE)}")


if __name__ == "__main__":
    clean()
