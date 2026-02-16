# Urban Mobility Data Explorer

## Setup

### Prerequisites
- Python 3.8+
- `pyshp` library (for shapefile to GeoJSON conversion)

```bash
pip install pyshp
```

### Data Files

The raw trip data file (`yellow_tripdata_2019-01.csv`) is too large. Download it from the [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) page and place it in the `data/` folder.

The cleaned dataset (`yellow_cleaned_tripdata.csv`) is also gitignored due to size (~1.3 GB). Generate it by running the pipeline:

```bash
python3 data/data_pipeline.py
```

This runs two stages:
1. **Cleaning & Integration** — validates rows, removes duplicates/outliers, enriches with zone metadata
2. **Feature Engineering** — adds 5 derived features (duration, fare/mile, pickup hour, weekend flag, avg speed)

Output: `data/yellow_cleaned_tripdata.csv` (27 columns, ~7.6M rows)

The pipeline also generates `data/exclusion_log.json` with a breakdown of all excluded records and assumptions made during cleaning.
