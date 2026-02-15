# Database Setup

## Requirements

- MySQL installed and running
- Python 3
- mysql-connector-python library

## Steps

### 1. Install Python library

```bash
pip install mysql-connector-python
```

### 2. Create your config file

Create a file called `db_config.py` inside the `database/` folder:

```python
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "yourpassword",
    "database": "urban_mobility"
}
```

Replace `yourpassword` with your actual MySQL password.

### 3. Create the database and tables

```bash
mysql -u root -p < database/schema.sql
```

### 4. Run the insertion script

```bash
python3 database/insert_tripdata.py
```

### 5. Verify

```bash
mysql -u root -p -e "USE urban_mobility; SHOW TABLES; SELECT COUNT(*) FROM trips;"
```

## Notes

- `db_config.py` is in `.gitignore` — each person creates it locally
- Run everything from the project root folder, not inside the database folder
- Make sure the `data/` folder has `yellow_cleaned_tripdata.csv` before running step 4
