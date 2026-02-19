"""
run_schema.py
-------------
Creates all database tables on CockroachDB.

This script converts the MySQL schema.sql to CockroachDB/PostgreSQL syntax
and runs it directly against your live CockroachDB database.

HOW TO RUN (from the project root):
    python run_schema.py

It will:
  1. Connect to CockroachDB
  2. Create all tables (boroughs, service_zones, taxi_zones, rate_codes,
     vendors, payment_types, trips)
  3. Create all indexes
  4. Insert all seed data (vendors, rate codes, payment types, boroughs,
     service zones, and all 265 taxi zones)

If a table already exists, it will be skipped (IF NOT EXISTS).
"""

import sys
import os

# Add the backend folder so we can use the same connection function
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "backend"))

from flask import Flask
from config import config_map
from app.db.connection import get_connection

# Create a minimal Flask app context (needed by get_connection)
app = Flask(__name__)
app.config.from_object(config_map["development"])

# ─────────────────────────────────────────────────────────────────────────────
# CockroachDB-compatible SQL statements
# Key differences from MySQL:
#   - No "CREATE DATABASE" or "USE database" (we connect to defaultdb directly)
#   - AUTO_INCREMENT  →  BIGSERIAL / SERIAL
#   - UNIQUE KEY      →  UNIQUE
#   - TINYINT(1)      →  SMALLINT
#   - TINYINT         →  SMALLINT
#   - CHAR(1)         →  CHAR(1)  (same)
#   - DECIMAL         →  DECIMAL  (same)
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_STATEMENTS = [

    # ── Lookup tables (no foreign keys, create first) ──────────────────────

    """
    CREATE TABLE IF NOT EXISTS boroughs (
        borough_id   SERIAL       NOT NULL,
        borough_name VARCHAR(100) NOT NULL,
        PRIMARY KEY (borough_id),
        UNIQUE (borough_name)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS service_zones (
        zone_type_id SERIAL      NOT NULL,
        zone_type    VARCHAR(50) NOT NULL,
        PRIMARY KEY (zone_type_id),
        UNIQUE (zone_type)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS rate_codes (
        ratecode_id          INT          NOT NULL,
        ratecode_description VARCHAR(100) NOT NULL,
        PRIMARY KEY (ratecode_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS vendors (
        vendor_id   INT          NOT NULL,
        vendor_name VARCHAR(100) NOT NULL,
        PRIMARY KEY (vendor_id)
    )
    """,

    """
    CREATE TABLE IF NOT EXISTS payment_types (
        payment_type_id   INT         NOT NULL,
        payment_type_name VARCHAR(50) NOT NULL,
        PRIMARY KEY (payment_type_id)
    )
    """,

    # ── taxi_zones depends on boroughs and service_zones ───────────────────

    """
    CREATE TABLE IF NOT EXISTS taxi_zones (
        location_id  INT          NOT NULL,
        zone_name    VARCHAR(200) NOT NULL,
        borough_id   INT          NOT NULL,
        zone_type_id INT          NOT NULL,
        PRIMARY KEY (location_id),
        CONSTRAINT fk_zone_borough
            FOREIGN KEY (borough_id)   REFERENCES boroughs(borough_id),
        CONSTRAINT fk_zone_type
            FOREIGN KEY (zone_type_id) REFERENCES service_zones(zone_type_id)
    )
    """,

    # ── trips (main fact table, depends on all lookup tables) ──────────────

    """
    CREATE TABLE IF NOT EXISTS trips (
        trip_id              BIGSERIAL      NOT NULL,

        vendor_id            INT            NOT NULL,
        ratecode_id          INT            NOT NULL,
        payment_type_id      INT            NOT NULL,

        pickup_location_id   INT            NOT NULL,
        dropoff_location_id  INT            NOT NULL,

        pickup_datetime      TIMESTAMP      NOT NULL,
        dropoff_datetime     TIMESTAMP      NOT NULL,

        passenger_count      SMALLINT       NOT NULL DEFAULT 1,
        trip_distance        DECIMAL(8, 2)  NOT NULL DEFAULT 0.00,
        store_and_fwd_flag   CHAR(1)        NOT NULL DEFAULT 'N',

        fare_amount          DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        extra                DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        mta_tax              DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        tip_amount           DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        tolls_amount         DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        improvement_surcharge DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        congestion_surcharge DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        total_amount         DECIMAL(10, 2) NOT NULL DEFAULT 0.00,

        trip_duration_minutes DECIMAL(8, 2)  DEFAULT NULL,
        fare_per_mile        DECIMAL(10, 4)  DEFAULT NULL,
        pickup_hour          SMALLINT        DEFAULT NULL,
        is_weekend           SMALLINT        DEFAULT NULL,
        avg_speed_mph        DECIMAL(10, 4)  DEFAULT NULL,

        PRIMARY KEY (trip_id),

        CONSTRAINT fk_trip_vendor
            FOREIGN KEY (vendor_id)           REFERENCES vendors(vendor_id),
        CONSTRAINT fk_trip_ratecode
            FOREIGN KEY (ratecode_id)         REFERENCES rate_codes(ratecode_id),
        CONSTRAINT fk_trip_payment
            FOREIGN KEY (payment_type_id)     REFERENCES payment_types(payment_type_id),
        CONSTRAINT fk_trip_pickup_zone
            FOREIGN KEY (pickup_location_id)  REFERENCES taxi_zones(location_id),
        CONSTRAINT fk_trip_dropoff_zone
            FOREIGN KEY (dropoff_location_id) REFERENCES taxi_zones(location_id)
    )
    """,

    # ── Indexes for fast queries ────────────────────────────────────────────

    "CREATE INDEX IF NOT EXISTS idx_trips_pickup_datetime  ON trips (pickup_datetime)",
    "CREATE INDEX IF NOT EXISTS idx_trips_dropoff_datetime ON trips (dropoff_datetime)",
    "CREATE INDEX IF NOT EXISTS idx_trips_pickup_hour      ON trips (pickup_hour)",
    "CREATE INDEX IF NOT EXISTS idx_trips_pickup_location  ON trips (pickup_location_id)",
    "CREATE INDEX IF NOT EXISTS idx_trips_dropoff_location ON trips (dropoff_location_id)",
    "CREATE INDEX IF NOT EXISTS idx_trips_fare_amount      ON trips (fare_amount)",
    "CREATE INDEX IF NOT EXISTS idx_trips_trip_distance    ON trips (trip_distance)",
    "CREATE INDEX IF NOT EXISTS idx_trips_zone_hour        ON trips (pickup_location_id, pickup_hour)",
    "CREATE INDEX IF NOT EXISTS idx_trips_is_weekend       ON trips (is_weekend)",

]

# ─────────────────────────────────────────────────────────────────────────────
# Seed data — inserted only if the table is empty (to avoid duplicate errors)
# ─────────────────────────────────────────────────────────────────────────────

SEED_DATA = {

    "vendors": {
        "check": "SELECT COUNT(*) AS n FROM vendors",
        "sql": """
            INSERT INTO vendors (vendor_id, vendor_name) VALUES
            (1, 'Creative Mobile Technologies'),
            (2, 'VeriFone Inc.')
        """
    },

    "rate_codes": {
        "check": "SELECT COUNT(*) AS n FROM rate_codes",
        "sql": """
            INSERT INTO rate_codes (ratecode_id, ratecode_description) VALUES
            (1, 'Standard rate'),
            (2, 'JFK'),
            (3, 'Newark'),
            (4, 'Nassau or Westchester'),
            (5, 'Negotiated fare'),
            (6, 'Group ride')
        """
    },

    "payment_types": {
        "check": "SELECT COUNT(*) AS n FROM payment_types",
        "sql": """
            INSERT INTO payment_types (payment_type_id, payment_type_name) VALUES
            (1, 'Credit card'),
            (2, 'Cash'),
            (3, 'No charge'),
            (4, 'Dispute'),
            (5, 'Unknown'),
            (6, 'Voided trip')
        """
    },

    "boroughs": {
        "check": "SELECT COUNT(*) AS n FROM boroughs",
        "sql": """
            INSERT INTO boroughs (borough_id, borough_name) VALUES
            (1, 'EWR'),
            (2, 'Queens'),
            (3, 'Bronx'),
            (4, 'Manhattan'),
            (5, 'Staten Island'),
            (6, 'Brooklyn'),
            (7, 'Unknown'),
            (8, 'N/A')
        """
    },

    "service_zones": {
        "check": "SELECT COUNT(*) AS n FROM service_zones",
        "sql": """
            INSERT INTO service_zones (zone_type_id, zone_type) VALUES
            (1, 'EWR'),
            (2, 'Boro Zone'),
            (3, 'Yellow Zone'),
            (4, 'Airports'),
            (5, 'N/A')
        """
    },

    "taxi_zones": {
        "check": "SELECT COUNT(*) AS n FROM taxi_zones",
        "sql": """
            INSERT INTO taxi_zones (location_id, zone_name, borough_id, zone_type_id) VALUES
            (1,'Newark Airport',1,1),(2,'Jamaica Bay',2,2),(3,'Allerton/Pelham Gardens',3,2),
            (4,'Alphabet City',4,3),(5,'Arden Heights',5,2),(6,'Arrochar/Fort Wadsworth',5,2),
            (7,'Astoria',2,2),(8,'Astoria Park',2,2),(9,'Auburndale',2,2),(10,'Baisley Park',2,2),
            (11,'Bath Beach',6,2),(12,'Battery Park',4,3),(13,'Battery Park City',4,3),
            (14,'Bay Ridge',6,2),(15,'Bay Terrace/Fort Totten',2,2),(16,'Bayside',2,2),
            (17,'Bedford',6,2),(18,'Bedford Park',3,2),(19,'Bellerose',2,2),(20,'Belmont',3,2),
            (21,'Bensonhurst East',6,2),(22,'Bensonhurst West',6,2),
            (23,'Bloomfield/Emerson Hill',5,2),(24,'Bloomingdale',4,3),(25,'Boerum Hill',6,2),
            (26,'Borough Park',6,2),(27,'Breezy Point/Fort Tilden/Riis Beach',2,2),
            (28,'Briarwood/Jamaica Hills',2,2),(29,'Brighton Beach',6,2),
            (30,'Broad Channel',2,2),(31,'Bronx Park',3,2),(32,'Bronxdale',3,2),
            (33,'Brooklyn Heights',6,2),(34,'Brooklyn Navy Yard',6,2),(35,'Brownsville',6,2),
            (36,'Bushwick North',6,2),(37,'Bushwick South',6,2),(38,'Cambria Heights',2,2),
            (39,'Canarsie',6,2),(40,'Carroll Gardens',6,2),(41,'Central Harlem',4,2),
            (42,'Central Harlem North',4,2),(43,'Central Park',4,3),
            (44,'Charleston/Tottenville',5,2),(45,'Chinatown',4,3),(46,'City Island',3,2),
            (47,'Claremont/Bathgate',3,2),(48,'Clinton East',4,3),(49,'Clinton Hill',6,2),
            (50,'Clinton West',4,3),(51,'Co-Op City',3,2),(52,'Cobble Hill',6,2),
            (53,'College Point',2,2),(54,'Columbia Street',6,2),(55,'Coney Island',6,2),
            (56,'Corona',2,2),(57,'Corona',2,2),(58,'Country Club',3,2),
            (59,'Crotona Park',3,2),(60,'Crotona Park East',3,2),
            (61,'Crown Heights North',6,2),(62,'Crown Heights South',6,2),
            (63,'Cypress Hills',6,2),(64,'Douglaston',2,2),
            (65,'Downtown Brooklyn/MetroTech',6,2),(66,'DUMBO/Vinegar Hill',6,2),
            (67,'Dyker Heights',6,2),(68,'East Chelsea',4,3),
            (69,'East Concourse/Concourse Village',3,2),(70,'East Elmhurst',2,2),
            (71,'East Flatbush/Farragut',6,2),(72,'East Flatbush/Remsen Village',6,2),
            (73,'East Flushing',2,2),(74,'East Harlem North',4,2),
            (75,'East Harlem South',4,2),(76,'East New York',6,2),
            (77,'East New York/Pennsylvania Avenue',6,2),(78,'East Tremont',3,2),
            (79,'East Village',4,3),(80,'East Williamsburg',6,2),(81,'Eastchester',3,2),
            (82,'Elmhurst',2,2),(83,'Elmhurst/Maspeth',2,2),
            (84,'Eltingville/Annadale/Prince''s Bay',5,2),(85,'Erasmus',6,2),
            (86,'Far Rockaway',2,2),(87,'Financial District North',4,3),
            (88,'Financial District South',4,3),(89,'Flatbush/Ditmas Park',6,2),
            (90,'Flatiron',4,3),(91,'Flatlands',6,2),(92,'Flushing',2,2),
            (93,'Flushing Meadows-Corona Park',2,2),(94,'Fordham South',3,2),
            (95,'Forest Hills',2,2),(96,'Forest Park/Highland Park',2,2),
            (97,'Fort Greene',6,2),(98,'Fresh Meadows',2,2),(99,'Freshkills Park',5,2),
            (100,'Garment District',4,3),(101,'Glen Oaks',2,2),(102,'Glendale',2,2),
            (103,'Governor''s Island/Ellis Island/Liberty Island',4,3),
            (104,'Governor''s Island/Ellis Island/Liberty Island',4,3),
            (105,'Governor''s Island/Ellis Island/Liberty Island',4,3),
            (106,'Gowanus',6,2),(107,'Gramercy',4,3),(108,'Gravesend',6,2),
            (109,'Great Kills',5,2),(110,'Great Kills Park',5,2),
            (111,'Green-Wood Cemetery',6,2),(112,'Greenpoint',6,2),
            (113,'Greenwich Village North',4,3),(114,'Greenwich Village South',4,3),
            (115,'Grymes Hill/Clifton',5,2),(116,'Hamilton Heights',4,2),
            (117,'Hammels/Arverne',2,2),(118,'Heartland Village/Todt Hill',5,2),
            (119,'Highbridge',3,2),(120,'Highbridge Park',4,2),
            (121,'Hillcrest/Pomonok',2,2),(122,'Hollis',2,2),(123,'Homecrest',6,2),
            (124,'Howard Beach',2,2),(125,'Hudson Sq',4,3),(126,'Hunts Point',3,2),
            (127,'Inwood',4,2),(128,'Inwood Hill Park',4,2),(129,'Jackson Heights',2,2),
            (130,'Jamaica',2,2),(131,'Jamaica Estates',2,2),(132,'JFK Airport',2,4),
            (133,'Kensington',6,2),(134,'Kew Gardens',2,2),(135,'Kew Gardens Hills',2,2),
            (136,'Kingsbridge Heights',3,2),(137,'Kips Bay',4,3),
            (138,'LaGuardia Airport',2,4),(139,'Laurelton',2,2),
            (140,'Lenox Hill East',4,3),(141,'Lenox Hill West',4,3),
            (142,'Lincoln Square East',4,3),(143,'Lincoln Square West',4,3),
            (144,'Little Italy/NoLiTa',4,3),(145,'Long Island City/Hunters Point',2,2),
            (146,'Long Island City/Queens Plaza',2,2),(147,'Longwood',3,2),
            (148,'Lower East Side',4,3),(149,'Madison',6,2),(150,'Manhattan Beach',6,2),
            (151,'Manhattan Valley',4,3),(152,'Manhattanville',4,2),
            (153,'Marble Hill',4,2),(154,'Marine Park/Floyd Bennett Field',6,2),
            (155,'Marine Park/Mill Basin',6,2),(156,'Mariners Harbor',5,2),
            (157,'Maspeth',2,2),(158,'Meatpacking/West Village West',4,3),
            (159,'Melrose South',3,2),(160,'Middle Village',2,2),
            (161,'Midtown Center',4,3),(162,'Midtown East',4,3),
            (163,'Midtown North',4,3),(164,'Midtown South',4,3),(165,'Midwood',6,2),
            (166,'Morningside Heights',4,2),(167,'Morrisania/Melrose',3,2),
            (168,'Mott Haven/Port Morris',3,2),(169,'Mount Hope',3,2),
            (170,'Murray Hill',4,3),(171,'Murray Hill-Queens',2,2),
            (172,'New Dorp/Midland Beach',5,2),(173,'North Corona',2,2),
            (174,'Norwood',3,2),(175,'Oakland Gardens',2,2),(176,'Oakwood',5,2),
            (177,'Ocean Hill',6,2),(178,'Ocean Parkway South',6,2),
            (179,'Old Astoria',2,2),(180,'Ozone Park',2,2),(181,'Park Slope',6,2),
            (182,'Parkchester',3,2),(183,'Pelham Bay',3,2),(184,'Pelham Bay Park',3,2),
            (185,'Pelham Parkway',3,2),(186,'Penn Station/Madison Sq West',4,3),
            (187,'Port Richmond',5,2),(188,'Prospect-Lefferts Gardens',6,2),
            (189,'Prospect Heights',6,2),(190,'Prospect Park',6,2),
            (191,'Queens Village',2,2),(192,'Queensboro Hill',2,2),
            (193,'Queensbridge/Ravenswood',2,2),(194,'Randalls Island',4,3),
            (195,'Red Hook',6,2),(196,'Rego Park',2,2),(197,'Richmond Hill',2,2),
            (198,'Ridgewood',2,2),(199,'Rikers Island',3,2),
            (200,'Riverdale/North Riverdale/Fieldston',3,2),(201,'Rockaway Park',2,2),
            (202,'Roosevelt Island',4,2),(203,'Rosedale',2,2),
            (204,'Rossville/Woodrow',5,2),(205,'Saint Albans',2,2),
            (206,'Saint George/New Brighton',5,2),
            (207,'Saint Michaels Cemetery/Woodside',2,2),
            (208,'Schuylerville/Edgewater Park',3,2),(209,'Seaport',4,3),
            (210,'Sheepshead Bay',6,2),(211,'SoHo',4,3),
            (212,'Soundview/Bruckner',3,2),(213,'Soundview/Castle Hill',3,2),
            (214,'South Beach/Dongan Hills',5,2),(215,'South Jamaica',2,2),
            (216,'South Ozone Park',2,2),(217,'South Williamsburg',6,2),
            (218,'Springfield Gardens North',2,2),(219,'Springfield Gardens South',2,2),
            (220,'Spuyten Duyvil/Kingsbridge',3,2),(221,'Stapleton',5,2),
            (222,'Starrett City',6,2),(223,'Steinway',2,2),
            (224,'Stuy Town/Peter Cooper Village',4,3),(225,'Stuyvesant Heights',6,2),
            (226,'Sunnyside',2,2),(227,'Sunset Park East',6,2),
            (228,'Sunset Park West',6,2),(229,'Sutton Place/Turtle Bay North',4,3),
            (230,'Times Sq/Theatre District',4,3),(231,'TriBeCa/Civic Center',4,3),
            (232,'Two Bridges/Seward Park',4,3),(233,'UN/Turtle Bay South',4,3),
            (234,'Union Sq',4,3),(235,'University Heights/Morris Heights',3,2),
            (236,'Upper East Side North',4,3),(237,'Upper East Side South',4,3),
            (238,'Upper West Side North',4,3),(239,'Upper West Side South',4,3),
            (240,'Van Cortlandt Park',3,2),(241,'Van Cortlandt Village',3,2),
            (242,'Van Nest/Morris Park',3,2),(243,'Washington Heights North',4,2),
            (244,'Washington Heights South',4,2),(245,'West Brighton',5,2),
            (246,'West Chelsea/Hudson Yards',4,3),(247,'West Concourse',3,2),
            (248,'West Farms/Bronx River',3,2),(249,'West Village',4,3),
            (250,'Westchester Village/Unionport',3,2),(251,'Westerleigh',5,2),
            (252,'Whitestone',2,2),(253,'Willets Point',2,2),
            (254,'Williamsbridge/Olinville',3,2),(255,'Williamsburg (North Side)',6,2),
            (256,'Williamsburg (South Side)',6,2),(257,'Windsor Terrace',6,2),
            (258,'Woodhaven',2,2),(259,'Woodlawn/Wakefield',3,2),(260,'Woodside',2,2),
            (261,'World Trade Center',4,3),(262,'Yorkville East',4,3),
            (263,'Yorkville West',4,3),(264,'N/A',7,5),(265,'Outside of NYC',8,5)
        """
    },
}


def run_schema():
    """Main function that creates all tables and inserts seed data."""

    print("Connecting to CockroachDB...")
    print()

    with app.app_context():
        conn = get_connection()
        cur  = conn.cursor()

        # ── Step 1: Create tables and indexes ──────────────────────────────
        print("Creating tables and indexes...")
        for i, statement in enumerate(SCHEMA_STATEMENTS, 1):
            # Get a short description for the progress message
            first_line = statement.strip().split("\n")[0].strip()
            try:
                cur.execute(statement)
                conn.commit()
                print("  [OK] " + first_line[:70])
            except Exception as e:
                conn.rollback()
                err = str(e).split("\n")[0]  # just the first line of the error
                print("  [SKIP/ERROR] " + first_line[:60] + " -> " + err)

        print()

        # ── Step 2: Insert seed data (only if tables are empty) ────────────
        print("Inserting seed data...")
        for table_name, info in SEED_DATA.items():
            try:
                # Check if the table already has rows
                cur.execute(info["check"])
                row = cur.fetchone()
                count = row["n"] if row else 0

                if count > 0:
                    print("  [SKIP] " + table_name + " already has " + str(count) + " rows")
                else:
                    cur.execute(info["sql"])
                    conn.commit()
                    print("  [OK]   " + table_name + " -- rows inserted")

            except Exception as e:
                conn.rollback()
                err = str(e).split("\n")[0]
                print("  [ERROR] " + table_name + " -> " + err)

        cur.close()
        conn.close()

    print()
    print("Done! Run 'python test_db_connection.py' to verify the tables exist.")
    print("Next step: run the data insertion script to load trip records.")


# Run the function when this script is executed
if __name__ == "__main__":
    run_schema()
