import sqlite3
import requests
import json
from tqdm import tqdm
import pandas as pd
from datetime import datetime, timedelta
import sys

DB_PATH = "cear.db"
BASE_URL = "https://api.sealevelsensors.org/v1.0"
OBSERVATION_LIMIT = 7000
BATCH_SIZE = 1000

# ---- Helpers ----

def safe_print(*args, **kwargs):
    tqdm.write(" ".join(str(a) for a in args), file=sys.stdout, **kwargs)

def get_api_data(path):
    url = BASE_URL + path
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def parse_interval(val):
    if isinstance(val, dict):
        return val.get("start", None), val.get("end", None)
    elif isinstance(val, str):
        return val, None
    else:
        return None, None

def truncate_and_shift_time(iso_time_str):
    """Truncate ISO timestamp to seconds and shift by +1 second."""
    if not iso_time_str:
        return None
    
    # Parse time
    try:
        dt = datetime.fromisoformat(iso_time_str.replace("Z", ""))
        # Shift forward 1 second
        dt += timedelta(seconds=1)
        # Reformat to ISO with Z
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        print(f"⚠️ Failed to parse and shift time: {iso_time_str} → {e}")
        return iso_time_str  # fallback: return original
    
def insert_observations(conn, c, datastream_id, observations, batch_size):
    count_since_commit = 0

    if not observations:
        print(f"No new observations for Datastream {datastream_id}. Skipping insert.")
        return

    print(f"Inserting {len(observations)} observations for Datastream {datastream_id}...")

    for obs in tqdm(observations, desc=f"Datastream {datastream_id} Observations", leave=False):
        obs_id = obs["@iot.id"]

        # FeatureOfInterest
        foi = fetch_feature_of_interest(obs_id)
        c.execute("""INSERT OR IGNORE INTO features_of_interest 
                     (feature_of_interest_id, name, description, encoding_type, feature, properties)
                     VALUES (?, ?, ?, ?, ?, ?)""",
                  (foi["@iot.id"], foi["name"], foi.get("description", ""), foi["encodingType"],
                   json.dumps(foi["feature"]), json.dumps(foi.get("properties", {}))))

        # Observation
        phenomenon_start, phenomenon_end = parse_interval(obs.get("phenomenonTime"))
        valid_start, valid_end = parse_interval(obs.get("validTime"))

        c.execute("""INSERT OR IGNORE INTO observations 
            (observation_id, datastream_id, phenomenon_time_start, phenomenon_time_end,
            result_time, result, result_quality, valid_time_start, valid_time_end, parameters, feature_of_interest_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (obs_id, datastream_id,
            phenomenon_start,
            phenomenon_end,
            obs.get("resultTime", None),
            json.dumps(obs.get("result", None)),
            json.dumps(obs.get("resultQuality", [])),
            valid_start,
            valid_end,
            json.dumps(obs.get("parameters", {})),
            foi["@iot.id"]))

        count_since_commit += 1

        # Commit every batch_size
        if count_since_commit >= batch_size:
            conn.commit()
            print(f"Committed {count_since_commit} observations so far for Datastream {datastream_id}.")
            count_since_commit = 0

    # Final commit for any remaining
    if count_since_commit > 0:
        conn.commit()
        print(f"Final commit of {count_since_commit} observations for Datastream {datastream_id}.")


# Step 1: Create database and tables
def create_db():
    schema_sql = """
    CREATE TABLE IF NOT EXISTS things (
        thing_id INTEGER PRIMARY KEY,
        name TEXT,
        description TEXT,
        properties TEXT
    );

    CREATE TABLE IF NOT EXISTS locations (
        location_id INTEGER PRIMARY KEY,
        name TEXT,
        description TEXT,
        encoding_type TEXT,
        location TEXT,
        properties TEXT
    );

    CREATE TABLE IF NOT EXISTS thing_locations (
        thing_id INTEGER,
        location_id INTEGER,
        PRIMARY KEY (thing_id, location_id),
        FOREIGN KEY (thing_id) REFERENCES things(thing_id),
        FOREIGN KEY (location_id) REFERENCES locations(location_id)
    );

    CREATE TABLE IF NOT EXISTS historical_locations (
        historical_location_id INTEGER PRIMARY KEY,
        thing_id INTEGER,
        time TEXT,
        FOREIGN KEY (thing_id) REFERENCES things(thing_id)
    );

    CREATE TABLE IF NOT EXISTS historical_location_locations (
        historical_location_id INTEGER,
        location_id INTEGER,
        PRIMARY KEY (historical_location_id, location_id),
        FOREIGN KEY (historical_location_id) REFERENCES historical_locations(historical_location_id),
        FOREIGN KEY (location_id) REFERENCES locations(location_id)
    );

    CREATE TABLE IF NOT EXISTS sensors (
        sensor_id INTEGER PRIMARY KEY,
        name TEXT,
        description TEXT,
        encoding_type TEXT,
        metadata TEXT,
        properties TEXT
    );

    CREATE TABLE IF NOT EXISTS observed_properties (
        observed_property_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        definition TEXT,
        properties TEXT
    );

    CREATE TABLE IF NOT EXISTS datastreams (
        datastream_id INTEGER PRIMARY KEY,
        thing_id INTEGER NOT NULL,
        sensor_id INTEGER NOT NULL,
        observed_property_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        description TEXT,
        observation_type TEXT,
        unit_of_measurement_name TEXT,
        unit_of_measurement_symbol TEXT,
        unit_of_measurement_definition TEXT,
        observed_area TEXT,
        phenomenon_time_start TEXT,
        phenomenon_time_end TEXT,
        result_time_start TEXT,
        result_time_end TEXT,
        properties TEXT,
        FOREIGN KEY (thing_id) REFERENCES things(thing_id),
        FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id),
        FOREIGN KEY (observed_property_id) REFERENCES observed_properties(observed_property_id)
    );

    CREATE TABLE IF NOT EXISTS features_of_interest (
        feature_of_interest_id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        description TEXT,
        encoding_type TEXT NOT NULL,
        feature TEXT,
        properties TEXT
    );

    CREATE TABLE IF NOT EXISTS observations (
        observation_id INTEGER PRIMARY KEY,
        datastream_id INTEGER NOT NULL,
        phenomenon_time_start TEXT,
        phenomenon_time_end TEXT,
        result_time TEXT,
        result TEXT,
        result_quality TEXT,
        valid_time_start TEXT,
        valid_time_end TEXT,
        parameters TEXT,
        feature_of_interest_id INTEGER NOT NULL,
        FOREIGN KEY (feature_of_interest_id) REFERENCES features_of_interest(feature_of_interest_id),
        FOREIGN KEY (datastream_id) REFERENCES datastreams(datastream_id)
    );
    """

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript(schema_sql)
    conn.commit()
    conn.close()
    print("Database schema created successfully.")

# ---- Fetch functions ----

def fetch_things() -> list:
    """Fetch all Things."""
    return get_api_data("/Things")["value"]

def fetch_locations(thing_id: int) -> list:
    """Fetch Locations for a Thing."""
    return get_api_data(f"/Things({thing_id})/Locations")["value"]

def fetch_historical_locations(thing_id: int) -> list:
    """Fetch HistoricalLocations for a Thing (with expanded Locations)."""
    hl_data = get_api_data(f"/Things({thing_id})/HistoricalLocations?$expand=Locations")["value"]
    for hl in hl_data:
        hl["Locations"] = hl.get("Locations", [])
    return hl_data

def fetch_datastreams(thing_id: int) -> list:
    """Fetch Datastreams for a Thing."""
    return get_api_data(f"/Things({thing_id})/Datastreams")["value"]

def fetch_datastream_full(datastream_id: int) -> dict:
    """Fetch full Datastream with Sensor and ObservedProperty links."""
    return get_api_data(f"/Datastreams({datastream_id})")

def fetch_sensor_from_link(sensor_link: str) -> dict:
    """Fetch Sensor using provided link."""
    path = sensor_link.replace(BASE_URL, "")
    return get_api_data(path)

def fetch_observed_property_from_link(op_link: str) -> dict:
    """Fetch ObservedProperty using provided link."""
    path = op_link.replace(BASE_URL, "")
    return get_api_data(path)

def fetch_observations(datastream_id: int) -> list:
    """Fetch first 10 Observations for a Datastream."""
    return get_api_data(f"/Datastreams({datastream_id})/Observations?$top=10&$orderby=phenomenonTime asc")["value"]

def fetch_feature_of_interest(observation_id: int) -> dict:
    """Fetch FeatureOfInterest for an Observation."""
    link = get_api_data(f"/Observations({observation_id})")["FeatureOfInterest@iot.navigationLink"]
    path = link.replace(BASE_URL, "")
    return get_api_data(path)

# ---- Main populate_db() ----

def fetch_all_observations(datastream_id: int, page_size=1000, after_time=None, limit=None) -> list:
    """Fetch Observations for a Datastream using paging. Optionally only fetch after a given time."""
    observations = []
    skip = 0
    fetched = 0

    print(f"Datastream {datastream_id} → Fallback fetching observations...")
    pbar = tqdm(desc=f"Datastream {datastream_id} Fallback paging", unit="obs")

    while True:
        params = {
            "$top": page_size,
            "$skip": skip,
            "$orderby": "phenomenonTime asc"
        }

        if after_time:
            params["$filter"] = f"phenomenonTime gt {repr(after_time)}"

        url = f"{BASE_URL}/Datastreams({datastream_id})/Observations"

        response = requests.get(url, params=params)
        response.raise_for_status()
        page = response.json()["value"]

        if not page:
            break

        observations.extend(page)
        fetched += len(page)
        skip += page_size

        # Update progress bar
        pbar.update(len(page))

        # Optional limit safeguard
        if limit and fetched >= limit:
            pbar.close()
            return observations[:limit]

    pbar.close()
    return observations


def parse_time_range(field):
    """Parse start/end time range."""
    if isinstance(field, str) and "/" in field:
        start, end = field.split("/")
        return start, end
    return None, None

def populate_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Step 1: Fetch all Things
    things = fetch_things()

    for thing in tqdm(things, desc="Processing Things"):
        thing_id = thing["@iot.id"]
        c.execute("""INSERT OR IGNORE INTO things 
                     (thing_id, name, description, properties)
                     VALUES (?, ?, ?, ?)""",
                  (thing_id, thing["name"], thing.get("description", ""), 
                   json.dumps(thing.get("properties", {}))))

        # Step 2: Fetch Locations
        locations = fetch_locations(thing_id)
        for loc in tqdm(locations, desc=f"Thing {thing_id} Locations", leave=False):
            location_id = loc["@iot.id"]
            c.execute("""INSERT OR IGNORE INTO locations 
                         (location_id, name, description, encoding_type, location, properties)
                         VALUES (?, ?, ?, ?, ?, ?)""",
                      (location_id, loc["name"], loc.get("description", ""), loc["encodingType"],
                       json.dumps(loc["location"]), json.dumps(loc.get("properties", {}))))
            c.execute("""INSERT OR IGNORE INTO thing_locations 
                         (thing_id, location_id)
                         VALUES (?, ?)""",
                      (thing_id, location_id))

        # Step 3: Fetch HistoricalLocations
        historical_locations = fetch_historical_locations(thing_id)
        for hl in tqdm(historical_locations, desc=f"Thing {thing_id} HistoricalLocations", leave=False):
            hl_id = hl["@iot.id"]
            c.execute("""INSERT OR IGNORE INTO historical_locations 
                         (historical_location_id, thing_id, time)
                         VALUES (?, ?, ?)""",
                      (hl_id, thing_id, hl["time"]))

            for loc in hl.get("Locations", []):
                location_id = loc["@iot.id"]
                c.execute("""INSERT OR IGNORE INTO locations 
                             (location_id, name, description, encoding_type, location, properties)
                             VALUES (?, ?, ?, ?, ?, ?)""",
                          (location_id, loc["name"], loc.get("description", ""), loc["encodingType"],
                           json.dumps(loc["location"]), json.dumps(loc.get("properties", {}))))
                c.execute("""INSERT OR IGNORE INTO historical_location_locations 
                             (historical_location_id, location_id)
                             VALUES (?, ?)""",
                          (hl_id, location_id))

        # Step 4: Fetch Datastreams
        datastreams = fetch_datastreams(thing_id)
        for ds in tqdm(datastreams, desc=f"Thing {thing_id} Datastreams", leave=False):
            ds_id = ds["@iot.id"]

            # Step 5: Fetch full Datastream once (optimized!)
            ds_full = fetch_datastream_full(ds_id)
            sensor_link = ds_full["Sensor@iot.navigationLink"]
            op_link = ds_full["ObservedProperty@iot.navigationLink"]

            # Step 6: Fetch Sensor
            sensor = fetch_sensor_from_link(sensor_link)
            c.execute("""INSERT OR IGNORE INTO sensors 
                         (sensor_id, name, description, encoding_type, metadata, properties)
                         VALUES (?, ?, ?, ?, ?, ?)""",
                      (sensor["@iot.id"], sensor["name"], sensor.get("description", ""),
                       sensor["encodingType"], sensor.get("metadata", ""), json.dumps(sensor.get("properties", {}))))

            # Step 7: Fetch ObservedProperty
            op = fetch_observed_property_from_link(op_link)
            c.execute("""INSERT OR IGNORE INTO observed_properties 
                         (observed_property_id, name, description, definition, properties)
                         VALUES (?, ?, ?, ?, ?)""",
                      (op["@iot.id"], op["name"], op.get("description", ""), op["definition"], 
                       json.dumps(op.get("properties", {}))))

            # Insert Datastream
            uom = ds_full.get("unitOfMeasurement", {})
            pheno_start, pheno_end = parse_time_range(ds_full.get("phenomenonTime", ""))
            result_start, result_end = parse_time_range(ds_full.get("resultTime", ""))

            c.execute("""INSERT OR IGNORE INTO datastreams 
                         (datastream_id, thing_id, sensor_id, observed_property_id, name, description, observation_type,
                          unit_of_measurement_name, unit_of_measurement_symbol, unit_of_measurement_definition,
                          observed_area, phenomenon_time_start, phenomenon_time_end, result_time_start, result_time_end, properties)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                      (ds_id, thing_id, sensor["@iot.id"], op["@iot.id"], ds_full["name"], ds_full.get("description", ""),
                       ds_full["observationType"], uom.get("name", ""), uom.get("symbol", ""), uom.get("definition", ""),
                       json.dumps(ds_full.get("observedArea", None)),
                       pheno_start, pheno_end, result_start, result_end,
                       json.dumps(ds_full.get("properties", {}))))

            # Step 8a: Fetch ALL Observations with paging
            observations = fetch_new_observations(ds_id, conn)

            for obs in tqdm(observations, desc=f"Datastream {ds_id} Observations", leave=False):
                obs_id = obs["@iot.id"]

                # Step 8b: Fetch FeatureOfInterest
                foi = fetch_feature_of_interest(obs_id)
                c.execute("""INSERT OR IGNORE INTO features_of_interest 
                             (feature_of_interest_id, name, description, encoding_type, feature, properties)
                             VALUES (?, ?, ?, ?, ?, ?)""",
                          (foi["@iot.id"], foi["name"], foi.get("description", ""), foi["encodingType"],
                           json.dumps(foi["feature"]), json.dumps(foi.get("properties", {}))))

                phenomenon_start, phenomenon_end = parse_interval(obs.get("phenomenonTime"))
                valid_start, valid_end = parse_interval(obs.get("validTime"))

                # Insert Observation
                c.execute("""INSERT OR IGNORE INTO observations 
                    (observation_id, datastream_id, phenomenon_time_start, phenomenon_time_end,
                    result_time, result, result_quality, valid_time_start, valid_time_end, parameters, feature_of_interest_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (obs_id, ds_id,
                    phenomenon_start,
                    phenomenon_end,
                    obs.get("resultTime", None),
                    json.dumps(obs.get("result", None)),
                    json.dumps(obs.get("resultQuality", [])),
                    valid_start,
                    valid_end,
                    json.dumps(obs.get("parameters", {})),
                    foi["@iot.id"]))

        # Commit per Thing
        conn.commit()

    conn.close()
    print("Database populated successfully.")

def list_things() -> list:
    """
    List Things with associated Location name.
    Return a list of tuples: [(thing_id, thing_name, location_name), ...]
    """
    things = get_api_data("/Things?$expand=Locations")["value"]

    numbered_list = []

    print("\nAvailable Things by Location:")
    for thing in things:
        thing_id = thing["@iot.id"]
        thing_name = thing["name"]
        locations = thing.get("Locations", [])
        if locations:
            for loc in locations:
                loc_name = loc["name"]
                numbered_list.append((thing_id, thing_name, loc_name))
        else:
            numbered_list.append((thing_id, thing_name, "(No Location)"))

    # Display numbered menu
    for i, (thing_id, thing_name, loc_name) in enumerate(numbered_list, start=1):
        print(f"{i:2}. {thing_name} → {loc_name}")

    return numbered_list

def populate_single_thing(thing_id: int):
    """Populate database for a single Thing (by thing_id)."""
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    try:
        # Fetch the Thing
        url = f"/Things({thing_id})"
        thing = get_api_data(url)

        print(f"\nPopulating Thing {thing_id} - {thing['name']}")

        c.execute("""INSERT OR IGNORE INTO things 
                    (thing_id, name, description, properties)
                    VALUES (?, ?, ?, ?)""",
                (thing_id, thing["name"], thing.get("description", ""), 
                json.dumps(thing.get("properties", {}))))

        # Locations
        locations = fetch_locations(thing_id)
        for loc in tqdm(locations, desc=f"Thing {thing_id} Locations"):
            location_id = loc["@iot.id"]
            c.execute("""INSERT OR IGNORE INTO locations 
                        (location_id, name, description, encoding_type, location, properties)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                    (location_id, loc["name"], loc.get("description", ""), loc["encodingType"],
                    json.dumps(loc["location"]), json.dumps(loc.get("properties", {}))))
            c.execute("""INSERT OR IGNORE INTO thing_locations 
                        (thing_id, location_id)
                        VALUES (?, ?)""",
                    (thing_id, location_id))

        # HistoricalLocations
        historical_locations = fetch_historical_locations(thing_id)
        for hl in tqdm(historical_locations, desc=f"Thing {thing_id} HistoricalLocations"):
            hl_id = hl["@iot.id"]
            c.execute("""INSERT OR IGNORE INTO historical_locations 
                        (historical_location_id, thing_id, time)
                        VALUES (?, ?, ?)""",
                    (hl_id, thing_id, hl["time"]))

            for loc in hl.get("Locations", []):
                location_id = loc["@iot.id"]
                c.execute("""INSERT OR IGNORE INTO locations 
                            (location_id, name, description, encoding_type, location, properties)
                            VALUES (?, ?, ?, ?, ?, ?)""",
                        (location_id, loc["name"], loc.get("description", ""), loc["encodingType"],
                        json.dumps(loc["location"]), json.dumps(loc.get("properties", {}))))
                c.execute("""INSERT OR IGNORE INTO historical_location_locations 
                            (historical_location_id, location_id)
                            VALUES (?, ?)""",
                        (hl_id, location_id))

        # Datastreams
        datastreams = fetch_datastreams(thing_id)
        for ds in tqdm(datastreams, desc=f"Thing {thing_id} Datastreams"):
            ds_id = ds["@iot.id"]

            # Fetch full Datastream once
            ds_full = fetch_datastream_full(ds_id)
            sensor_link = ds_full["Sensor@iot.navigationLink"]
            op_link = ds_full["ObservedProperty@iot.navigationLink"]

            # Sensor
            sensor = fetch_sensor_from_link(sensor_link)
            c.execute("""INSERT OR IGNORE INTO sensors 
                        (sensor_id, name, description, encoding_type, metadata, properties)
                        VALUES (?, ?, ?, ?, ?, ?)""",
                    (sensor["@iot.id"], sensor["name"], sensor.get("description", ""),
                    sensor["encodingType"], sensor.get("metadata", ""), json.dumps(sensor.get("properties", {}))))

            # ObservedProperty
            op = fetch_observed_property_from_link(op_link)
            c.execute("""INSERT OR IGNORE INTO observed_properties 
                        (observed_property_id, name, description, definition, properties)
                        VALUES (?, ?, ?, ?, ?)""",
                    (op["@iot.id"], op["name"], op.get("description", ""), op["definition"], 
                    json.dumps(op.get("properties", {}))))

            # Insert Datastream
            uom = ds_full.get("unitOfMeasurement", {})
            pheno_start, pheno_end = parse_time_range(ds_full.get("phenomenonTime", ""))
            result_start, result_end = parse_time_range(ds_full.get("resultTime", ""))

            c.execute("""INSERT OR IGNORE INTO datastreams 
                        (datastream_id, thing_id, sensor_id, observed_property_id, name, description, observation_type,
                        unit_of_measurement_name, unit_of_measurement_symbol, unit_of_measurement_definition,
                        observed_area, phenomenon_time_start, phenomenon_time_end, result_time_start, result_time_end, properties)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (ds_id, thing_id, sensor["@iot.id"], op["@iot.id"], ds_full["name"], ds_full.get("description", ""),
                    ds_full["observationType"], uom.get("name", ""), uom.get("symbol", ""), uom.get("definition", ""),
                    json.dumps(ds_full.get("observedArea", None)),
                    pheno_start, pheno_end, result_start, result_end,
                    json.dumps(ds_full.get("properties", {}))))

            # Observations
            observations, latest_db_time = fetch_new_observations(ds_id, conn)
            insert_observations(conn, c, ds_id, observations, batch_size=BATCH_SIZE)

            print(f"Finished Datastream {ds_id}.")

    except Exception as e:
        print(f"Error while populating Thing {thing_id}: {e}")
        conn.rollback()  # rollback if something failed

    finally:
        # Always close connection
        conn.close()
        print(f"\nFinished populating Thing {thing_id}.\n")

def is_thing_up_to_date(thing_id: int, conn, page_size=1) -> bool:
    """
    Check if a Thing is up to date in the DB.
    Returns True if all Datastreams are up to date, else False.
    """
    datastreams = fetch_datastreams(thing_id)

    for ds in datastreams:
        ds_id = ds["@iot.id"]

        # Step 1: Get latest observation from API
        url = f"/Datastreams({ds_id})/Observations?$orderby=phenomenonTime desc&$top={page_size}"
        api_obs = get_api_data(url)["value"]
        if not api_obs:
            print(f"Datastream {ds_id} → No Observations in API.")
            continue

        latest_api_time = api_obs[0]["phenomenonTime"]

        # Step 2: Get latest observation from DB
        c = conn.cursor()
        c.execute("""
            SELECT MAX(phenomenon_time_start)
            FROM observations
            WHERE datastream_id = ?;
        """, (ds_id,))
        result = c.fetchone()
        latest_db_time = result[0]

        print(f"Datastream {ds_id} → API: {latest_api_time} | DB: {latest_db_time}")

        # Step 3: Compare
        if latest_db_time is None:
            print(f"Datastream {ds_id} is missing in DB → Not up to date.")
            return False

        if latest_db_time < latest_api_time:
            print(f"Datastream {ds_id} has newer data in API → Not up to date.")
            return False

    print(f"Thing {thing_id} → All Datastreams up to date")
    return True


def fetch_new_observations(datastream_id: int, conn, page_size=1000, limit=None) -> tuple[list, str]:
    """
    Fetch only new Observations for a Datastream:
    - Check the DB for latest phenomenon_time_start
    - Use $filter to fetch only newer observations
    - Return (observations, latest_db_time)
    """
    # Step 1: Find latest observation time in DB
    c = conn.cursor()
    c.execute("""
        SELECT MAX(phenomenon_time_start) 
        FROM observations 
        WHERE datastream_id = ?;
    """, (datastream_id,))
    result = c.fetchone()
    latest_time = result[0]

    # Truncate microseconds
    if latest_time and "." in latest_time:
        latest_time = latest_time.split(".")[0] + "Z"

    safe_print(f"Datastream {datastream_id} → Latest DB time: {latest_time}")

    # Step 2: Setup paging
    observations = []
    skip = 0
    fetched = 0

    try:
        while True:
            params = {
                "$top": page_size,
                "$skip": skip,
                "$orderby": "phenomenonTime asc"
            }

            # Some APIs are picky about microseconds, so you can truncate them safely if needed:
            if latest_time.endswith("Z"):
                filter_time = latest_time
            else:
                filter_time = latest_time + "Z"

            params["$filter"] = f"phenomenonTime gt {filter_time}"


            url = f"{BASE_URL}/Datastreams({datastream_id})/Observations"

            response = requests.get(url, params=params)
            response.raise_for_status()
            page = response.json()["value"]

            if not page:
                break

            observations.extend(page)
            fetched += len(page)
            skip += page_size

            if limit and fetched >= limit:
                return observations[:limit], latest_time

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 500:
            print("⚠️ 500 Error on filtered query → falling back to fetch ALL observations and filter in Python.")
            observations = fetch_all_observations(datastream_id, after_time=latest_time, limit=limit)
            print(f"Fallback fetched {len(observations)} new observations.")
        else:
            raise  # Re-raise other errors

    return observations, latest_time

def view_db():
    conn = sqlite3.connect(DB_PATH)

    tables = [
        "things", "locations", "thing_locations", "historical_locations", "historical_location_locations",
        "sensors", "observed_properties", "datastreams", "features_of_interest", "observations"
    ]

    while True:
        print("\n--- Available Tables ---")
        for i, table in enumerate(tables, start=1):
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"{i:2}. {table} ({count} rows)")
        print(" Q. Return to main menu")

        choice = input("\nEnter table number to view, or 'Q' to return: ").strip().lower()

        if choice == "q":
            break
        if not choice.isdigit() or not (1 <= int(choice) <= len(tables)):
            print("Invalid choice. Please enter a valid number.")
            continue

        index = int(choice) - 1
        table = tables[index]

        print(f"\n--- Preview: {table} ---")
        df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 10", conn)
        print(df.to_string(index=False))

    conn.close()

def delete_thing(thing_id: int):
    """Delete a Thing and all related data from the database."""

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    print(f"\nDeleting Thing {thing_id} and all related data...")

    # Delete Observations (must be first due to FK constraints)
    c.execute("""
        DELETE FROM observations 
        WHERE datastream_id IN (
            SELECT datastream_id FROM datastreams WHERE thing_id = ?
        )
    """, (thing_id,))

    # Delete Datastreams
    c.execute("DELETE FROM datastreams WHERE thing_id = ?", (thing_id,))

    # Delete HistoricalLocation-Location links
    c.execute("""
        DELETE FROM historical_location_locations
        WHERE historical_location_id IN (
            SELECT historical_location_id FROM historical_locations WHERE thing_id = ?
        )
    """, (thing_id,))

    # Delete HistoricalLocations
    c.execute("DELETE FROM historical_locations WHERE thing_id = ?", (thing_id,))

    # Delete Thing-Location links
    c.execute("DELETE FROM thing_locations WHERE thing_id = ?", (thing_id,))

    # Finally delete the Thing
    c.execute("DELETE FROM things WHERE thing_id = ?", (thing_id,))

    conn.commit()
    conn.close()

    print(f"Thing {thing_id} deleted from database.\n")

def display_numbered_list(numbered_list):
    print("\nAvailable Things by Location:")
    for i, (thing_id, thing_name, loc_name) in enumerate(numbered_list, start=1):
        print(f"{i:2}. {thing_name} → {loc_name}")

if __name__ == "__main__":
    create_db()

    numbered_list = list_things()

    while True:
        print("\nOptions:")
        print("  V        → View database contents")
        print("  C        → Check if selected Thing is up to date")
        print("  D        → Delete selected Thing from database")
        print("  L        → List available Things again")
        print("  [number] → Populate selected Location / Thing")
        print("  Q        → Quit")

        choice = input("\nEnter choice: ").strip().lower()
        if choice == "q":
            print("Goodbye!")
            break
        elif choice == "v":
            view_db()
            continue
        elif choice == "c":
            # Ask which Thing to check
            check_choice = input("Enter number of Location / Thing to check: ").strip()
            if not check_choice.isdigit() or not (1 <= int(check_choice) <= len(numbered_list)):
                print("Invalid choice.")
                continue

            index = int(check_choice) - 1
            thing_id, thing_name, loc_name = numbered_list[index]

            print(f"\nChecking if Thing {thing_id} → {thing_name} → {loc_name} is up to date...\n")
            conn = sqlite3.connect(DB_PATH)
            up_to_date = is_thing_up_to_date(thing_id, conn)
            conn.close()

            if up_to_date:
                print(f"Thing {thing_id} → {thing_name} is up to date.")
            else:
                print(f"Thing {thing_id} → {thing_name} has new data in API.")
            continue
        elif choice == "d":
            delete_choice = input("Enter number of Location / Thing to delete: ").strip()
            if not delete_choice.isdigit() or not (1 <= int(delete_choice) <= len(numbered_list)):
                print("Invalid choice.")
                continue

            index = int(delete_choice) - 1
            thing_id, thing_name, loc_name = numbered_list[index]

            confirm = input(f"Are you sure you want to DELETE Thing {thing_id} → {thing_name}? Type 'yes' to confirm: ")
            if confirm.lower() == "yes":
                delete_thing(thing_id)
            else:
                print("Cancelled.")
            continue
        elif choice == "l":
            display_numbered_list(numbered_list)
            continue
        # Otherwise, assume populate
        if not choice.isdigit() or not (1 <= int(choice) <= len(numbered_list)):
            print("Invalid choice. Please enter a valid number.")
            continue

        index = int(choice) - 1
        thing_id, thing_name, loc_name = numbered_list[index]

        print(f"\nPopulating Thing {thing_id} → {thing_name} → {loc_name}\n")
        populate_single_thing(thing_id)



