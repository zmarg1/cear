import sqlite3
import requests
import json
from tqdm import tqdm
import pandas as pd

DB_PATH = "cear.db"
BASE_URL = "https://api.sealevelsensors.org/v1.0"

# ---- Helper ----
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

def fetch_all_observations(datastream_id: int, page_size=1000) -> list:
    """Fetch *all* Observations for a Datastream using paging."""
    observations = []
    skip = 0
    while True:
        url = f"/Datastreams({datastream_id})/Observations?$top={page_size}&$skip={skip}&$orderby=phenomenonTime asc"
        page = get_api_data(url)["value"]
        if not page:
            break
        observations.extend(page)
        skip += page_size
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
            observations = fetch_all_observations(ds_id)

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
        observations = fetch_all_observations(ds_id)
        for obs in tqdm(observations, desc=f"Datastream {ds_id} Observations", leave=False):
            obs_id = obs["@iot.id"]

            # FeatureOfInterest
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
    print(f"\nFinished populating Thing {thing_id}.\n")

if __name__ == "__main__":
    create_db()  # ensure schema exists

    numbered_list = list_things()

    while True:
        choice = input("\nEnter number of Location to populate (or 'q' to quit): ").strip()
        if choice.lower() == "q":
            print("Goodbye!")
            break

        if not choice.isdigit() or not (1 <= int(choice) <= len(numbered_list)):
            print("Invalid choice. Please enter a valid number.")
            continue

        index = int(choice) - 1
        thing_id, thing_name, loc_name = numbered_list[index]

        print(f"\nPopulating Thing {thing_id} → {thing_name} → {loc_name}\n")
        populate_single_thing(thing_id)


