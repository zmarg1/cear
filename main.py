import sqlite3
import requests
import json
from tqdm import tqdm

DB_PATH = "cear.db"
BASE_URL = "https://api.sealevelsensors.org/v1.0"

# Step 1: Create database and tables
def create_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS api_locations (
        location_id INTEGER PRIMARY KEY,
        name TEXT,
        description TEXT,
        latitude REAL,
        longitude REAL
    );

    CREATE TABLE IF NOT EXISTS api_sensors (
        sensor_id INTEGER PRIMARY KEY,
        name TEXT,
        description TEXT,
        location_id INTEGER,
        FOREIGN KEY (location_id) REFERENCES api_locations(location_id)
    );

    CREATE TABLE IF NOT EXISTS api_datastreams (
        datastream_id INTEGER PRIMARY KEY,
        sensor_id INTEGER,
        name TEXT,
        unit_of_measurement TEXT,
        notes TEXT,
        FOREIGN KEY (sensor_id) REFERENCES api_sensors(sensor_id)
    );

    CREATE TABLE IF NOT EXISTS api_observations (
        observation_id INTEGER PRIMARY KEY,
        datastream_id INTEGER,
        result_time TEXT,
        result REAL,
        sensor_id INTEGER,
        FOREIGN KEY (datastream_id) REFERENCES api_datastreams(datastream_id),
        FOREIGN KEY (sensor_id) REFERENCES api_sensors(sensor_id)
    );
    """)
    conn.commit()
    conn.close()

# Step 2: Fetch all sensors
def fetch_sensors():
    response = requests.get(f"{BASE_URL}/Things?$top=1000")
    response.raise_for_status()
    return response.json()["value"]

# Step 3: Fetch location info for a sensor
def fetch_location(sensor_id):
    response = requests.get(f"{BASE_URL}/Things({sensor_id})/Locations")
    response.raise_for_status()
    locations = response.json()["value"]
    if not locations:
        return None
    loc = locations[0]
    coordinates = loc.get("location", {}).get("coordinates", [None, None])
    return {
        "location_id": loc["@iot.id"],
        "name": loc["name"],
        "description": loc["description"],
        "latitude": coordinates[1],
        "longitude": coordinates[0]
    }

# Step 4: Fetch datastreams for a sensor
def fetch_datastreams(sensor_id):
    response = requests.get(f"{BASE_URL}/Things({sensor_id})/Datastreams")
    response.raise_for_status()
    return response.json()["value"]

# Step 5: Fetch latest 100 observations for a datastream
def fetch_observations(datastream_id, sensor_id):
    url = f"{BASE_URL}/Datastreams({datastream_id})/Observations?$select=resultTime,result&$top=100"
    response = requests.get(url)
    response.raise_for_status()
    return [
        {
            "datastream_id": datastream_id,
            "sensor_id": sensor_id,
            "result_time": obs["resultTime"],
            "result": obs["result"]
        }
        for obs in response.json().get("value", [])
    ]

# Step 6: Populate database
def populate_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    sensors = fetch_sensors()

    for sensor in tqdm(sensors, desc="Processing sensors"):
        sensor_id = sensor["@iot.id"]
        sensor_name = sensor["name"]
        description = sensor["description"]

        # Location
        loc = fetch_location(sensor_id)
        if loc:
            c.execute("""INSERT OR IGNORE INTO api_locations 
                         (location_id, name, description, latitude, longitude)
                         VALUES (?, ?, ?, ?, ?)""",
                      (loc["location_id"], loc["name"], loc["description"], loc["latitude"], loc["longitude"]))
            location_id = loc["location_id"]
        else:
            location_id = None

        # Sensor
        c.execute("""INSERT OR IGNORE INTO api_sensors 
                     (sensor_id, name, description, location_id)
                     VALUES (?, ?, ?, ?)""",
                  (sensor_id, sensor_name, description, location_id))

        # Datastreams
        datastreams = fetch_datastreams(sensor_id)
        for ds in datastreams:
            ds_id = ds["@iot.id"]
            ds_name = ds["name"]
            unit = ds.get("unitOfMeasurement", {}).get("name", None)
            notes = ds.get("description", "")

            c.execute("""INSERT OR IGNORE INTO api_datastreams 
                         (datastream_id, sensor_id, name, unit_of_measurement, notes)
                         VALUES (?, ?, ?, ?, ?)""",
                      (ds_id, sensor_id, ds_name, unit, notes))

            # Observations (just 100 for now)
            observations = fetch_observations(ds_id, sensor_id)
            for obs in observations:
                c.execute("""INSERT OR IGNORE INTO api_observations 
                             (datastream_id, sensor_id, result_time, result)
                             VALUES (?, ?, ?, ?)""",
                          (obs["datastream_id"], obs["sensor_id"], obs["result_time"], obs["result"]))

        conn.commit()
    conn.close()

if __name__ == "__main__":
    create_db()
    populate_db()
