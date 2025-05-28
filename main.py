import os
import requests
import pandas as pd
import sqlite3
from tqdm import tqdm

DATA_URL = "https://www.dropbox.com/s/a3qivjdpc30aqg1/all_sensor_data.csv?dl=1"

def download_data():
    url = DATA_URL
    output_dir = "raw_data"
    output_filename = "all_sensor_data.csv"
    output_path = os.path.join(output_dir, output_filename)

    # Create the directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    print(f"Downloading from {url}")
    response = requests.get(url, stream=True)

    if response.status_code == 200:
        with open(output_path, 'wb') as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Download complete: {output_path}")
    else:
        print(f"Failed to download file. Status code: {response.status_code}")

def explore_data(csv_path):
    try:
        df = pd.read_csv(csv_path)
        print("Data loaded successfully.\n")

        print("First 5 rows:")
        print(df.head(), end="\n\n")

        print("DataFrame shape (rows, columns):")
        print(df.shape, end="\n\n")

        print("Column names:")
        print(df.columns.tolist(), end="\n\n")

        print("Missing values per column:")
        print(df.isnull().sum(), end="\n\n")

        print("Data types:")
        print(df.dtypes, end="\n\n")

        print("Descriptive statistics:")
        print(df.describe(include='all'), end="\n\n")

    except Exception as e:
        print(f"Failed to load or analyze CSV. Error: {e}")

def populate_database(csv_path="raw_data/all_sensor_data.csv", db_path="sea_level_data.db"):
    print("Loading data from CSV...")
    df = pd.read_csv(csv_path)

    # Convert timestamp and clean up
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df["sensor_id"] = df["ID"].astype(int)
    df["lat"] = df["lat"].round(6)
    df["lon"] = df["lon"].round(6)

    # Clean sensors_df
    sensors_df = (
        df.groupby("sensor_id")[["desc", "lat", "lon"]]
        .first()
        .reset_index()
        .rename(columns={"desc": "description", "lat": "latitude", "lon": "longitude"})
    )

    # Connect to the SQLite DB
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Load existing sensor IDs from DB
    cursor.execute("SELECT sensor_id FROM sensors")
    existing_ids = set(row[0] for row in cursor.fetchall())

    # Filter new sensors only
    new_sensors_df = sensors_df[~sensors_df["sensor_id"].isin(existing_ids)]

    if not new_sensors_df.empty:
        new_sensors_df.to_sql("sensors", conn, if_exists="append", index=False)
        print(f"Inserted {len(new_sensors_df)} new sensors.")
    else:
        print("No new sensors to insert.")

    # Prepare and clean measurements
    measurements_df = df[["sensor_id", "timestamp", "water_level", "filtered_water_level"]]
    measurements_df.columns = ["sensor_id", "timestamp", "water_level", "filtered_level"]
    measurements_df = measurements_df.dropna(subset=["timestamp"])

    print(f"\nMeasurements to insert: {len(measurements_df)} rows.")

    # Insert measurements with progress bar
    print("Inserting measurements (this may take a while)...")
    chunksize = 50000
    total_chunks = (len(measurements_df) + chunksize - 1) // chunksize

    for i in tqdm(range(total_chunks), desc="Inserting measurements"):
        start = i * chunksize
        end = min(start + chunksize, len(measurements_df))
        measurements_df.iloc[start:end].to_sql("measurements", conn, if_exists="append", index=False)

    print("Measurements inserted successfully.")
    conn.close()

def reset_database(db_path="sea_level_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("DROP VIEW IF EXISTS daily_sensor_stats;")
    cursor.execute("DROP TABLE IF EXISTS measurements;")
    cursor.execute("DROP TABLE IF EXISTS sensors;")
    cursor.execute("DROP TABLE IF EXISTS events;")
    conn.commit()
    conn.close()
    print("Database tables dropped.")

def explore_sensor_conflicts(df):
    print("\nExploring sensor_id metadata conflicts...")

    grouped = df.groupby("ID")

    conflicting_sensors = []

    for sensor_id, group in grouped:
        unique_desc = group["desc"].nunique()
        unique_lat = group["lat"].nunique()
        unique_lon = group["lon"].nunique()

        if unique_desc > 1 or unique_lat > 1 or unique_lon > 1:
            conflicting_sensors.append({
                "sensor_id": int(sensor_id),
                "desc_count": unique_desc,
                "lat_count": unique_lat,
                "lon_count": unique_lon
            })

    if conflicting_sensors:
        print(f"Found {len(conflicting_sensors)} sensor_id(s) with conflicting metadata:")
        for conflict in conflicting_sensors:
            print(conflict)
    else:
        print("No sensor_id conflicts found. All metadata consistent.")

def get_conflicting_sensors(df):
    """Returns list of sensor IDs with >1 unique lat/lon value."""
    conflicting_ids = []

    grouped = df.groupby("ID")
    for sensor_id, group in grouped:
        if group["lat"].nunique() > 1 or group["lon"].nunique() > 1:
            conflicting_ids.append(int(sensor_id))  # Convert from float to int

    return conflicting_ids

def explor_location_differences(df, sensor_ids):
    """Prints distinct locations and water level stats for each conflicting sensor ID."""
    for sensor_id in sensor_ids:
        print(f"\nSensor ID: {sensor_id}")
        subset = df[df["ID"] == sensor_id]

        locs = subset[["lat", "lon"]].drop_duplicates()
        print(f"  Unique locations ({len(locs)}):")
        print(locs)

        level_stats = (
            subset.groupby(["lat", "lon"])["water_level"]
            .describe()[["mean", "std", "min", "max"]]
            .reset_index()
        )

        print("  Water level stats by location:")
        print(level_stats.round(3))

if __name__ == "__main__":
    sensor_data_path = os.path.join("raw_data", "all_sensor_data.csv")
    df = pd.read_csv(sensor_data_path)
    populate_database()

