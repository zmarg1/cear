import os
import sys
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
from tqdm import tqdm
import psycopg2
from dotenv import load_dotenv
from sqlalchemy import create_engine

# Global Constants
BASE_URL = "https://api.sealevelsensors.org/v1.0"
BATCH_SIZE = 1000

# Global session with retries and backoff
session = requests.Session()

retries = Retry(
    total=5,
    backoff_factor=1,
    status_forcelist=[500, 502, 503, 504, 429],
    allowed_methods=["HEAD", "GET", "OPTIONS"],
    raise_on_status=False
)

adapter = HTTPAdapter(max_retries=retries)
session.mount("http://", adapter)
session.mount("https://", adapter)

def get_db_url():
    load_dotenv()
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        raise ValueError("SUPABASE_DB_URL not set in .env file or environment.")
    return db_url

def get_connection():
    return psycopg2.connect(get_db_url())

def get_engine():
    return create_engine(get_db_url())

def clean_iso_datetime(ts):
    # Example input → '2025-06-09T18:22:37.983312' OR '2025-06-09TT18:22:37.983312'
    ts = ts.replace("TT", "T")  # fix any accidental double T
    if "." in ts:
        ts = ts.split(".")[0]
    if not ts.endswith("Z"):
        ts += "Z"
    return ts

def list_datastreams_for_thing(thing_id: int) -> list[tuple[int, str]]:
    """Return a numbered list of (datastream_id, datastream_name) for a Thing."""
    datastreams = fetch_datastreams(thing_id)
    numbered_list = []

    print(f"\nDatastreams for Thing {thing_id}:\n")
    for i, ds in enumerate(datastreams, start=1):
        ds_id = ds["@iot.id"]
        ds_name = ds.get("name", f"Datastream {ds_id}")
        print(f"{i:2}. {ds_name}")
        numbered_list.append((ds_id, ds_name))

    return numbered_list

def get_datastream_check(datastream_id: int, conn) -> dict:
    """
    Check observation ranges for Datastream and whether new observations exist.

    Returns a dict:
    {
        'oldest_api_time': str,
        'oldest_db_time' : str,
        'newest_api_time': str,
        'newest_db_time' : str,
        'up_to_date'     : bool,
        'new_obs_count'  : int
    }
    """
    # Step 1: Get oldest and newest API observation times
    try:
        oldest_api_obs = get_api_data(f"/Datastreams({datastream_id})/Observations?$orderby=phenomenonTime asc&$top=1")["value"]
        newest_api_obs = get_api_data(f"/Datastreams({datastream_id})/Observations?$orderby=phenomenonTime desc&$top=1")["value"]

        oldest_api_time = oldest_api_obs[0]["phenomenonTime"] if oldest_api_obs else "(no observations)"
        newest_api_time = newest_api_obs[0]["phenomenonTime"] if newest_api_obs else "(no observations)"
    except Exception as e:
        print(f"⚠️ Error fetching API range for Datastream {datastream_id}: {e}")
        oldest_api_time = newest_api_time = f"(error: {e})"

    # Step 2: Get oldest and newest DB observation times
    c = conn.cursor()
    c.execute("""
        SELECT MIN(phenomenon_time_start), MAX(phenomenon_time_start)
        FROM observations
        WHERE datastream_id = %s;
    """, (datastream_id,))
    result = c.fetchone()
    oldest_db_time, newest_db_time = result if result else (None, None)

    oldest_db_time = oldest_db_time if oldest_db_time else "(no observations)"
    newest_db_time = newest_db_time if newest_db_time else "(no observations)"

    # Step 3: Check if new observations exist
    up_to_date = False
    new_obs_count = -1  # default if unknown

    # Case 1: No DB observations → total API count = new_obs_count
    if isinstance(newest_db_time, str) and newest_db_time.startswith("("):
        try:
            url = f"{BASE_URL}/Datastreams({datastream_id})/Observations?$top=0&$count=true"
            response = requests.get(url)
            response.raise_for_status()
            count_response = response.json()

            total_api_count = count_response.get("@iot.count", -1)
            new_obs_count = total_api_count
            up_to_date = (total_api_count == 0)

        except Exception as e:
            print(f"⚠️ Could not fetch total observation count for Datastream {datastream_id}: {e}")
            new_obs_count = -1
            up_to_date = False

    # Case 2: API error → cannot check → assume not up to date
    elif isinstance(newest_api_time, str) and newest_api_time.startswith("("):
        up_to_date = False

    else:
        # Clean times for comparison
        clean_db_time = clean_iso_datetime(newest_db_time)
        clean_api_time = clean_iso_datetime(newest_api_time)

        # Short-circuit: if DB newest == API newest → up to date
        if clean_db_time == clean_api_time:
            up_to_date = True
            new_obs_count = 0
        else:
            try:
                #print(f"DEBUG: db_time_iso for fallback comparison = {clean_db_time}")

                # Get most recent observation (no $filter)
                url = f"{BASE_URL}/Datastreams({datastream_id})/Observations"
                params = {
                    "$top": 1,
                    "$orderby": "phenomenonTime desc"
                }

                full_url = requests.Request('GET', url, params=params).prepare().url
                #print(f"DEBUG: Fallback request URL (no filter) = {full_url}")

                response = requests.get(url, params=params)
                response.raise_for_status()
                result = response.json()
                api_latest_obs = result.get("value", [{}])[0].get("phenomenonTime", "")

                #print(f"DEBUG: Latest phenomenonTime from API = {api_latest_obs}")

                # Compare timestamps directly
                up_to_date = (api_latest_obs <= clean_db_time)
                new_obs_count = 1 if not up_to_date else 0

            except Exception as e:
                print(f"⚠️ Could not check new observations for Datastream {datastream_id}: {e}")
                up_to_date = False
                new_obs_count = -1

    c.close()

    # Final return dict
    return {
        "oldest_api_time": oldest_api_time,
        "oldest_db_time": oldest_db_time,
        "newest_api_time": newest_api_time,
        "newest_db_time": newest_db_time,
        "up_to_date": up_to_date,
        "new_obs_count": new_obs_count
    }

def get_api_data(path, timeout=10):
    """GET data from API with retries and timeout."""
    url = BASE_URL + path
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"⚠️ Error during API call: {url}\n→ {e}")
        raise

def get_datastream_metadata(ds_id: int) -> dict:
    """Fetch Datastream name, ObservedProperty, Sensor, and total observation count."""
    ds = fetch_datastream_full(ds_id)

    # Get ObservedProperty name
    op_link = ds["ObservedProperty@iot.navigationLink"]
    op = fetch_observed_property_from_link(op_link)

    # Get Sensor name
    sensor_link = ds["Sensor@iot.navigationLink"]
    sensor = fetch_sensor_from_link(sensor_link)

    # Get Observation count
    count_url = f"/Datastreams({ds_id})/Observations?$top=0&$count=true"
    count_response = get_api_data(count_url)
    obs_count = count_response.get("@iot.count", "?")  # fallback in case not returned

    return {
        "datastream_name": ds.get("name", f"Datastream {ds_id}"),
        "observed_property_name": op.get("name", "(unknown)"),
        "sensor_name": sensor.get("name", "(unknown)"),
        "observation_count": obs_count
    }

def safe_print(*args, **kwargs):
    tqdm.write(" ".join(str(a) for a in args), file=sys.stdout, **kwargs)

def parse_interval(val):
    if isinstance(val, dict):
        return val.get("start", None), val.get("end", None)
    elif isinstance(val, str):
        return val, None
    else:
        return None, None

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
        c.execute("""INSERT INTO features_of_interest 
                     (feature_of_interest_id, name, description, encoding_type, feature, properties)
                     VALUES (%s, %s, %s, %s, %s, %s)
                     ON CONFLICT (feature_of_interest_id) DO NOTHING""",
                  (foi["@iot.id"], foi["name"], foi.get("description", ""), foi["encodingType"],
                   json.dumps(foi["feature"]), json.dumps(foi.get("properties", {}))))

        # Observation
        phenomenon_start, phenomenon_end = parse_interval(obs.get("phenomenonTime"))
        phenomenon_start = clean_iso_datetime(phenomenon_start) if phenomenon_start else None
        phenomenon_end = clean_iso_datetime(phenomenon_end) if phenomenon_end else None
        valid_start, valid_end = parse_interval(obs.get("validTime"))

        c.execute("""INSERT INTO observations 
            (observation_id, datastream_id, phenomenon_time_start, phenomenon_time_end,
            result_time, result, result_quality, valid_time_start, valid_time_end, parameters, feature_of_interest_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (observation_id) DO NOTHING""",
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
            safe_print(f"Committed {count_since_commit} observations so far for Datastream {datastream_id}.")
            count_since_commit = 0

    # Final commit for any remaining
    if count_since_commit > 0:
        conn.commit()
        print(f"Final commit of {count_since_commit} observations for Datastream {datastream_id}.")

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

    conn = get_connection()
    c = conn.cursor()
    
    # Split SQL script into individual statements and execute one by one
    for statement in schema_sql.strip().split(';'):
        if statement.strip():
            c.execute(statement + ';')
    
    conn.commit()
    c.close()
    conn.close()
    #print("Database schema created successfully.")

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
def fetch_new_observations(datastream_id: int, conn, page_size=1000, limit=None, start_time=None) -> tuple[list, str]:
    """
    Fetch only new Observations for a Datastream:
    - Check the DB for latest phenomenon_time_start
    - Use $filter to fetch only newer observations
    - Return (observations, latest_db_time)
    """
    c = conn.cursor()
    c.execute("""
        SELECT MAX(phenomenon_time_start) 
        FROM observations 
        WHERE datastream_id = %s;
    """, (datastream_id,))
    result = c.fetchone()
    latest_time = result[0]

    if latest_time and "." in latest_time:
        latest_time = latest_time.split(".")[0] + "Z"

    if latest_time:
        safe_print(f"Datastream {datastream_id} → Latest DB time: {latest_time}")
    else:
        safe_print(f"Datastream {datastream_id} → No existing DB observations → fetching all.")

    observations = []
    skip = 0
    fetched = 0

    try:
        # --- NEW FAST FETCH LOGIC ---
        fast_mode = limit is not None and limit <= 10 * page_size
        switched_to_fallback = False

        if fast_mode:
            total_pages = (limit + page_size - 1) // page_size  # ceil division
            #safe_print(f"→ Using FAST MODE (limit {limit}, page_size {page_size}, total_pages {total_pages})")

            for page_num in range(total_pages):
                params = {
                    "$top": page_size,
                    "$skip": skip,
                    "$orderby": "phenomenonTime desc"
                }

                if start_time:
                    start_time_for_filter = start_time[:-1] if start_time.endswith("Z") else start_time
                    params["$filter"] = f"phenomenonTime ge datetime'{start_time_for_filter}'"
                elif latest_time:
                    latest_time_for_filter = latest_time[:-1] if latest_time.endswith("Z") else latest_time
                    params["$filter"] = f"phenomenonTime gt datetime'{latest_time_for_filter}'"

                url = f"{BASE_URL}/Datastreams({datastream_id})/Observations"

                response = requests.get(url, params=params)
                response.raise_for_status()
                page = response.json()["value"]
                #safe_print(f"Fetched page {page_num+1}/{total_pages} with {len(page)} observations (skip={skip})")

                # Check if API is enforcing a smaller page size
                if page_num == 0 and len(page) < page_size:
                    #safe_print(f"⚠️ API returned only {len(page)} obs (requested {page_size}) — switching to fallback mode.")
                    switched_to_fallback = True
                    break  # exit fast_mode loop → fallback will take over

                if not page:
                    break

                observations.extend(page)
                fetched += len(page)
                skip += len(page)

                if fetched >= limit:
                    return observations[:limit], latest_time

            # If fallback was triggered
            if switched_to_fallback:
                observations = fetch_all_observations(
                    datastream_id,
                    after_time=latest_time or start_time,
                    limit=limit
                )
                return observations, latest_time

            return observations[:limit], latest_time
        # --- END FAST FETCH LOGIC ---

        # Fallback to full loop (normal case)
        observations = fetch_all_observations(
            datastream_id,
            after_time=latest_time or start_time,
            limit=limit
        )
        return observations, latest_time

    except requests.exceptions.HTTPError as e:
        if e.response.status_code in [400, 500]:
            # fallback again using correct after_time
            observations = fetch_all_observations(
                datastream_id,
                after_time=latest_time or start_time,
                limit=limit
            )
        else:
            raise  # Re-raise other errors
        
    return observations, latest_time

def fetch_all_observations(datastream_id: int, page_size=1000, after_time=None, limit=None) -> list:
    """
    Fetch Observations for a Datastream using paging.
    Optionally fetch only Observations after a given 'after_time'.
    If 'limit' is provided, stop after 'limit' Observations.
    """
    observations = []
    skip = 0
    fetched = 0

    pbar = tqdm(desc=f"Datastream {datastream_id} Fallback paging", unit="obs")

    while True:
        params = {
            "$top": page_size,
            "$skip": skip,
            "$orderby": "phenomenonTime desc"
        }

        url = f"{BASE_URL}/Datastreams({datastream_id})/Observations"

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            page = response.json()["value"]
            #safe_print(f"Fetched page with {len(page)} observations (skip={skip})")
            #if len(page) < page_size:
                #safe_print(f"⚠️ API returned only {len(page)} obs (requested {page_size}) — server-side page size limit likely in effect.")

        except Exception as e:
            print(f"⚠️ Error during fallback paging for Datastream {datastream_id}: {e}")
            break

        if not page:
            break

        for obs in page:
            obs_time = obs["phenomenonTime"]

            # If after_time is provided, stop early if we reach older observations
            if after_time and obs_time < after_time:
                print(f"→ Reached observation older than after_time {after_time} → stopping fallback early.")
                pbar.close()
                return observations

            observations.append(obs)
            fetched += 1

            if limit and fetched >= limit:
                pbar.close()
                return observations

        skip += len(page)
        pbar.update(len(page))

    pbar.close()
    return observations

def parse_time_range(field):
    """Parse start/end time range."""
    if isinstance(field, str) and "/" in field:
        start, end = field.split("/")
        return start, end
    return None, None

def list_things(show_output=True) -> list:
    things = get_api_data("/Things?$expand=Locations")["value"]

    numbered_list = []

    if show_output:
        print("\nAvailable Things by Location:")

    for thing in things:
        thing_id = thing["@iot.id"]
        thing_name = thing["name"]
        locations = thing.get("Locations", [])
        if locations:
            for loc in locations:
                loc_name = loc["name"]
                numbered_list.append((thing_id, thing_name, loc_name))
                if show_output:
                    print(f"{len(numbered_list):2}. {thing_name} → {loc_name}")
        else:
            numbered_list.append((thing_id, thing_name, "(No Location)"))
            if show_output:
                print(f"{len(numbered_list):2}. {thing_name} → (No Location)")

    return numbered_list

def get_datastream_time_range(datastream_id: int) -> tuple[str, str]:
    """Return (oldest_observation_time, newest_observation_time) for a Datastream."""
    try:
        oldest_api_obs = get_api_data(f"/Datastreams({datastream_id})/Observations?$orderby=phenomenonTime asc&$top=1")["value"]
        newest_api_obs = get_api_data(f"/Datastreams({datastream_id})/Observations?$orderby=phenomenonTime desc&$top=1")["value"]

        oldest_api_time = oldest_api_obs[0]["phenomenonTime"] if oldest_api_obs else None
        newest_api_time = newest_api_obs[0]["phenomenonTime"] if newest_api_obs else None

        # Clean both (safe — avoids TT or microseconds when printing)
        oldest_api_time = clean_iso_datetime(oldest_api_time) if oldest_api_time else None
        newest_api_time = clean_iso_datetime(newest_api_time) if newest_api_time else None

        return oldest_api_time, newest_api_time
    except Exception as e:
        print(f"⚠️ Error fetching time range for Datastream {datastream_id}: {e}")
        return None, None

def populate_single_thing(thing_id: int):
    """Populate database for a single Thing (by thing_id)."""

    print(f"\nPopulating Thing {thing_id} - {thing_id}")

    conn = get_connection()
    c = conn.cursor()

    # Fetch Datastreams
    datastreams = fetch_datastreams(thing_id)

    # Build list of (ds_id, ds_name, outstanding_obs)
    ds_info_list = []

    print("\nChecking outstanding observations for Datastreams...")

    for ds in tqdm(datastreams, desc=f"Thing {thing_id} Datastreams", leave=False):
        ds_id = ds["@iot.id"]
        ds_name = ds.get("name", f"Datastream {ds_id}")

        # Get total API obs count
        count_url = f"/Datastreams({ds_id})/Observations?$top=0&$count=true"
        try:
            count_response = get_api_data(count_url)
            total_api_count = count_response.get("@iot.count", "?")
        except Exception as e:
            print(f"⚠️ Could not fetch observation count for Datastream {ds_id}: {e}")
            total_api_count = "?"

        # Get total DB obs count
        c.execute("""SELECT COUNT(*) FROM observations WHERE datastream_id = %s;""", (ds_id,))
        existing_obs_count = c.fetchone()[0]

        # Compute outstanding observations
        if isinstance(total_api_count, int):
            outstanding_obs = total_api_count - existing_obs_count
            outstanding_obs = max(outstanding_obs, 0)
        else:
            outstanding_obs = "?"

        ds_info_list.append((ds_id, ds_name, outstanding_obs))

    c.close()
    conn.close()

    # Print menu
    print(f"\nThing {thing_id} → Populating available Datastreams:\n")

    print("Available Datastreams:")
    for i, (ds_id, ds_name, outstanding_obs) in enumerate(ds_info_list, start=1):
        obs_str = f"{outstanding_obs}" if isinstance(outstanding_obs, int) else "(unknown)"
        print(f"{i:2}. {ds_name} → {obs_str} outstanding observations")

    # Ask which Datastream(s) to populate
    ds_choice = input("\nEnter Datastream number to populate, 'A' to populate ALL, or 'Q' to cancel: ").strip().lower()

    if ds_choice == "q":
        print("Cancelled populate.")
        return
    elif ds_choice == "a":
        ds_indexes = range(len(ds_info_list))
    elif ds_choice.isdigit() and (1 <= int(ds_choice) <= len(ds_info_list)):
        ds_indexes = [int(ds_choice) - 1]
    else:
        print("Invalid choice.")
        return

    # Now proceed to populate selected Datastream(s)
    conn = get_connection()
    c = conn.cursor()

    try:
        # Fetch the Thing
        url = f"/Things({thing_id})"
        thing = get_api_data(url)

        c.execute("""INSERT INTO things 
                    (thing_id, name, description, properties)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (thing_id) DO NOTHING""",
                (thing_id, thing["name"], thing.get("description", ""), 
                json.dumps(thing.get("properties", {}))))

        # Locations
        locations = fetch_locations(thing_id)
        for loc in tqdm(locations, desc=f"Thing {thing_id} Locations"):
            location_id = loc["@iot.id"]
            c.execute("""INSERT INTO locations 
                        (location_id, name, description, encoding_type, location, properties)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (location_id) DO NOTHING""",
                    (location_id, loc["name"], loc.get("description", ""), loc["encodingType"],
                    json.dumps(loc["location"]), json.dumps(loc.get("properties", {}))))
            c.execute("""INSERT INTO thing_locations 
                        (thing_id, location_id)
                        VALUES (%s, %s)
                        ON CONFLICT (thing_id) DO NOTHING""",
                    (thing_id, location_id))

        # HistoricalLocations
        historical_locations = fetch_historical_locations(thing_id)
        for hl in tqdm(historical_locations, desc=f"Thing {thing_id} HistoricalLocations"):
            hl_id = hl["@iot.id"]
            c.execute("""INSERT INTO historical_locations 
                        (historical_location_id, thing_id, time)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (historical_location_id) DO NOTHING""",
                    (hl_id, thing_id, hl["time"]))

            for loc in hl.get("Locations", []):
                location_id = loc["@iot.id"]
                c.execute("""INSERT INTO locations 
                            (location_id, name, description, encoding_type, location, properties)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            ON CONFLICT (location_id) DO NOTHING""",
                        (location_id, loc["name"], loc.get("description", ""), loc["encodingType"],
                        json.dumps(loc["location"]), json.dumps(loc.get("properties", {}))))
                c.execute("""INSERT INTO historical_location_locations 
                            (historical_location_id, location_id)
                            VALUES (%s, %s)
                            ON CONFLICT (historical_location_id) DO NOTHING""",
                        (hl_id, location_id))

        # Now process selected Datastream(s)
        for ds_index in ds_indexes:
            ds_id, ds_name, _ = ds_info_list[ds_index]

            print(f"\n--- Processing Datastream {ds_id} ---")

            # Get metadata
            meta = get_datastream_metadata(ds_id)

            # Count existing observations in DB
            c.execute("""SELECT COUNT(*) FROM observations WHERE datastream_id = %s;""", (ds_id,))
            existing_obs_count = c.fetchone()[0]

            # Print clean summary
            print(f"Name: {meta['datastream_name']}")
            print(f"Observed Property: {meta['observed_property_name']}")
            print(f"Sensor: {meta['sensor_name']}")
            print(f"Total Observations in API: {meta['observation_count']}")
            print(f"Total Observations already in DB: {existing_obs_count}")

            # Show observation time range
            oldest_api_time, newest_api_time = get_datastream_time_range(ds_id)
            print(f"Oldest observation in API    : {oldest_api_time or '(no observations)'}")
            print(f"Most recent observation in API: {newest_api_time or '(no observations)'}")

            # Show DB time range + outstanding obs from full check
            range_info = get_datastream_check(ds_id, conn)

            print(f"Oldest observation in DB     : {range_info['oldest_db_time']}")
            print(f"Most recent observation in DB : {range_info['newest_db_time']}")

            # Ask user whether to fetch this Datastream
            while True:
                user_input = input("Fetch this Datastream? (y/n): ").strip().lower()
                if user_input in ("y", "n"):
                    break
                print("Please enter 'y' or 'n'.")

            if user_input == "n":
                print(f"Skipping Datastream {ds_id}.")
                continue  # skip this datastream

            # Ask how far back to go
            start_back_input = input(f"How far back in time do you want to go? (Enter YYYY-MM-DD, Enter for ALL): ").strip()
            if start_back_input:
                start_time = start_back_input + "T00:00:00Z"
            else:
                start_time = None

            # Ask how many obs toward present to fetch
            obs_limit_input = input("What is the maximum number of observations you want to collect? (Enter for ALL): ").strip()
            obs_limit = int(obs_limit_input) if obs_limit_input.isdigit() else None

            print(f"→ Will fetch up to {obs_limit if obs_limit else 'ALL'} observations\n")

            # Now proceed — Fetch full Datastream (required for DB insert)
            ds_full = fetch_datastream_full(ds_id)
            sensor_link = ds_full["Sensor@iot.navigationLink"]
            op_link = ds_full["ObservedProperty@iot.navigationLink"]

            # Fetch new observations ONCE
            observations, latest_db_time = fetch_new_observations(ds_id, conn, limit=obs_limit, start_time=start_time)

            # Print latest time AFTER fetch — this is perfectly fine
            #safe_print(f"Latest Observation in DB after fetch: {latest_db_time}")

            # Sensor
            sensor = fetch_sensor_from_link(sensor_link)
            c.execute("""INSERT INTO sensors 
                        (sensor_id, name, description, encoding_type, metadata, properties)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sensor_id) DO NOTHING""",
                    (sensor["@iot.id"], sensor["name"], sensor.get("description", ""),
                    sensor["encodingType"], sensor.get("metadata", ""), json.dumps(sensor.get("properties", {}))))

            # ObservedProperty
            op = fetch_observed_property_from_link(op_link)
            c.execute("""INSERT INTO observed_properties 
                        (observed_property_id, name, description, definition, properties)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (observed_property_id) DO NOTHING""",
                    (op["@iot.id"], op["name"], op.get("description", ""), op["definition"], 
                    json.dumps(op.get("properties", {}))))

            # Insert Datastream
            uom = ds_full.get("unitOfMeasurement", {})
            pheno_start, pheno_end = parse_time_range(ds_full.get("phenomenonTime", ""))
            result_start, result_end = parse_time_range(ds_full.get("resultTime", ""))

            c.execute("""INSERT INTO datastreams 
                        (datastream_id, thing_id, sensor_id, observed_property_id, name, description, observation_type,
                        unit_of_measurement_name, unit_of_measurement_symbol, unit_of_measurement_definition,
                        observed_area, phenomenon_time_start, phenomenon_time_end, result_time_start, result_time_end, properties)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (datastream_id) DO NOTHING""",
                    (ds_id, thing_id, sensor["@iot.id"], op["@iot.id"], ds_full["name"], ds_full.get("description", ""),
                    ds_full["observationType"], uom.get("name", ""), uom.get("symbol", ""), uom.get("definition", ""),
                    json.dumps(ds_full.get("observedArea", None)),
                    pheno_start, pheno_end, result_start, result_end,
                    json.dumps(ds_full.get("properties", {}))))

            # Insert observations — reuse previously fetched observations
            insert_observations(conn, c, ds_id, observations, batch_size=BATCH_SIZE)

            print(f"Finished Datastream {ds_id}.")

    except Exception as e:
        print(f"Error while populating Thing {thing_id}: {e}")
        conn.rollback()  # rollback if something failed

    finally:
        # Always close connection
        c.close()
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
            WHERE datastream_id = %s;
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


def view_db():
    engine = get_engine()
    conn = engine.raw_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name;
    """)
    tables = cur.fetchall()  # List of tuples like [('things',), ('locations',)]

    while True:
        print("\n--- Available Tables ---")
        for i, (table,) in enumerate(tables, start=1):
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"{i:2}. {table} ({count} rows)")
        print(" Q. Return to main menu")

        choice = input("\nEnter table number to view, or 'Q' to return: ").strip().lower()

        if choice == "q":
            break
        if not choice.isdigit() or not (1 <= int(choice) <= len(tables)):
            print("Invalid choice. Please enter a valid number.")
            continue

        index = int(choice) - 1
        table = tables[index][0]  # Extract table name from tuple

        print(f"\n--- Preview: {table} ---")
        df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 10", engine)
        print(df.to_string(index=False))

    cur.close()
    conn.close()


def delete_thing(thing_id: int):
    """Delete a Thing and all related data from the database."""

    conn = get_connection()
    c = conn.cursor()

    print(f"\nDeleting Thing {thing_id} and all related data...")

    # Delete Observations (must be first due to FK constraints)
    c.execute("""
        DELETE FROM observations 
        WHERE datastream_id IN (
            SELECT datastream_id FROM datastreams WHERE thing_id = %s
        )
    """, (thing_id,))

    # Delete Datastreams
    c.execute("DELETE FROM datastreams WHERE thing_id = %s", (thing_id,))

    # Delete HistoricalLocation-Location links
    c.execute("""
        DELETE FROM historical_location_locations
        WHERE historical_location_id IN (
            SELECT historical_location_id FROM historical_locations WHERE thing_id = %s
        )
    """, (thing_id,))

    # Delete HistoricalLocations
    c.execute("DELETE FROM historical_locations WHERE thing_id = %s", (thing_id,))

    # Delete Thing-Location links
    c.execute("DELETE FROM thing_locations WHERE thing_id = %s", (thing_id,))

    # Finally delete the Thing
    c.execute("DELETE FROM things WHERE thing_id = %s", (thing_id,))

    conn.commit()
    c.close()
    conn.close()

    print(f"Thing {thing_id} deleted from database.\n")


def display_numbered_list(numbered_list):
    print("\nAvailable Things by Location:")
    for i, (thing_id, thing_name, loc_name) in enumerate(numbered_list, start=1):
        print(f"{i:2}. {thing_name} → {loc_name}")


def update_datastreams(thing_id: int):
    """Update observations for a selected Thing and selected Datastream(s) (menu U)."""
    print(f"\nUpdating Observations for Thing {thing_id} → ", end="")
    conn = get_connection()
    c = conn.cursor()

    # Get Thing name and Location for display
    thing = get_api_data(f"/Things({thing_id})")
    thing_name = thing.get("name", f"Thing {thing_id}")
    locations = fetch_locations(thing_id)
    loc_name = locations[0]["name"] if locations else "(No Location)"
    print(f"{thing_name} → {loc_name}\n")

    # List Datastreams for the Thing
    ds_numbered_list = list_datastreams_for_thing(thing_id)

    # Ask which Datastream to update
    print("\nEnter Datastream number to update, or 'A' to update ALL Datastreams.")
    ds_choice = input("Your choice: ").strip().lower()

    if ds_choice == "a":
        ds_indexes = range(len(ds_numbered_list))  # all datastreams
    elif ds_choice.isdigit() and (1 <= int(ds_choice) <= len(ds_numbered_list)):
        ds_indexes = [int(ds_choice) - 1]  # single datastream
    else:
        print("Invalid choice.")
        c.close()
        conn.close()
        return

    for ds_index in ds_indexes:
        ds_id, ds_name = ds_numbered_list[ds_index]

        print(f"\n→ Checking Datastream {ds_id} → {ds_name}...\n")
        range_info = get_datastream_check(ds_id, conn)

        print(f"Oldest observation in API    : {range_info['oldest_api_time']}")
        print(f"Oldest observation in DB     : {range_info['oldest_db_time']}")
        print(f"Most recent observation in API: {range_info['newest_api_time']}")
        print(f"Most recent observation in DB : {range_info['newest_db_time']}")

        if range_info["up_to_date"]:
            print(f"→ Datastream {ds_id} is already UP TO DATE.")
            print(f"New observations available   : 0\n")
            continue

        new_obs = range_info["new_obs_count"]
        if new_obs >= 0:
            print(f"→ Datastream {ds_id} is NOT up to date.")
            print(f"New observations available   : {new_obs}")
        else:
            print(f"→ Datastream {ds_id} is NOT up to date.")
            print(f"New observations available   : (unknown)")

        # Ask user whether to proceed with update
        while True:
            user_input = input("Fetch and insert new observations? (y/n): ").strip().lower()
            if user_input in ("y", "n"):
                break
            print("Please enter 'y' or 'n'.")

        if user_input == "n":
            print(f"Skipping update for Datastream {ds_id}.\n")
            continue

        # --- Fetch and insert new observations ---
        limit = range_info["new_obs_count"] if range_info["new_obs_count"] > 0 else 1000
        observations, latest_db_time_after = fetch_new_observations(ds_id, conn, limit=limit)

        print(f"Latest Observation in DB before update: {range_info['newest_db_time']}")
        if not observations:
            print(f"No new observations for Datastream {ds_id}. Skipping insert.\n")
            continue

        insert_observations(conn, c, ds_id, observations, batch_size=BATCH_SIZE)
        print(f"Finished updating Datastream {ds_id}.\n")

    c.close()
    conn.close()


if __name__ == "__main__":
    create_db()
    print("Welcome to the CEAR Hub Database CLI")

    numbered_list = list_things(show_output=False)

    while True:
        print("\nOptions:")
        print("  V        → View Database Contents")
        print("  L        → List Available Things in API")
        print("  P        → Add Thing to the DB")
        print("  U        → Update DB Datastream")
        print("  C        → Check Datastream Details")
        print("  D        → Delete Thing from DB")
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

            # List Datastreams
            ds_numbered_list = list_datastreams_for_thing(thing_id)

            # Prompt user to check one or all
            print("\nEnter Datastream number to check, or 'A' to check ALL Datastreams.")
            ds_choice = input("Your choice: ").strip().lower()

            conn = get_connection()

            if ds_choice == "a":
                print(f"\nChecking observation ranges and up-to-date status for Thing {thing_id} → {thing_name} → {loc_name}...\n")
                for i, (ds_id, ds_name) in enumerate(ds_numbered_list, start=1):
                    print(f"\nDatastream {ds_id} → {ds_name}")
                    range_info = get_datastream_check(ds_id, conn)

                    print(f"Oldest observation in API    : {range_info['oldest_api_time']}")
                    print(f"Oldest observation in DB     : {range_info['oldest_db_time']}")
                    print(f"Most recent observation in API: {range_info['newest_api_time']}")
                    print(f"Most recent observation in DB : {range_info['newest_db_time']}")

                    if range_info["up_to_date"]:
                        print(f"→ Datastream {ds_id} is UP TO DATE.")
                        print(f"New observations available   : 0")
                    else:
                        # If we know the count, show it cleanly
                        new_obs = range_info["new_obs_count"]
                        if new_obs >= 0:
                            print(f"→ Datastream {ds_id} is NOT up to date.")
                            print(f"New observations available   : {new_obs}")
                        else:
                            # Fallback if count not available
                            print(f"→ Datastream {ds_id} is NOT up to date.")
                            print(f"New observations available   : (unknown)")
            elif ds_choice.isdigit() and (1 <= int(ds_choice) <= len(ds_numbered_list)):
                ds_index = int(ds_choice) - 1
                ds_id, ds_name = ds_numbered_list[ds_index]

                print(f"\nChecking Datastream {ds_id} → {ds_name}...\n")
                range_info = get_datastream_check(ds_id, conn)

                print(f"Oldest observation in API    : {range_info['oldest_api_time']}")
                print(f"Oldest observation in DB     : {range_info['oldest_db_time']}")
                print(f"Most recent observation in API: {range_info['newest_api_time']}")
                print(f"Most recent observation in DB : {range_info['newest_db_time']}")

                if range_info["up_to_date"]:
                    print(f"→ Datastream {ds_id} is UP TO DATE.")
                    print(f"New observations available   : 0")
                else:
                    new_obs = range_info["new_obs_count"]
                    if new_obs >= 0:
                        print(f"→ Datastream {ds_id} is NOT up to date.")
                        print(f"New observations available   : {new_obs}")
                    else:
                        print(f"→ Datastream {ds_id} is NOT up to date.")
                        print(f"New observations available   : (unknown)")
            else:
                print("Invalid choice.")

            conn.close()
            continue

        elif choice == "u":
            # Ask which Thing to update
            update_choice = input("Enter number of Location / Thing to update: ").strip()
            if not update_choice.isdigit() or not (1 <= int(update_choice) <= len(numbered_list)):
                print("Invalid choice.")
                continue

            index = int(update_choice) - 1
            thing_id, thing_name, loc_name = numbered_list[index]
            update_datastreams(thing_id)
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
            numbered_list = list_things(show_output=True)
            continue
        elif choice == "p":
            # Ask which Thing to populate
            populate_choice = input("Enter number of Location / Thing to populate: ").strip()
            if not populate_choice.isdigit() or not (1 <= int(populate_choice) <= len(numbered_list)):
                print("Invalid choice.")
                continue

            index = int(populate_choice) - 1
            thing_id, thing_name, loc_name = numbered_list[index]

            print(f"\nPopulating Thing {thing_id} → {thing_name} → {loc_name}\n")
            populate_single_thing(thing_id)
            continue



