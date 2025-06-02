CREATE TABLE api_locations (
    location_id INTEGER PRIMARY KEY,
    name TEXT,
    description TEXT,
    latitude REAL,
    longitude REAL
);

CREATE TABLE api_sensors (
    sensor_id INTEGER PRIMARY KEY,
    name TEXT,
    description TEXT,
    location_id INTEGER,
    FOREIGN KEY (location_id) REFERENCES api_locations(location_id)
);

CREATE TABLE api_datastreams (
    datastream_id INTEGER PRIMARY KEY,
    name TEXT,
    description TEXT,
    sensor_id INTEGER,
    unit_of_measurement TEXT,
    FOREIGN KEY (sensor_id) REFERENCES api_sensors(sensor_id)
);

CREATE TABLE api_observations (
    observation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    datastream_id INTEGER,
    sensor_id INTEGER,
    timestamp TEXT,
    result REAL,
    FOREIGN KEY (datastream_id) REFERENCES api_datastreams(datastream_id),
    FOREIGN KEY (sensor_id) REFERENCES api_sensors(sensor_id)
);
