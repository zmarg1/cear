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
