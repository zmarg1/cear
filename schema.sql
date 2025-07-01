-- === CEAR Hub Database Schema for Supabase (Postgres) ===

CREATE TABLE things (
    thing_id INTEGER PRIMARY KEY,
    name TEXT,
    description TEXT,
    properties JSONB
);

CREATE TABLE locations (
    location_id INTEGER PRIMARY KEY,
    name TEXT,
    description TEXT,
    encoding_type TEXT,
    location JSONB,
    properties JSONB
);

CREATE TABLE thing_locations (
    thing_id INTEGER,
    location_id INTEGER,
    PRIMARY KEY (thing_id, location_id),
    FOREIGN KEY (thing_id) REFERENCES things(thing_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

CREATE TABLE historical_locations (
    historical_location_id INTEGER PRIMARY KEY,
    thing_id INTEGER,
    time TEXT,
    FOREIGN KEY (thing_id) REFERENCES things(thing_id)
);

CREATE TABLE historical_location_locations (
    historical_location_id INTEGER,
    location_id INTEGER,
    PRIMARY KEY (historical_location_id, location_id),
    FOREIGN KEY (historical_location_id) REFERENCES historical_locations(historical_location_id),
    FOREIGN KEY (location_id) REFERENCES locations(location_id)
);

CREATE TABLE sensors (
    sensor_id INTEGER PRIMARY KEY,
    name TEXT,
    description TEXT,
    encoding_type TEXT,
    metadata TEXT,
    properties JSONB
);

CREATE TABLE observed_properties (
    observed_property_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    definition TEXT,
    properties JSONB
);

CREATE TABLE datastreams (
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
    observed_area JSONB,
    phenomenon_time_start TEXT,
    phenomenon_time_end TEXT,
    result_time_start TEXT,
    result_time_end TEXT,
    properties JSONB,
    FOREIGN KEY (thing_id) REFERENCES things(thing_id),
    FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id),
    FOREIGN KEY (observed_property_id) REFERENCES observed_properties(observed_property_id)
);

CREATE TABLE features_of_interest (
    feature_of_interest_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    encoding_type TEXT NOT NULL,
    feature JSONB,
    properties JSONB
);

CREATE TABLE observations (
    observation_id INTEGER PRIMARY KEY,
    datastream_id INTEGER NOT NULL,
    phenomenon_time_start TEXT,
    phenomenon_time_end TEXT,
    result_time TEXT,
    result JSONB,
    result_navd88 NUMERIC(6,3),
    result_quality JSONB,
    valid_time_start TEXT,
    valid_time_end TEXT,
    parameters JSONB,
    feature_of_interest_id INTEGER NOT NULL,
    FOREIGN KEY (feature_of_interest_id) REFERENCES features_of_interest(feature_of_interest_id),
    FOREIGN KEY (datastream_id) REFERENCES datastreams(datastream_id)
);
