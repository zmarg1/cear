import sqlite3

def create_database(db_path="sea_level_data.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create sensors table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sensors (
        sensor_id INTEGER PRIMARY KEY,
        description TEXT NOT NULL,
        latitude FLOAT NOT NULL,
        longitude FLOAT NOT NULL
    );
    """)

    # Create measurements table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS measurements (
        measurement_id INTEGER PRIMARY KEY AUTOINCREMENT,
        sensor_id INTEGER NOT NULL,
        timestamp TIMESTAMP NOT NULL,
        water_level FLOAT NOT NULL,
        filtered_level FLOAT,
        FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id)
    );
    """)

    # Create events table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        start_time TIMESTAMP NOT NULL,
        end_time TIMESTAMP NOT NULL,
        notes TEXT
    );
    """)

    # Create daily_sensor_stats view (aggregated)
    cursor.execute("""
    CREATE VIEW IF NOT EXISTS daily_sensor_stats AS
    SELECT
        sensor_id,
        DATE(timestamp) AS date,
        COUNT(*) AS num_readings,
        AVG(water_level) AS avg_water_level,
        MIN(water_level) AS min_water_level,
        MAX(water_level) AS max_water_level
    FROM measurements
    GROUP BY sensor_id, DATE(timestamp);
    """)

    conn.commit()
    conn.close()
    print("✅ Database and tables created successfully.")

if __name__ == "__main__":
    create_database()
