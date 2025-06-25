# CEAR Hub Sensor Database CLI

## Description 

The **CEAR Hub Sensor Database CLI** is a Python command-line interface that connects to the Smart Sea Level Sensors API and ingests environmental sensor data into a structured PostgreSQL (e.g., Supabase) database. It supports full schema creation, observation insertion, and routine updating.

## Installation 

1. **Clone the repository**:

```bash
    git clone https://github.com/zmarg1/cear.git
    cd cear
```

2. **Create a virtual environment**:

```bash
    python -m venv venv
    source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. **Install dependencies:**:

```bash
    pip install -r requirements.txt
```

4. **Set up environment variables**:

Create a .env file with a URL for the Supabase Database. See .env.example for the correct format.

You will need a password to access the database. You may request a password by emailing zmarg@gatech.edu


## Usage

Run the main CLI:

```bash
    python main.py
```

You will be presented with a menu:

```bash
    Welcome to the CEAR Hub Database CLI

    Options:
    V → View database contents
    L → List available Things in API
    P → Add Thing to the DB
    U → Update DB Datastream
    C → Check Datastream Details
    D → Delete Thing from DB
    Q → Quit
```

For example, you can type "V" in the terminal to preview the database contents. 

```bash
    Enter choice: V

    --- Available Tables --- 
    1. features_of_interest (4 rows)
    2. historical_location_locations (6 rows)
    3. historical_locations (6 rows)
    4. locations (5 rows)
    5. observations (4002 rows)
    6. observed_properties (1 rows)
    7. sensors (1 rows)
    8. thing_locations (4 rows)
    9. things (4 rows)
    Q. Return to main menu

    Enter table number to view, or 'Q' to return:
```

## Contributing 

Pull requests are welcome. For major changes, please open an issue first to discuss what you’d like to change.

## Authors and Acknowledgment

Developed by Zach Margulies (zmarg@gatech.edu) as part of the CEAR Hub.
