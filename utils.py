from datetime import datetime, timedelta, timezone
from openmeteopy import OpenMeteo
from openmeteopy.daily import DailyHistorical
from openmeteopy.options import HistoricalOptions, ForecastOptions
import pandas as pd
import openrouteservice
import psycopg2
from psycopg2.extras import execute_values


def get_connection(config):
    '''
    Warning is thrown from pandas when using psycopg2. Is no concern with this simple application.
    '''
    return psycopg2.connect(**config)


def fetch_weather_data(resort_id, lat, lon):
    '''
    Fetch the weather data for specific resort from the OpenMeteo API.
    Returns a pandas dataframe with the weather data.
    Used to populate the historical_weather table.
    '''
    
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=90)

    options = HistoricalOptions(latitude=lat, longitude=lon, start_date=str(start_date), end_date=str(end_date))
    mgr = OpenMeteo(options, daily=DailyHistorical().all())
    data = mgr.get_pandas()

    if data.empty:
        return pd.DataFrame()

    data = data.reset_index() # saves the index (time) as a column

    data['id'] = resort_id
    data['time'] = pd.to_datetime(data['time']).dt.date
    return data[[
        'id', 'time', 'temperature_2m_max', 'temperature_2m_min',
        'apparent_temperature_max', 'apparent_temperature_min',
        'precipitation_sum', 'precipitation_hours', 'snowfall_sum'
    ]]


def populate_weather_data(conn, cursor, WEATHER_TABLE):
    '''
    Populate the weather table with data from the resorts table.
    '''
    
    resorts = pd.read_sql("SELECT id, resort, latitude, longitude, state FROM resorts", conn)
    
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS historical_weather (
        id INTEGER REFERENCES resorts(id),
        time TIMESTAMP NOT NULL,
        temperature_2m_max REAL,
        temperature_2m_min REAL,
        apparent_temperature_max REAL,
        apparent_temperature_min REAL,
        precipitation_sum REAL,
        precipitation_hours REAL,
        snowfall_sum REAL,
        PRIMARY KEY (id, time)
    );
    """)
    
    conn.commit()

    # Check if the resort has 90 days of data
    for _, row in resorts.iterrows():
        resort_id, lat, lon = row['id'], row['latitude'], row['longitude']
        cursor.execute(f"SELECT COUNT(*) FROM {WEATHER_TABLE} WHERE id = %s AND time >= %s", (resort_id, datetime.now(timezone.utc).date() - timedelta(days=90)))
        
        count = cursor.fetchone()[0]
        print(f'Count: {count}')

        if count < 90:
            df = fetch_weather_data(resort_id, lat, lon)
            
            for _, record in df.iterrows():
                cursor.execute(f"""
                    INSERT INTO {WEATHER_TABLE} (id, time, temperature_2m_max, temperature_2m_min,
                        apparent_temperature_max, apparent_temperature_min, precipitation_sum,
                        precipitation_hours, snowfall_sum)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (id, time) DO NOTHING;
                """, tuple(record))
                
                print(f'Executed: {record}')
            conn.commit()
     
            
def update_resorts(conn, cursor, RESORTS_TABLE, local_file):
    '''
    Update the resorts table with new data from the csv file.
    If the id is already in the table, update the row with the new data.
    If the id is not in the table, insert the row.
    '''

    # columns: id, resort, latitude, longitude, state
    resorts = pd.read_csv(local_file)

    insert_query = f"""
        INSERT INTO {RESORTS_TABLE} (id, resort, latitude, longitude, state)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
            resort = EXCLUDED.resort,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            state = EXCLUDED.state;
    """

    # Convert DataFrame to list of tuples
    values = list(resorts.itertuples(index=False, name=None))

    # Fast batch insert, via psycopg2.extras.execute_values
    # This is faster than the regular insert query, this is also PARAMETERIZED !
    execute_values(cursor, insert_query, values)

    conn.commit()


def get_nearby_resorts_within_driving_distance(DB_CONFIG, ORS_API_KEY, user_lat, user_lon, max_miles):
    '''
    Get the nearby resorts within a driving distance of the user.
    '''
    
    conn = get_connection(DB_CONFIG)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, resort, state, latitude, longitude FROM resorts")
    all_resorts = cursor.fetchall()
    
    coords = [(resort[4], resort[3]) for resort in all_resorts]  # (lon, lat)
    origin = (user_lon, user_lat)
    
    client = openrouteservice.Client(key=ORS_API_KEY)
    response = client.distance_matrix(
        locations=[origin] + coords,
        profile='driving-car',
        metrics=['distance', 'duration'],
        units='mi',
        sources=[0],
        destinations=list(range(1, len(coords) + 1))
    )
    
    distances = response['distances'][0]
    durations = response['durations'][0]
    nearby = []
    
    for i, (miles, seconds) in enumerate(zip(distances, durations)):
        if miles is not None and miles <= max_miles:
            nearby.append({
                "id": all_resorts[i][0],
                "resort": all_resorts[i][1],
                "state": all_resorts[i][2],
                "distance": miles,
                "duration_minutes": round(seconds / 60, 1)
            })
    
    # Get the forecast data for nearby resorts
    if nearby:
        nearby_ids = tuple([r["id"] for r in nearby])
        
        cursor.execute("SELECT MIN(time) FROM hourly")
        now = cursor.fetchone()[0]
        cutoff = now + timedelta(days=4)
        
        query = """
            SELECT h.id, SUM(h.snowfall) AS total_snowfall_next_4_days
            FROM hourly h
            WHERE h.id IN %s AND time >= %s AND time <= %s
            GROUP BY h.id
            ORDER BY total_snowfall_next_4_days DESC
        """
        cursor.execute(query, (nearby_ids, now.isoformat(), cutoff.isoformat()))
        snowfall_forecast = cursor.fetchall()
        
        # Add forecast to nearby resorts
        snowfall_dict = {resort_id: snowfall for resort_id, snowfall in snowfall_forecast}
        for resort in nearby:
            resort["forecast_snowfall"] = snowfall_dict.get(resort["id"], 0)
    
    cursor.close()
    conn.close()
    return nearby


# Format drive time for display in streamlit
def format_drive_time(minutes):
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours}h {mins}m" if hours else f"{mins}m"


def get_weather_data(resorts_df, weather_code_map, hourly_obj, daily_obj):
    '''
    This function is used to get the weather data for the resorts in the resorts_df dataframe.
    It returns a list of the hourly and daily dataframes.

    '''
    
    hourly_df = pd.DataFrame()
    daily_df = pd.DataFrame()
    
    for _, row in resorts_df.iterrows():
        # lat and long for current resort
        current_resort_id = row['id']
        latitude = row['latitude']
        longitude = row['longitude']

        print(f'Current resort: {row["resort"]}')
        print(f'Current resort id: {current_resort_id}')
        print(latitude, longitude)
        
        # Forecast options
        options = ForecastOptions(latitude, longitude)

        # Request OpenMeteo data for the current resort
        mgr = OpenMeteo(options, daily=daily_obj, hourly=hourly_obj)

        # Get the data as 2 pandas DataFrames (first is hourly, second is daily)
        meteo = mgr.get_pandas()  

        # Append rows of hourly and daily dataframes        
        for i, df in enumerate(meteo):
            
            # Save datetime and add id column to every entry
            df = df.reset_index() # saves the index (datetime) as a column
            df['id'] = current_resort_id 
            df = df[['id'] + [col for col in df.columns if col != 'id']]
            
            # Map the weather code to the weather description
            df["weather_description"] = df["weathercode"].map(weather_code_map)
            
            if i == 0:
                hourly_df = pd.concat([hourly_df, df])
            else:
                daily_df = pd.concat([daily_df, df])
                
    return hourly_df, daily_df


def insert_hourly_df(df, cursor, connection):
    print("Inserting hourly data...")
    
    # Drop and create table if it exists
    cursor.execute("DROP TABLE IF EXISTS hourly")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly (
        id INTEGER REFERENCES resorts(id),
        time TIMESTAMP NOT NULL,
        precipitation REAL,
        snowfall REAL,
        snow_height REAL,
        freezinglevel_height REAL,
        rain REAL,
        showers REAL,
        weathercode INTEGER,
        weather_description TEXT,
        PRIMARY KEY (id, time)
        );
    """)
    
    query = """
        INSERT INTO hourly (
            id, time, precipitation, snowfall, snow_height,
            freezinglevel_height, rain, showers,
            weathercode, weather_description
        )
        VALUES %s
        ON CONFLICT DO NOTHING;
    """
    # Convert DataFrame rows to list of tuples
    data = list(df.itertuples(index=False, name=None))

    # Bulk insert using execute_values
    execute_values(cursor, query, data)
    connection.commit()


def insert_daily_df(df, cursor, connection):
    print("Inserting daily data...")
    
    cursor.execute("DROP TABLE IF EXISTS daily")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily (
        ID INTEGER REFERENCES resorts(ID),
        time TIMESTAMP,
        windspeed_10m_max REAL,
        windgusts_10m_max REAL,
        winddirection_10m_dominant INTEGER,
        temperature_2m_max REAL,
        temperature_2m_min REAL,
        apparent_temperature_max REAL,
        apparent_temperature_min REAL,
        weathercode INTEGER,
        weather_description TEXT,
        PRIMARY KEY (id, time)
        );
    """)

    # Prepare the INSERT statement
    query = """
        INSERT INTO daily (
            id, time, windspeed_10m_max, windgusts_10m_max, winddirection_10m_dominant,
            temperature_2m_max, temperature_2m_min, apparent_temperature_max,
            apparent_temperature_min, weathercode, weather_description
        ) VALUES %s
        ON CONFLICT DO NOTHING;
    """

    # Convert DataFrame rows to list of tuples
    data = list(df.itertuples(index=False, name=None))

    # Bulk insert using execute_values
    execute_values(cursor, query, data)
    connection.commit()