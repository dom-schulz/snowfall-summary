import psycopg2
from datetime import datetime, timedelta, timezone
from openmeteopy import OpenMeteo
from openmeteopy.daily import DailyHistorical
from openmeteopy.options import HistoricalOptions
import pandas as pd
import openrouteservice

def get_connection(config):
    return psycopg2.connect(**config)


def fetch_weather_data(resort_id, lat, lon):
    '''
    Fetch the weather data for the resort from the OpenMeteo API.
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

    data = data.reset_index() # saves the index (datetime) as a column

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
        CREATE TABLE IF NOT EXISTS {WEATHER_TABLE} (
            id INTEGER,
            time DATE,
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

    for _, row in resorts.iterrows():
        resort_id, lat, lon = row['id'], row['latitude'], row['longitude']
        cursor.execute("SELECT COUNT(*) FROM historical_weather WHERE id = %s AND time >= %s", (resort_id, datetime.now(timezone.utc).date() - timedelta(days=90)))
        count = cursor.fetchone()[0]

        if count < 85:
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
     
            
def update_resorts(conn, cursor, RESORTS_TABLE):
    '''
    Update the resorts table with new data from the csv file.
    If the id is already in the table, update the row with the new data.
    If the id is not in the table, insert the row.
    '''
    
    resorts = pd.read_csv("final_resorts_us.csv")
    resorts = resorts[["id", "resort", "latitude", "longitude", "state"]]

    for _, row in resorts.iterrows():
        cursor.execute(
            f"""
            INSERT INTO {RESORTS_TABLE} (ID, Resort, Latitude, Longitude, State)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (ID) DO UPDATE
            SET Resort = EXCLUDED.Resort,
                Latitude = EXCLUDED.Latitude,
                Longitude = EXCLUDED.Longitude,
                State = EXCLUDED.State;
            """,
            tuple(row)
        )

    conn.commit()


def get_nearby_resorts_within_driving_distance(DB_CONFIG, ORS_API_KEY, user_lat, user_lon, max_miles):
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