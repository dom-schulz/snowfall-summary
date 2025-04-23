from openmeteopy import OpenMeteo
from openmeteopy.hourly import HourlyForecast
from openmeteopy.daily import DailyForecast
from openmeteopy.options import ForecastOptions
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

'''

This script is used to get the hourly weather data for a specific resort.
It is used to get the snowfall data for US Ski Resorts file.

This currently deletes the existing forecast tables and creates new ones.
    This improves the performance and allows for the data to be the most

'''

def get_weather_data(resorts_df, weather_code_map, hourly_obj, daily_obj, hourly_df, daily_df):
    '''
    This function is used to get the weather data for the resorts in the resorts_df dataframe.
    It returns a list of the hourly and daily dataframes.
    
    :param resorts_df: dataframe of the resorts
    :param weather_code_map: dictionary of the weather codes and their descriptions
    :param hourly_obj: hourly forecast object
    :param daily_obj: daily forecast object
    :param hourly_df: output dataframe of the hourly weather data
    :param daily_df: output dataframe of the daily weather data
    :return: list of the hourly and daily dataframes
    
    '''

    for i, row in resorts_df.iterrows():
        # lat and long for current resort
        current_resort_row = resorts_df[resorts_df['resort'] == row['resort']]
        current_resort_id = current_resort_row['id'].values[0]

        latitude = current_resort_row['latitude'].values[0]
        longitude = current_resort_row['longitude'].values[0]

        print(f'Current resort: {current_resort_row["resort"].values[0]}')
        print(f'Current resort id: {current_resort_id}')
        print(latitude, longitude)
        
        # Forecast options
        options = ForecastOptions(latitude, longitude)

        # Request OpenMeteo data for the current resort
        mgr = OpenMeteo(options, daily=daily_obj, hourly=hourly_obj)

        # Get the data as pandas DataFrames
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
                
    return [hourly_df, daily_df]

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
        weather_description TEXT
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
        weather_description TEXT
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


# ---------------------- Setup ---------------------- #
load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# OpenMeteo objects
hourly_obj = HourlyForecast()
daily_obj = DailyForecast()

# Set the hourly and daily forecast objects to include the desired data
hourly_obj = hourly_obj.precipitation().snowfall().snow_depth().freezinglevel_height().rain().Showers().weathercode()
daily_obj = daily_obj.windspeed_10m_max().windgusts_10m_max().winddirection_10m_dominant().temperature_2m_max()\
            .temperature_2m_min().apparent_temperature_max().apparent_temperature_min().weathercode()

# initialize output dataframes 
hourly_df = pd.DataFrame()
daily_df = pd.DataFrame()

# Map weather codes to their descriptions
weather_code_map = {
    0: "Clear sky",
    1: "Mainly clear",
    2: "Partly cloudy",
    3: "Overcast",
    45: "Fog and depositing rime fog",
    48: "Fog and depositing rime fog",
    51: "Drizzle: Light intensity",
    53: "Drizzle: Moderate intensity",
    55: "Drizzle: Dense intensity",
    56: "Freezing Drizzle: Light intensity",
    57: "Freezing Drizzle: Dense intensity",
    61: "Rain: Slight intensity",
    63: "Rain: Moderate intensity",
    65: "Rain: Heavy intensity",
    66: "Freezing Rain: Light intensity",
    67: "Freezing Rain: Heavy intensity",
    71: "Snow fall: Slight intensity",
    73: "Snow fall: Moderate intensity",
    75: "Snow fall: Heavy intensity",
    77: "Snow grains",
    80: "Rain showers: Slight",
    81: "Rain showers: Moderate",
    82: "Rain showers: Violent",
    85: "Snow showers: Slight",
    86: "Snow showers: Heavy",
    95: "Thunderstorm: Slight or moderate",
    96: "Thunderstorm: With Slight Hail",
    99: "Thunderstorm: With Heavy Hail"
}



# ------------------- Connect DB -------------------- #
try:
    conn = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    cur = conn.cursor()
    print("Connection successful!")
except Exception as e:
    print(f"Failed to connect: {e}")

resorts = pd.read_sql("SELECT * FROM resorts", conn)
print(resorts.head())

# ------------------- Upload Weather Data ------------------- #

# Get weather data
hourly_daily_dfs = get_weather_data(resorts, weather_code_map, hourly_obj, daily_obj, hourly_df, daily_df)

hourly_df = hourly_daily_dfs[0] 
daily_df = hourly_daily_dfs[1]
insert_hourly_df(hourly_df, cur, conn)
insert_daily_df(daily_df, cur, conn)

cur.close()
conn.close()



# ------------------- Local Testing ------------------- #

# # Save data frames
# hourly_daily_df[0].to_csv("meteo_hourly.csv", index=False)
# hourly_daily_df[1].to_csv("meteo_daily.csv", index=False)