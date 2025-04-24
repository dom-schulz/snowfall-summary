from openmeteopy import OpenMeteo
from openmeteopy.hourly import HourlyForecast
from openmeteopy.daily import DailyForecast
from openmeteopy.options import ForecastOptions
import pandas as pd
from utils import get_connection, get_weather_data, insert_hourly_df, insert_daily_df
from dotenv import load_dotenv
import os  

'''

This script is used to get the hourly weather data for a specific resort.
It is used to get the snowfall data for US Ski Resorts file.

This currently deletes the existing forecast tables and creates new ones.
    This improves the performance and allows for the data to be the most

'''


load_dotenv()

DB_CONFIG = {
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "dbname": os.getenv("dbname"),
    "port": os.getenv("port")
}

# Initialize OpenMeteo objects
hourly_obj = HourlyForecast()
daily_obj = DailyForecast()

# Set the hourly and daily forecast objects to include the desired data
hourly_obj = hourly_obj.precipitation().snowfall().snow_depth().freezinglevel_height().rain().Showers().weathercode()

daily_obj = daily_obj.windspeed_10m_max().windgusts_10m_max().winddirection_10m_dominant().temperature_2m_max()\
            .temperature_2m_min().apparent_temperature_max().apparent_temperature_min().weathercode()

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
    conn = get_connection(DB_CONFIG)
    cur = conn.cursor()
    print("Connection successful!")
except Exception as e:
    print(f"Failed to connect: {e}")

resorts = pd.read_sql("SELECT * FROM resorts", conn)
print(resorts.head())

# ------------------- Upload Weather Data ------------------- #

# Get weather data
hourly_df, daily_df = get_weather_data(resorts, weather_code_map, hourly_obj, daily_obj)

insert_hourly_df(hourly_df, cur, conn)
insert_daily_df(daily_df, cur, conn)

cur.close()
conn.close()



# ------------------- Local Testing ------------------- #

# # Save data frames
# hourly_daily_df[0].to_csv("meteo_hourly.csv", index=False)
# hourly_daily_df[1].to_csv("meteo_daily.csv", index=False)