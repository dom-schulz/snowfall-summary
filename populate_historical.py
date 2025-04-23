from utils import populate_weather_data, get_connection
from dotenv import load_dotenv
import os

load_dotenv()

DB_CONFIG = {
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "dbname": os.getenv("dbname"),
    "port": os.getenv("port")
}

WEATHER_TABLE = "historical_weather"

conn = get_connection(DB_CONFIG)
cursor = conn.cursor()

populate_weather_data(conn, cursor, WEATHER_TABLE)

cursor.close()
conn.close()
