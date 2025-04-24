from utils import update_resorts, get_connection
from dotenv import load_dotenv
import os

'''
Run this script to update the resorts table with the latest data from the local file.

'''


load_dotenv()

DB_CONFIG = {
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "dbname": os.getenv("dbname"),
    "port": os.getenv("port")
}

RESORTS_TABLE = "resorts"

conn = get_connection(DB_CONFIG)
cursor = conn.cursor()

update_resorts(conn, cursor, RESORTS_TABLE, "final_resorts_us.csv")

cursor.close()
conn.close()

print("Resorts updated successfully")