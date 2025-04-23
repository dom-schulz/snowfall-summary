import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from geopy.geocoders import Nominatim
import openrouteservice

# ---------------------- Setup ---------------------- #
load_dotenv()

USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")
ORS_API_KEY = os.getenv("ors_api_key")

desired_distance = 400  # in miles

# ------------------- Connect DB -------------------- #
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    cursor = connection.cursor()
    print("Connection successful!")
except Exception as e:
    print(f"Failed to connect: {e}")

# ------------------ Sample Query ------------------- #
cursor.execute("SELECT resort, state FROM resorts LIMIT 5")
print("Resorts:")
for row in cursor.fetchall():
    print(row)
print(f'finished sample query\n\n\n')

# ------------------ Geocode Address ---------------- #
geolocator = Nominatim(user_agent="resort_finder")
while True:
    address_input = input("Enter your full address (e.g. 123 Main St, Spokane, WA): ")
    location = geolocator.geocode(address_input, timeout=10)

    if not location:
        print("Could not geocode that address. Please try again.")
        continue

    address = location.address
    print(f"\nYou entered: {address}")
    confirm = input("Is this correct? (y/n): ").strip().lower()
    if confirm == 'y':
        break

user_lat, user_lon = location.latitude, location.longitude
print(f"\nUsing coordinates: ({user_lat:.4f}, {user_lon:.4f})")

# ---------------- Get Nearby Resorts ---------------- #
def get_nearby_resorts_within_driving_distance(max_miles=desired_distance):
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

    return nearby


nearby_resorts = get_nearby_resorts_within_driving_distance()
nearby_ids = tuple([r["id"] for r in nearby_resorts])

if not nearby_ids:
    print(f"No resorts found within {desired_distance} miles.")
    cursor.close()
    connection.close()
    exit()

# ------------- Forecast Aggregation ---------------- #
cursor.execute("SELECT MIN(time) FROM hourly")
now = cursor.fetchone()[0]
cutoff = now + timedelta(days=4)

query = f"""
    SELECT h.id, SUM(h.snowfall) AS total_snowfall_next_4_days
    FROM hourly h
    WHERE h.id IN %s AND time >= %s AND time <= %s
    GROUP BY h.id
    ORDER BY total_snowfall_next_4_days DESC
    LIMIT 5
"""
cursor.execute(query, (nearby_ids, now.isoformat(), cutoff.isoformat()))
top_resorts = cursor.fetchall()

# ----------------- Display Results ----------------- #
def format_drive_time(minutes):
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    return f"{hours} hr {mins} min" if hours else f"{mins} min"


print(f"\nTop 5 resorts by snowfall within {desired_distance} driving miles:\n")
for resort_id, snowfall in top_resorts:
    resort = next((r for r in nearby_resorts if r["id"] == resort_id), None)
    if resort:
        drive_time_str = format_drive_time(resort['duration_minutes'])
        print(f"{resort['resort']} ({resort['state']}) - {float(snowfall):.2f} inches, {resort['distance']:.1f} miles, {drive_time_str} drive")

cursor.close()
connection.close()
print("\nConnection closed.")
