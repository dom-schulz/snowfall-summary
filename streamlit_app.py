import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, timezone
from utils import get_connection, populate_weather_data, get_nearby_resorts_within_driving_distance
from dotenv import load_dotenv
import os
import openrouteservice
from geopy.geocoders import Nominatim


# ------------------- CONFIG DB ------------------- #
load_dotenv()
DB_CONFIG = {
    "host": os.getenv("host"),
    "user": os.getenv("user"),
    "password": os.getenv("password"),
    "dbname": os.getenv("dbname"),
    "port": os.getenv("port")
}

WEATHER_TABLE = "historical_weather"
ORS_API_KEY = os.getenv("ors_api_key")

# --- STREAMLIT APP ---
st.set_page_config(page_title="Snowfall Dashboard", layout="wide")
tabs = st.tabs(["Dashboard", "Insights", "About"])

# --- TAB 1: DASHBOARD ---
with tabs[0]:
    st.header("Historical Snowfall Dashboard")
    with st.spinner("Loading data..."):
        conn = get_connection(DB_CONFIG)
        cursor = conn.cursor()
        
        df = pd.read_sql(f"SELECT * FROM {WEATHER_TABLE} INNER JOIN resorts USING(id)", conn)
        
        cursor.close()
        conn.close()

    df['time'] = pd.to_datetime(df['time'])
    df = df[df['time'] >= (datetime.now() - timedelta(days=90))]

    states = st.multiselect("Select states:", sorted(df['state'].unique()), default=df['state'].unique())
    resorts = st.multiselect("Select resorts:", sorted(df[df['state'].isin(states)]['resort'].unique()))
    date_range = st.date_input("Select date range:", [df['time'].min(), df['time'].max()])
    variable = st.selectbox("Variable to display:", [
        'snowfall_sum', 'temperature_2m_max', 'temperature_2m_min',
        'precipitation_sum', 'apparent_temperature_max', 'apparent_temperature_min'])

    filtered_df = df[
        (df['state'].isin(states)) &
        (df['resort'].isin(resorts) if resorts else True) &
        (df['time'] >= pd.to_datetime(date_range[0])) & (df['time'] <= pd.to_datetime(date_range[1]))
    ]

    if filtered_df.empty:
        st.warning("No data for selected filters.")
    else:
        # Aggregate the data to handle duplicates
        aggregated_df = filtered_df.groupby(['time', 'resort'])[variable].mean().reset_index()

        # Create a pivot table to have resorts as columns
        pivot_df = aggregated_df.pivot(index='time', columns='resort', values=variable)

        # Plot the line chart with all resorts overlaid
        st.line_chart(
            pivot_df,
            height=250,
            use_container_width=True
        )

# --- TAB 2: INSIGHTS ---
with tabs[1]:
    st.header("Find Nearby Resorts")
    
    # Define unit dictionary for display
    units_dict = {
        'temperature_2m_max': '°C (°F)',
        'temperature_2m_min': '°C (°F)',
        'apparent_temperature_max': '°C (°F)',
        'apparent_temperature_min': '°C (°F)',
        'precipitation_sum': 'mm',
        'precipitation_hours': 'hours',
        'snowfall_sum': 'cm',
        'sunrise': 'iso8601',
        'sunset': 'iso8601',
        'windspeed_10m_max': 'km/h (mph, m/s, knots)',
        'windgusts_10m_max': 'km/h (mph, m/s, knots)',
        'winddirection_10m_dominant': '°',
        'shortwave_radiation_sum': 'MJ/m²',
        'et0_fao_evapotranspiration': 'mm',
        'all': '-'
    }
    
    # Get address from user
    address_input = st.text_input("Enter your address:", placeholder="e.g. 123 Main St, Spokane, WA")
    max_distance = st.slider("Maximum driving distance (miles):", min_value=50, max_value=800, value=400, step=50)
    
    # Process when user submits address
    if st.button("Find Nearby Resorts"):
        if address_input:
            with st.spinner("Geocoding address and finding nearby resorts..."):
                try:
                    # Geocode the address
                    geolocator = Nominatim(user_agent="resort_finder")
                    location = geolocator.geocode(address_input, timeout=10)
                    
                    if not location:
                        st.error("Could not geocode that address. Please try again with a more specific address.")
                    else:
                        st.success(f"Found location: {location.address}")
                        user_lat, user_lon = location.latitude, location.longitude
                        
                        # Get nearby resorts
                        nearby_resorts = get_nearby_resorts_within_driving_distance(DB_CONFIG, ORS_API_KEY, user_lat, user_lon, max_distance)
                        
                        # Display results
                        if not nearby_resorts:
                            st.warning(f"No resorts found within {max_distance} miles driving distance.")
                        else:
                            # Sort by snowfall forecast (descending)
                            nearby_resorts.sort(key=lambda x: x.get("forecast_snowfall", 0), reverse=True)
                            
                            # Create a DataFrame for display
                            results_df = pd.DataFrame(nearby_resorts)
                            
                            # Format drive time
                            def format_drive_time(minutes):
                                hours = int(minutes // 60)
                                mins = int(minutes % 60)
                                return f"{hours}h {mins}m" if hours else f"{mins}m"
                            
                            results_df["drive_time"] = results_df["duration_minutes"].apply(format_drive_time)
                            results_df["distance"] = results_df["distance"].round(1).astype(str) + " miles"
                            results_df["forecast_snowfall"] = results_df["forecast_snowfall"].round(2).astype(str) + " inches"
                            
                            # Display top 5 resorts
                            st.subheader("Top Resorts by Forecast Snowfall")
                            st.dataframe(
                                results_df[["resort", "state", "forecast_snowfall", "distance", "drive_time"]].head(5),
                                use_container_width=True
                            )
                            
                            # Show all resorts in a table
                            with st.expander("Show all nearby resorts"):
                                st.dataframe(
                                    results_df[["resort", "state", "forecast_snowfall", "distance", "drive_time"]],
                                    use_container_width=True
                                )
                                
                except Exception as e:
                    st.error(f"An error occurred: {str(e)}")
        else:
            st.warning("Please enter an address to find nearby resorts.")


# --- TAB 3: ABOUT ---
with tabs[2]:
    st.header("About")
    st.write("This dashboard was built to visualize snowfall trends across U.S. resorts using OpenMeteo data and Supabase.")
