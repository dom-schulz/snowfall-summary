# Ski Resort Weather Dashboard

A Streamlit application to track historical snowfall trends and find the best ski resorts based on weather conditions, snowfall forecasts, and driving distances from your location. This project is not fully complete but serves as a proof of concept. 

## Project Overview

This application helps skiers and snowboarders decide where to ski based on:
- Historical snowfall and weather data visualization
- Finding nearby resorts within a specified driving distance
- Sorting resorts by forecast snowfall for the next 4 days (example application of this stysem. )

## Main Components

### Core Files

- `streamlit_app.py` - Main Streamlit application that provides the user interface
- `utils.py` - Utility functions for database connectivity, weather data fetching, and resort finding
- `update_resorts.py` - Script to update the resorts database table with the latest resort information
- `populate_historical.py` - Script to populate the historical weather data for all resorts
- `ometeo_connect.py` - Connects to OpenMeteo API (free) to fetch weather forecast data
- `create_tables.sql` - SQL to create the database schema in Supabase/PostgreSQL

### Data Files
- `final_resorts_us.csv` - Dataset of US ski resorts with coordinates

## Database Design

The application uses a Supabase PostgreSQL database with the following schema:

```sql
-- Resorts table with location data
CREATE TABLE IF NOT EXISTS resorts (
    id INTEGER PRIMARY KEY,
    resort TEXT,
    latitude REAL,
    longitude REAL,
    state TEXT
);

-- Hourly forecast data (snowfall predictions)
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

-- Daily weather data (temperature, wind, etc.)
CREATE TABLE IF NOT EXISTS daily (
    id INTEGER REFERENCES resorts(id),
    time TIMESTAMP NOT NULL,
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
```

### Data Update Considerations

The application faces a challenge with updates to weather data:
- Updating weather data for all resorts takes several minutes
    - Updates should not occur during page load as this creates poor user experience
- Current implementation requires manual updates via scripts

#### Future Development Plans:
- Implement scheduled updates using cron jobs or a serverless function
- Update hourly forecast data every 6 hours
- Update historical data a few days per week
- Add a "last updated" indicator to inform users of data freshness
- Some of the data from OpenMeteo API is missing/lacking. Looking into utilizing NOAA API or others in the future.

## Deployment

The application will be deployed on Streamlit Cloud. 
The current user interface is a proof of concept and needs a lot more work still. 
[Streamlit Cloud Link] - Coming soon!

## Development Challenges

One of the main challenges was designing an efficient database structure that:
1. Uses correct relationships between resorts and their weather data
2. Balances between data freshness and application performance
3. Handles the time-intensive process of fetching weather data for many resorts

This was overcome by:
- Using proper foreign key relationships between tables
- Separating the data update process from the application
- Implementing efficient bulk insert operations for weather data