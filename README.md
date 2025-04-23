# Ski Resort Weather Dashboard

A Streamlit application to track historical snowfall trends and find the best ski resorts based on weather conditions, snowfall forecasts, and driving distances from your location. Note that this project is not fully completed and still needs many improvements.

## Project Overview

This application helps skiers and snowboarders decide where to ski based on:
- Historical snowfall and weather data visualization
- Finding nearby resorts within a specified driving distance
- Sorting resorts by forecast snowfall for the next 4 days

## Main Components

### Core Files

- `streamlit_app.py` - Main Streamlit application that provides the user interface
- `utils.py` - Utility functions for database connectivity, weather data fetching, and resort finding
- `update_resorts.py` - Script to update the resorts database table with the latest resort information
- `populate_historical.py` - Script to populate the historical weather data for all resorts
- `ometeo_connect.py` - Connects to OpenMeteo API to fetch weather forecast data
- `create_tables.sql` - SQL to create the database schema in Supabase/PostgreSQL

### Data Files
- `final_resorts_us.csv` - Dataset of US ski resorts with coordinates
- `meteo_hourly.csv` - Hourly weather forecast data
- `meteo_daily.csv` - Daily weather forecast data

### Data Cleaning
The `data_cleaning` directory contains scripts and data files used to prepare the ski resort dataset:

- `clean_data1.py` - Initial processing script that filters Kaggle ski resort data to US resorts only and adds state information using reverse geocoding
- `clean_data2_scrape.py` - Web scraper that uses Selenium to search for each resort on Google and extract more accurate coordinates and state information
- `clean_data3.py` - Final processing script that merges resort IDs with coordinates data to create the final dataset
- `final_resorts_us.csv` - The clean, final dataset with accurate coordinates and state information
- `resorts_us.csv` - Intermediate dataset with US resorts and initial state information
- `resort_coords.csv` - Dataset with coordinates from web scraping
- `resorts.csv` - Original Kaggle dataset containing worldwide ski resorts

**Note:**: Some manual alteration of the final dataset has been made and will need to be made in the futre (ie. not all the coordinates scraped are accurate locations of ski resorts)

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
CREATE TABLE daily (
    id INTEGER REFERENCES resorts(id),
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
```

### Data Update Considerations

The application faces a challenge with updates to weather data:
- Updating weather data for all resorts takes several minutes
- Updates should not occur during page load as this creates poor user experience
- Current implementation requires manual updates via scripts

#### Future Development Plans:
- Implement scheduled updates using cron jobs or a serverless function
- Update hourly forecast data every 6 hours
- Update historical data once per day during low-traffic periods
- Add a "last updated" indicator to inform users of data freshness

## Deployment

The application will be deployed on Streamlit Cloud. 
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

### Data Cleaning Challenges

Obtaining accurate resort data presented several challenges:
- The original Kaggle dataset contained worldwide resorts with inconsistent or missing information
- Many resorts had incorrect coordinates or were missing state information
- Reverse geocoding was necessary but sometimes imprecise near state borders
- Web scraping and manual edits were required to obtain the most accurate resort information

The data cleaning pipeline involved:
1. Filtering the original dataset to US resorts only
2. Using reverse geocoding to add state information
3. Scraping Google search results to verify and correct coordinates
4. Merging and reformatting data to create the final dataset 