-- Postgres SQL Create Tables in Supabase

DROP TABLE IF EXISTS daily;
DROP TABLE IF EXISTS hourly;
DROP TABLE IF EXISTS resorts;

CREATE TABLE IF NOT EXISTS resorts (
    id INTEGER PRIMARY KEY,
    resort TEXT,
    latitude REAL,
    longitude REAL,
    state TEXT
);

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
