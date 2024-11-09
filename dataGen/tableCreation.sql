-- create schema
create schema test_weather10;

-- Create Stations Table (Spatial Data)
CREATE TABLE test_weather10.stations (
    station_id serial PRIMARY KEY,
    station_name varchar(255),
    latitude double precision,
    longitude double precision,
    elevation double precision
);

-- Create Weather Parameters Table
CREATE TABLE test_weather10.weather_parameters (
    parameter_id serial PRIMARY KEY,
    parameter_name varchar(255)
);

-- Create Time Series Data Table
CREATE TABLE test_weather10.time_series_data (
    data_id serial PRIMARY KEY,
    station_id integer REFERENCES stations(station_id),
    parameter_id integer REFERENCES weather_parameters(parameter_id),
    timestamp timestamp,
    value double precision
);

-- Create Spatial Data Table
CREATE TABLE test_weather10.spatial_data (
    station_id integer REFERENCES stations(station_id),
    region varchar(255),
    country varchar(255)
);

