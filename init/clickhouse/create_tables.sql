CREATE DATABASE IF NOT EXISTS weather;

CREATE TABLE IF NOT EXISTS weather.weather_hourly (
    city String,
    date Date,
    hour UInt8,
    temperature Float32,
    precipitation Float32,
    windspeed Float32,
    winddirection UInt16,
    _loaded_at DateTime
) ENGINE = MergeTree
ORDER BY (city, date, hour);

CREATE TABLE IF NOT EXISTS weather.weather_daily (
    city String,
    date Date,
    min_temperature Float32,
    max_temperature Float32,
    avg_temperature Float32,
    total_precipitation Float32,
    _loaded_at DateTime
) ENGINE = MergeTree
ORDER BY (city, date);