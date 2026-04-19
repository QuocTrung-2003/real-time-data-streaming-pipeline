CREATE TABLE IF NOT EXISTS stream_aggregates (
    window_start TIMESTAMP,
    window_end TIMESTAMP,

    avg_temperature DOUBLE PRECISION,
    avg_windspeed DOUBLE PRECISION,
    max_temperature DOUBLE PRECISION,
    max_windspeed DOUBLE PRECISION,

    wind_level TEXT,
    day_night TEXT,
    weather_category TEXT
);