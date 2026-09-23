-- DDL for raw openmeteo weather landing table
CREATE TABLE IF NOT EXISTS raw_openmeteo_weather (
    city VARCHAR(100) NOT NULL,
    observation_date DATE NOT NULL,
    latitude NUMERIC(8, 4),
    longitude NUMERIC(8, 4),
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, observation_date)
);
