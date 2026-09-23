-- DDL for raw weather landing table
CREATE TABLE IF NOT EXISTS raw_weather (
    city VARCHAR(100) NOT NULL,
    observation_date DATE NOT NULL,
    payload JSONB NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (city, observation_date)
);
