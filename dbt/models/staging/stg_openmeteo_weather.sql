with source as (
    select * from {{ source('raw', 'raw_openmeteo_weather') }}
),

extracted as (
    select
        city,
        observation_date,

        -- Location and metadata
        cast(payload->>'latitude' as numeric(8, 4)) as latitude,
        cast(payload->>'longitude' as numeric(8, 4)) as longitude,
        cast(payload->>'elevation' as numeric(6, 2)) as elevation,
        payload->>'timezone' as timezone,
        payload->>'timezone_abbreviation' as timezone_abbreviation,
        cast(payload->>'utc_offset_seconds' as integer) as utc_offset_seconds,

        -- Daily weather measurements from daily array index 0
        cast(payload->'daily'->'weather_code'->>0 as integer) as weather_code,
        cast(payload->'daily'->'temperature_2m_max'->>0 as numeric(5, 2)) as temperature_2m_max_c,
        cast(payload->'daily'->'temperature_2m_min'->>0 as numeric(5, 2)) as temperature_2m_min_c,
        cast(payload->'daily'->'temperature_2m_mean'->>0 as numeric(5, 2)) as temperature_2m_mean_c,
        cast(payload->'daily'->'apparent_temperature_max'->>0 as numeric(5, 2)) as apparent_temperature_max_c,
        cast(payload->'daily'->'apparent_temperature_min'->>0 as numeric(5, 2)) as apparent_temperature_min_c,
        cast(payload->'daily'->'apparent_temperature_mean'->>0 as numeric(5, 2)) as apparent_temperature_mean_c,
        cast(payload->'daily'->'sunrise'->>0 as timestamp) as sunrise,
        cast(payload->'daily'->'sunset'->>0 as timestamp) as sunset,
        cast(payload->'daily'->'daylight_duration'->>0 as numeric(10, 2)) as daylight_duration_s,
        cast(payload->'daily'->'sunshine_duration'->>0 as numeric(10, 2)) as sunshine_duration_s,
        cast(payload->'daily'->'precipitation_sum'->>0 as numeric(6, 2)) as precipitation_sum_mm,
        cast(payload->'daily'->'rain_sum'->>0 as numeric(6, 2)) as rain_sum_mm,
        cast(payload->'daily'->'precipitation_hours'->>0 as numeric(5, 2)) as precipitation_hours,
        cast(payload->'daily'->'wind_speed_10m_max'->>0 as numeric(6, 2)) as wind_speed_10m_max_kmh,
        cast(payload->'daily'->'wind_gusts_10m_max'->>0 as numeric(6, 2)) as wind_gusts_10m_max_kmh,
        cast(payload->'daily'->'wind_direction_10m_dominant'->>0 as integer) as wind_direction_10m_dominant,
        cast(payload->'daily'->'shortwave_radiation_sum'->>0 as numeric(8, 2)) as shortwave_radiation_sum_mj_m2,
        cast(payload->'daily'->'et0_fao_evapotranspiration'->>0 as numeric(6, 2)) as et0_fao_evapotranspiration_mm,

        -- Ingestion timestamp
        ingested_at

    from source
)

select * from extracted
