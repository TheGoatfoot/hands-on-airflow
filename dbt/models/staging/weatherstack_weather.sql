with source as (
    select * from {{ source('raw', 'weatherstack_weather') }}
),

extracted as (
    select
        city,
        observation_date,

        -- Location attributes
        payload->'location'->>'name' as location_name,
        payload->'location'->>'country' as country,
        payload->'location'->>'region' as region,
        cast(payload->'location'->>'lat' as numeric(8, 4)) as latitude,
        cast(payload->'location'->>'lon' as numeric(8, 4)) as longitude,
        payload->'location'->>'timezone_id' as timezone_id,
        payload->'location'->>'localtime' as localtime,
        cast(payload->'location'->>'localtime_epoch' as bigint) as localtime_epoch,
        payload->'location'->>'utc_offset' as utc_offset,

        -- Current observation metrics & weather conditions
        payload->'current'->>'observation_time' as observation_time,
        cast(payload->'current'->>'temperature' as numeric(5, 2)) as temperature_c,
        cast(payload->'current'->>'feelslike' as numeric(5, 2)) as feelslike_c,
        cast(payload->'current'->>'weather_code' as integer) as weather_code,
        payload->'current'->'weather_descriptions'->>0 as weather_description,
        payload->'current'->'weather_icons'->>0 as weather_icon,
        cast(payload->'current'->>'wind_speed' as numeric(6, 2)) as wind_speed_kmh,
        cast(payload->'current'->>'wind_degree' as integer) as wind_degree,
        payload->'current'->>'wind_dir' as wind_dir,
        cast(payload->'current'->>'pressure' as numeric(7, 2)) as pressure_mb,
        cast(payload->'current'->>'precip' as numeric(6, 2)) as precip_mm,
        cast(payload->'current'->>'humidity' as integer) as humidity,
        cast(payload->'current'->>'cloudcover' as integer) as cloudcover,
        cast(payload->'current'->>'uv_index' as integer) as uv_index,
        cast(payload->'current'->>'visibility' as numeric(6, 2)) as visibility_km,

        -- Astronomical attributes
        payload->'current'->'astro'->>'sunrise' as astro_sunrise,
        payload->'current'->'astro'->>'sunset' as astro_sunset,
        payload->'current'->'astro'->>'moonrise' as astro_moonrise,
        payload->'current'->'astro'->>'moonset' as astro_moonset,
        payload->'current'->'astro'->>'moon_phase' as astro_moon_phase,
        cast(payload->'current'->'astro'->>'moon_illumination' as integer) as astro_moon_illumination,

        -- Air quality indicators
        cast(payload->'current'->'air_quality'->>'co' as numeric(10, 2)) as air_quality_co,
        cast(payload->'current'->'air_quality'->>'no2' as numeric(10, 2)) as air_quality_no2,
        cast(payload->'current'->'air_quality'->>'o3' as numeric(10, 2)) as air_quality_o3,
        cast(payload->'current'->'air_quality'->>'so2' as numeric(10, 2)) as air_quality_so2,
        cast(payload->'current'->'air_quality'->>'pm2_5' as numeric(10, 2)) as air_quality_pm2_5,
        cast(payload->'current'->'air_quality'->>'pm10' as numeric(10, 2)) as air_quality_pm10,
        cast(payload->'current'->'air_quality'->>'us-epa-index' as integer) as air_quality_us_epa_index,
        cast(payload->'current'->'air_quality'->>'gb-defra-index' as integer) as air_quality_gb_defra_index,

        -- Metadata
        ingested_at

    from source
)

select * from extracted

