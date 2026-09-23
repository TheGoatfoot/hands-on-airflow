with stg as (
    select * from {{ ref('stg_weatherstack_weather') }}
),

cities as (
    select
        city_sk,
        city
    from {{ ref('dim_weatherstack_cities') }}
),

final as (
    select
        md5(lower(trim(stg.city)) || '_' || cast(stg.observation_date as text)) as weather_observation_sk,
        cities.city_sk,
        stg.observation_date,
        stg.observation_time,
        stg.weather_code,
        stg.weather_description,
        stg.temperature_c,
        stg.feelslike_c,
        stg.humidity,
        stg.wind_speed_kmh,
        stg.wind_degree,
        stg.wind_dir,
        stg.pressure_mb,
        stg.precip_mm,
        stg.cloudcover,
        stg.uv_index,
        stg.visibility_km,
        stg.air_quality_pm2_5,
        stg.air_quality_pm10,
        stg.air_quality_co,
        stg.air_quality_no2,
        stg.air_quality_o3,
        stg.air_quality_so2,
        stg.air_quality_us_epa_index,
        stg.air_quality_gb_defra_index,
        stg.astro_sunrise,
        stg.astro_sunset,
        stg.astro_moonrise,
        stg.astro_moonset,
        stg.astro_moon_phase,
        stg.astro_moon_illumination,
        stg.ingested_at,
        current_timestamp as dbt_loaded_at
    from stg
    inner join cities
        on lower(trim(stg.city)) = lower(trim(cities.city))
)

select * from final

