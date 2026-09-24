with stg as (
    select * from {{ ref('openmeteo_weather') }}
),

cities as (
    select
        city_sk,
        city
    from {{ ref('dim_cities') }}
),

final as (
    select
        md5(lower(trim(stg.city)) || '_' || cast(stg.observation_date as text)) as weather_observation_sk,
        cities.city_sk,
        stg.city,
        stg.observation_date,
        stg.weather_code,
        stg.temperature_2m_max_c,
        stg.temperature_2m_min_c,
        stg.temperature_2m_mean_c,
        stg.apparent_temperature_max_c,
        stg.apparent_temperature_min_c,
        stg.apparent_temperature_mean_c,
        stg.sunrise,
        stg.sunset,
        stg.daylight_duration_s,
        stg.sunshine_duration_s,
        stg.precipitation_sum_mm,
        stg.rain_sum_mm,
        stg.precipitation_hours,
        stg.wind_speed_10m_max_kmh,
        stg.wind_gusts_10m_max_kmh,
        stg.wind_direction_10m_dominant,
        stg.shortwave_radiation_sum_mj_m2,
        stg.et0_fao_evapotranspiration_mm,
        stg.ingested_at,
        current_timestamp as dbt_loaded_at
    from stg
    inner join cities
        on lower(trim(stg.city)) = lower(trim(cities.city))
)

select * from final

