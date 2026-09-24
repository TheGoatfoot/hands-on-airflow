with openmeteo_cities as (
    select
        lower(trim(city)) as city_key,
        city,
        latitude,
        longitude,
        elevation,
        timezone,
        timezone_abbreviation,
        utc_offset_seconds,
        row_number() over (
            partition by lower(trim(city))
            order by observation_date desc, ingested_at desc
        ) as rn
    from {{ ref('openmeteo_weather') }}
),

latest as (
    select * from openmeteo_cities where rn = 1
),

final as (
    select
        md5(city_key) as city_sk,
        city,
        latitude,
        longitude,
        elevation,
        timezone,
        timezone_abbreviation,
        cast(utc_offset_seconds / 3600.0 as text) as utc_offset,
        current_timestamp as dbt_loaded_at
    from latest
)

select * from final
