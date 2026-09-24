with weatherstack_cities as (
    select
        lower(trim(city)) as city_key,
        city,
        location_name,
        country,
        region,
        latitude,
        longitude,
        timezone_id,
        utc_offset,
        row_number() over (
            partition by lower(trim(city))
            order by observation_date desc, ingested_at desc
        ) as rn
    from {{ ref('weatherstack_weather') }}
),

openmeteo_cities as (
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

ws_latest as (
    select * from weatherstack_cities where rn = 1
),

om_latest as (
    select * from openmeteo_cities where rn = 1
),

all_cities as (
    select distinct city_key from ws_latest
    union
    select distinct city_key from om_latest
),

final as (
    select
        md5(ac.city_key) as city_sk,
        coalesce(ws.city, om.city) as city,
        ws.location_name,
        ws.country,
        ws.region,
        coalesce(ws.latitude, om.latitude) as latitude,
        coalesce(ws.longitude, om.longitude) as longitude,
        om.elevation,
        coalesce(ws.timezone_id, om.timezone) as timezone,
        om.timezone_abbreviation,
        coalesce(ws.utc_offset, cast(om.utc_offset_seconds / 3600.0 as text)) as utc_offset,
        current_timestamp as dbt_loaded_at
    from all_cities as ac
    left join ws_latest as ws on ac.city_key = ws.city_key
    left join om_latest as om on ac.city_key = om.city_key
)

select * from final

