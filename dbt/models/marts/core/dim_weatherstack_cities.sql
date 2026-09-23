with stg as (
    select * from {{ ref('stg_weatherstack_weather') }}
),

latest_cities as (
    select
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
        ) as row_num
    from stg
),

final as (
    select
        md5(lower(trim(city))) as city_sk,
        city,
        location_name,
        country,
        region,
        latitude,
        longitude,
        timezone_id,
        utc_offset,
        current_timestamp as dbt_loaded_at
    from latest_cities
    where row_num = 1
)

select * from final
