with om as (
    select
        city_sk,
        observation_date,
        apparent_temperature_mean_c as openmeteo_temperature_c
    from {{ ref('fct_openmeteo_daily_weather') }}
),

ws as (
    select
        city_sk,
        observation_date,
        temperature_c as weatherstack_temperature_c
    from {{ ref('fct_weatherstack_daily_weather') }}
),

combined as (
    select
        coalesce(om.city_sk, ws.city_sk) as city_sk,
        coalesce(om.observation_date, ws.observation_date) as observation_date,
        om.openmeteo_temperature_c,
        ws.weatherstack_temperature_c,
        case
            when om.openmeteo_temperature_c is not null and ws.weatherstack_temperature_c is not null
                then round((om.openmeteo_temperature_c + ws.weatherstack_temperature_c) / 2.0, 2)
            when om.openmeteo_temperature_c is not null
                then om.openmeteo_temperature_c
            when ws.weatherstack_temperature_c is not null
                then ws.weatherstack_temperature_c
            else null
        end as avg_temperature_c,
        case
            when om.openmeteo_temperature_c is not null and ws.weatherstack_temperature_c is not null
                then round(abs(om.openmeteo_temperature_c - ws.weatherstack_temperature_c), 2)
            else null
        end as temperature_difference_c,
        (
            case when om.openmeteo_temperature_c is not null then 1 else 0 end +
            case when ws.weatherstack_temperature_c is not null then 1 else 0 end
        ) as sources_count,
        (
            om.openmeteo_temperature_c is not null
            and ws.weatherstack_temperature_c is not null
        ) as has_both_sources
    from om
    full outer join ws
        on om.city_sk = ws.city_sk
        and om.observation_date = ws.observation_date
),

cities as (
    select
        city_sk,
        city,
        country
    from {{ ref('dim_cities') }}
),

final as (
    select
        md5(lower(trim(cities.city)) || '_' || cast(c.observation_date as text)) as consensus_sk,
        c.city_sk,
        cities.city,
        cities.country,
        c.observation_date,
        c.openmeteo_temperature_c,
        c.weatherstack_temperature_c,
        c.avg_temperature_c,
        c.temperature_difference_c,
        c.sources_count,
        c.has_both_sources,
        current_timestamp as dbt_loaded_at
    from combined as c
    inner join cities
        on c.city_sk = cities.city_sk
)

select * from final

