with source as (
    select * from {{ ref('raw_customers') }}
),

transformed as (
    select
        id as customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        lower(trim(email)) as email,
        signup_date::date as signup_date
    from source
)

select * from transformed

