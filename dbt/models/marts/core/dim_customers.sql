with customers as (
    select * from {{ ref('stg_customers') }}
),

final as (
    select
        md5(cast(customer_id as text)) as customer_sk,
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name as full_name,
        email,
        signup_date,
        current_timestamp as dbt_loaded_at
    from customers
)

select * from final

