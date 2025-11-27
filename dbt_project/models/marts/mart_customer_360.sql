{{ config(
    materialized='incremental',
    unique_key='customer_id',
    incremental_strategy='merge'
) }}

-- Only get current customers
with current_customers as (
    select *
    from {{ ref('dim_customers') }}
    where is_current = true
),

-- Delivered orders (incremental-friendly)
delivered_orders as (
    select
        order_id,
        order_date,
        customer_id,
        total_amount
    from {{ ref('fact_orders') }}
    where lower(order_status) = 'delivered'
    
    {% if is_incremental() %}
      -- Only get new or updated orders since last load
      and order_date > (select coalesce(max(last_order_date), '1900-01-01') from {{ this }})
    {% endif %}
),

-- Aggregate metrics per customer
customers_orders as (
    select 
        o.customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        count(distinct o.order_id) as number_of_orders,
        sum(o.total_amount) as total_revenue,
        sum(d.discount_amount) as total_discount
    from delivered_orders o
    inner join {{ ref('fact_order_items') }} d
        on o.order_id = d.order_id
    group by o.customer_id
),

-- Final 360 view
customer_360 as (
    select
        c.customer_id,
        c.customer_full_name,
        c.customer_email,
        c.signup_date,
        c.customer_country,
        co.first_order_date,
        co.last_order_date,
        datediff(day, co.last_order_date, current_date()) as days_since_last_order,
        co.number_of_orders,
        co.total_revenue,
        co.total_discount
    from current_customers c
    left join customers_orders co
        on c.customer_id = co.customer_id
)

select * from customer_360
