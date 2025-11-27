-- depends_on: {{ ref('fact_orders') }}

{{ config(
    materialized='incremental',
    unique_key='reseller_id',
    incremental_strategy='merge'
) }}

-- Only current products
with current_products as (
    select *
    from {{ ref('dim_products') }}
    where is_current = true
),

-- Precompute last order date in target table for incremental runs
last_order_date as (
    {% if is_incremental() %}
    select coalesce(max(order_date), '1900-01-01') as last_date
    from {{ ref('fact_orders') }}
    {% else %}
    select '1900-01-01'::date as last_date
    {% endif %}
),

-- Only new delivered orders
new_orders as (
    select o.order_id
    from {{ ref('fact_orders') }} o
    cross join last_order_date l
    where lower(o.order_status) = 'delivered'
      and o.order_date > l.last_date
),

-- Combine product and order data
item_performance as (
    select 
        p.product_id,
        p.product_name,
        p.reseller_id,
        oi.order_id,
        oi.order_item_id,
        oi.quantity,
        oi.revenue
    from current_products p
    left join {{ ref('fact_order_items') }} oi
        on p.product_id = oi.product_id
    join new_orders n
        on oi.order_id = n.order_id
),

-- Aggregate per reseller
products_performance as (
    select 
        reseller_id,
        count(distinct product_id) as products,
        count(distinct order_item_id) as products_sold,
        count(distinct order_id) as unique_orders,
        sum(quantity) as quantities_sold,
        sum(revenue) as total_revenue
    from item_performance
    group by reseller_id
)

-- Final reseller 360 view
select 
    r.reseller_id,
    r.reseller_name,
    r.reseller_email,
    r.reseller_country,
    r.reseller_join_date,
    pp.products,
    pp.products_sold,
    pp.unique_orders,
    pp.total_revenue,
    pp.quantities_sold
from {{ ref('dim_resellers') }} r
left join products_performance pp
    on r.reseller_id = pp.reseller_id
where r.is_current = true
