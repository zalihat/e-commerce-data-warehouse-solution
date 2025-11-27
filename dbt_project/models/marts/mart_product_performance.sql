-- depends_on: {{ ref('fact_orders') }}

{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

with current_products as (
    select *
    from {{ ref('dim_products') }}
    where is_current = true
),

-- Precompute last order date in target table (for incremental runs)
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

-- Aggregate performance metrics
item_performance as (
    select 
        f.product_id,
        count(distinct f.order_id) as orders,
        sum(f.unit_price * f.quantity) as revenue,
        count(f.quantity) as quantities_sold
    from {{ ref('fact_order_items') }} f
    join new_orders n
      on f.order_id = n.order_id
    group by f.product_id
)

select 
    p.product_id,
    p.product_name,
    p.product_category,
    ip.orders as total_orders,
    ip.revenue as total_revenue,
    ip.quantities_sold as units_sold
from current_products p
left join item_performance ip
    on p.product_id = ip.product_id
