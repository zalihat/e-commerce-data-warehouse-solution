with item_performance as (
    select 
        product_id,
        count(distinct order_id) as orders,
        sum(unit_price * quantity) as revenue,
        count(quantity) as quantities_sold
    from {{ref(('fact_order_items'))}} 
    group by product_id
)
select 
    p.product_id,
    product_name,
    product_category,
    orders as total_orders,
    revenue as total_revenue,
    quantities_sold as units_sold
from {{ref('dim_products')}} as p
left join item_performance
where p.is_current = True
