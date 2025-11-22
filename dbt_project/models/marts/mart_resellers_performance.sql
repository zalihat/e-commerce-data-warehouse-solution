with item_performance as (
    select 
        p.product_id,
        product_name,
        reseller_id,
        order_id,
        order_item_id,
        quantity,
        unit_price * quantity as revenue
    from {{ref('stg_products')}} as p
    left join {{ref('stg_order_items')}} as oi
        on p.product_id = oi.product_id
),

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


select 
    r.reseller_id,
    reselller_name,
    reseller_email,
    reseller_country,
    reseller_join_date,
    products,
    products_sold,
    unique_orders,
    total_revenue,
    quantities_sold


from {{ref('stg_resellers')}} as r
left join products_performance as p
    on r.reseller_id = p.reseller_id