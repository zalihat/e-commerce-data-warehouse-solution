
{{ 
    config(
        materialized='incremental',
        unique_key='order_id'  
    ) 
}}
with order_items_summary as (
    select 
        order_id,
        sum(discount_amount) as total_discount,
        count(distinct order_item_id) as number_of_items
    from {{ref('fact_order_items')}} 
    group by order_id
),


sales_overview as (
    select 
        orders.order_id,
        order_date,
        total_amount,
        order_status,
        payment_method,
        channel,
        payment_id,
        p.payment_provider,
        payment_status,
        order_items.number_of_items,
        order_items.total_discount,
        orders.customer_id,
        customer_full_name,
        customer_email,
        customer_country,
        shipment_id,
        shipping_company,
        delivery_days
    from {{ref('fact_orders')}} as orders
    inner join order_items_summary as order_items
        on orders.order_id = order_items.order_id
    inner join {{ref('fact_payments')}} as p 
        on orders.order_id = p.order_id
    inner join {{ref('fact_shipments')}} as shipping 
        on orders.order_id = shipping.order_id
    left join {{ref('dim_customers')}} as c
        on orders.customer_id = c.customer_id
        AND orders.order_date >= c.valid_from
        AND (
          orders.order_date < c.valid_to OR c.valid_to IS NULL
        )

    
    
)

select 
    * 
from sales_overview


{% if is_incremental() %}
    -- only load new/updated orders for incremental runs
    where order_date > (select max(order_date) from {{ this }})
{% endif %}