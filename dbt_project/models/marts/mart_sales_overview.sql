with order_items as (
    select    
        order_item_id,
        order_id,
        {{ calculate_discount_amount('discount', 'unit_price', 'quantity') }} 
            as discount_amount
    from {{ref('stg_order_items')}}
),

sales_items as (
    select 
        order_id,
        count(distinct order_item_id) as number_of_items,
        sum(discount_amount) as total_discount
    from order_items
    group by order_id

),

orders as (
    select
        order_id,
        order_date,
        customer_id,
        total_amount,
        order_status,
        payment_method,
        channel
    from {{ref('stg_orders')}}
),

customers as (
    select
        customer_id,
        country
    from {{ref('stg_customers')}}
),

payments as (
    select
        payment_id,
        max(order_id) as order_id,
        max(payment_provider) as payment_provider,
        avg(payment_amount) as payment_amount,
        max(payment_status) as payment_status
    from {{ref('stg_payments')}}
    group by payment_id
),

sales_overview as (
    select 
        orders.*,
        p.payment_provider,
        payment_status,
        sales_items.number_of_items,
        sales_items.total_discount,
        c.country as customer_country
    from orders 
    inner join sales_items 
        on orders.order_id = sales_items.order_id
    inner join payments as p 
        on orders.order_id = p.order_id
    left join customers as c
        on orders.customer_id = c.customer_id
    
)

select 
    * 
from sales_overview


