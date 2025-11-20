
with delivered_orders as (
    select
        order_id,
        order_date,
        customer_id,
        total_amount
    from {{ref('stg_orders')}}
    where lower(order_status) = 'delivered'
),


customers_orders as (
    select 
        customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        count(distinct o.order_id) as number_of_orders,
        sum(o.total_amount) as total_revenue,
        sum(d.total_discount) as total_discount
    from delivered_orders as o
    inner join {{ref('int_order_discount')}} as d
        on o.order_id = d.order_id
    group by customer_id

),

customer_360 as (
    select
        c.customer_id,
        first_name,
        last_name,
        email,
        country,
        signup_date
        first_order_date,
        last_order_date,
        number_of_orders,
        total_revenue,
        total_discount

    from {{ref('stg_customers')}} as c
    left join customers_orders as co
    on c.customer_id = co.customer_id

)

select * from customer_360