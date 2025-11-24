
with delivered_orders as (
    select
        order_id,
        order_date,
        customer_id,
        total_amount
    from {{ref('fact_orders')}}
    where lower(order_status) = 'delivered'
),


customers_orders as (
    select 
        customer_id,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date,
        count(distinct o.order_id) as number_of_orders,
        sum(o.total_amount) as total_revenue,
        sum(d.discount_amount) as total_discount
    from delivered_orders as o
    inner join {{ref('fact_order_items')}} as d
        on o.order_id = d.order_id
    group by customer_id

),

customer_360 as (
    select
        c.customer_id,
        customer_full_name,
        customer_email,
        signup_date,
        customer_country,
        first_order_date,
        last_order_date,
        DATEDIFF(day, last_order_date, CURRENT_DATE() ) AS days_since_last_order,
        number_of_orders,
        total_revenue,
        total_discount

    from {{ref('dim_customers')}} as c
    left join customers_orders as co
    on c.customer_id = co.customer_id
    where c.is_current = True

)

select * from customer_360