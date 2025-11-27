
select 
    order_id,
    customer_id,
    status as order_status,
    total_amount,
    payment_method,
    coupon_code,
    channel,
    created_at as order_date,
    updated_at
from {{ source('raw', 'orders_ext') }}

