
{{ config(materialized='ephemeral') }}

with order_items as (
    select    
        order_id,
        order_item_id,
        {{ calculate_discount_amount('discount', 'unit_price', 'quantity') }} 
            as discount_amount
    from {{ref('stg_order_items')}}
),

order_discount as (
    select 
        order_id,
        sum(discount_amount) as total_discount,
        count(distinct order_item_id) as number_of_items
    from order_items
    group by order_id

)
select 
    *  
from order_discount

