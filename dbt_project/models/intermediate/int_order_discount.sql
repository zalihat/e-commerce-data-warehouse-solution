
{{ config(materialized='ephemeral') }}


    select 
        order_id,
        sum(discount_amount) as total_discount,
        count(distinct order_item_id) as number_of_items
    from order_items
    group by order_id



