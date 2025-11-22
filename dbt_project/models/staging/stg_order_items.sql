select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    {{ calculate_discount_amount('discount', 'unit_price', 'quantity') }} 
            as discount_amount,
    created_at,
    updated_at
from {{ source('raw', 'order_items_ext') }}

