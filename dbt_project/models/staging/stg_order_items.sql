select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    created_at,
    updated_at
from {{ source('raw', 'order_items_ext') }}

