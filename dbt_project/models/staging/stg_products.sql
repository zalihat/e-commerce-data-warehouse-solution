select 
    product_id,
    name as product_name,
    category,
    price,
    reseller_id,
    created_at,
    updated_at
from {{ source('raw', 'products_ext') }}
