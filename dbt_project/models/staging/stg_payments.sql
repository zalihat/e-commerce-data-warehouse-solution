select 
    payment_id,
    order_id,
    provider as payment_provider,
    amount as payment_amount,
    status as payment_status,
    transaction_id,
    created_at,
    updated_at
from {{ source('raw', 'payments_ext') }}
