select
    shipment_id,
    order_id,
    carrier as shipping_company,
    shipped_date,
    delivered_date,
    tracking_number,
    created_at,
    updated_at

from {{source('raw', 'shipments_ext')}}