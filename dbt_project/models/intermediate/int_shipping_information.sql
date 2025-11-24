
{{ config(materialized='ephemeral') }}
select 
    shipment_id,
    order_id,
    shipping_company,
    datediff('day', shipped_date, delivered_date) as delivery_days,
    datediff('hour', shipped_date, delivered_date) as delivery_hours
from {{ref('fact_shipments')}}