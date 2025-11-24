{{ config(
    materialized='incremental',
    unique_key='shipment_id'
) }}

WITH deduped_shipments AS (
    SELECT
        shipment_id,
        order_id,
        shipping_company,
        shipped_date,
        delivered_date,
        tracking_number,
        created_at,
        updated_at,
        datediff('day', shipped_date, delivered_date) as delivery_days,
        datediff('hour', shipped_date, delivered_date) as delivery_hours,
        ROW_NUMBER() OVER (
            PARTITION BY shipment_id
            ORDER BY updated_at DESC  -- keep latest row if duplicates exist
        ) AS rn
    FROM {{ ref('stg_shipments') }}
    {% if is_incremental() %}
        WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    shipment_id,
    order_id,
    shipping_company,
    shipped_date,
    delivered_date,
    tracking_number,
    delivery_days,
    delivery_hours,
    created_at,
    updated_at
FROM deduped_shipments
WHERE rn = 1
