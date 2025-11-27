{{ config(
    materialized='incremental',
    unique_key='order_item_id'
) }}

WITH deduped_order_items AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        discount,
        {{ calculate_discount_amount('discount', 'unit_price', 'quantity') }} AS discount_amount,
        unit_price * quantity as revenue,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY updated_at DESC  -- keep the latest row per duplicate
        ) AS rn
    FROM {{ ref('stg_order_items') }}
    {% if is_incremental() %}
        WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    discount,
    discount_amount,
    revenue,
    created_at,
    updated_at
FROM deduped_order_items
WHERE rn = 1
