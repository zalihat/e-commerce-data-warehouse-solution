{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

WITH ordered AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        total_amount,
        payment_method,
        coupon_code,
        channel,
        order_date,
        updated_at,

        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM {{ ref('stg_orders') }}

    {% if is_incremental() %}
        -- only load new/updated rows into the window
        WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    order_id,
    customer_id,
    order_status,
    total_amount,
    payment_method,
    coupon_code,
    channel,
    order_date,
    updated_at
FROM ordered
WHERE rn = 1
