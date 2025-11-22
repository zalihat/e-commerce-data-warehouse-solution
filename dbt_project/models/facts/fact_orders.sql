{{ config(
    materialized='incremental',
    unique_key='order_id'
) }}

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
FROM {{ ref('stg_orders') }}

{% if is_incremental() %}
    WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
