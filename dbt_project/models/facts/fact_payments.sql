{{ config(
    materialized='incremental',
    unique_key='payment_id'
) }}

WITH deduped_payments AS (
    SELECT
        payment_id,
        order_id,
        payment_provider,
        payment_amount,
        payment_status,
        transaction_id,
        created_at,
        updated_at,

        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM {{ ref('stg_payments') }}
    {% if is_incremental() %}
        WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    payment_id,
    order_id,
    payment_provider,
    payment_amount,
    payment_status,
    transaction_id,     
    created_at,
    updated_at
FROM deduped_payments
WHERE rn = 1
