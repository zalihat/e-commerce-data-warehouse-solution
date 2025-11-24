{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,
    product_category,
    product_price,
    reseller_id,
    created_at,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    dbt_valid_to IS NULL AS is_current
FROM {{ ref('product_snapshot') }}
