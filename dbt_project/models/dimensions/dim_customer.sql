{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_full_name,
    customer_email,
    signup_date,
    marketing_opt_in,
    customer_country,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    dbt_valid_to IS NULL AS is_current
FROM {{ ref('customer_snapshot') }}
