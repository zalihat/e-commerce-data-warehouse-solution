{{ config(materialized='table') }}

SELECT
    reseller_id,
    reseller_name,
    reseller_country,
    reseller_join_date,
    reseller_email,
    dbt_valid_from AS valid_from,
    dbt_valid_to AS valid_to,
    dbt_valid_to IS NULL AS is_current
FROM {{ ref('reseller_snapshot') }}
