{% snapshot customer_snapshot %}

{{
  config(
    target_schema='Analytics',
    unique_key='customer_id',
    strategy='timestamp',
    updated_at='updated_at'
  )
}}

SELECT
    customer_id,
    customer_full_name,
    customer_email,
    signup_date,
    marketing_opt_in,
    customer_country,
    updated_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
