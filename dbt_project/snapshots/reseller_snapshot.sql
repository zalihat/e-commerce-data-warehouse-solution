{% snapshot reseller_snapshot %}

{{
  config(
    target_schema='Analytics',      
    unique_key='reseller_id',     
    strategy='timestamp',           
    updated_at='updated_at'
  )
}}

SELECT
    reseller_id,
    reseller_name,
    reseller_country,
    reseller_join_date,
    reseller_email,
    updated_at
FROM {{ ref('stg_resellers') }}

{% endsnapshot %}
