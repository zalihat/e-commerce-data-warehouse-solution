{% snapshot product_snapshot %}

{{
  config(
    target_schema='Analytics',     
    unique_key='product_id',        
    strategy='timestamp',           
    updated_at='updated_at'
  )
}}

SELECT
    product_id,
    product_name,
    product_category,
    product_price,
    reseller_id,
    created_at,
    updated_at
FROM {{ ref('stg_products') }}

{% endsnapshot %}
