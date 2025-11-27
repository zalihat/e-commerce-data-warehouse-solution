USE DATABASE ECOMMERCE_DW;
use schema raw;
-- create customer external table. Table will be used as dbt source
CREATE
OR REPLACE EXTERNAL TABLE raw.customers_ext (
    customer_id NUMBER AS (value:customer_id::NUMBER),
    first_name STRING AS (value:first_name::STRING),
    last_name STRING AS (value:last_name::STRING),
    email STRING AS (value:email::STRING),
    signup_date DATE AS (value:signup_date::DATE),
    marketing_opt_in STRING AS (value:marketing_opt_in::STRING),
    country STRING AS (value:country::STRING),
    created_at TIMESTAMP_NTZ AS (value:created_at::TIMESTAMP_NTZ),
    updated_at TIMESTAMP_NTZ AS (value:updated_at::TIMESTAMP_NTZ)
) WITH LOCATION = @s3_ecommerce_stage/customers/ FILE_FORMAT = (TYPE = PARQUET) AUTO_REFRESH = FALSE;
SELECT
    *
FROM
    raw.customers_ext;
LIMIT
    5;
    -- Create an external table for payment that will be used by dbt as source
    CREATE
    OR REPLACE EXTERNAL TABLE raw.payments_ext (
        payment_id NUMBER AS (value:payment_id::NUMBER),
        order_id NUMBER AS (value:order_id::NUMBER),
        provider STRING AS (value:provider::STRING),
        amount FLOAT AS (value:amount::FLOAT),
        status STRING AS (value:status::STRING),
        transaction_id STRING AS (value:transaction_id::STRING),
        created_at TIMESTAMP_NTZ AS (value:created_at::TIMESTAMP_NTZ),
        updated_at TIMESTAMP_NTZ AS (value:updated_at::TIMESTAMP_NTZ)
    ) WITH LOCATION = @s3_ecommerce_stage/payments/ FILE_FORMAT = (TYPE = PARQUET) AUTO_REFRESH = FALSE;
select
    *
from
    raw.payments_ext;
-- Create an external table for shipments that will be used by dbt as source
    CREATE
    OR REPLACE EXTERNAL TABLE raw.shipments_ext (
        shipment_id NUMBER AS (value:shipment_id::NUMBER),
        order_id NUMBER AS (value:order_id::NUMBER),
        carrier STRING AS (value:carrier::STRING),
        tracking_number STRING AS (value:tracking_number::STRING),
        shipped_date TIMESTAMP_NTZ AS (value:shipped_date::TIMESTAMP_NTZ),
        delivered_date TIMESTAMP_NTZ AS (value:delivered_date::TIMESTAMP_NTZ),
        created_at TIMESTAMP_NTZ AS (value:created_at::TIMESTAMP_NTZ),
        updated_at TIMESTAMP_NTZ AS (value:updated_at::TIMESTAMP_NTZ)
    ) WITH LOCATION = @s3_ecommerce_stage/shipments/ FILE_FORMAT = (TYPE = PARQUET) AUTO_REFRESH = FALSE;
select
    *
from
    raw.shipments_ext
limit
    2;
-- Create an external table for resellers that will be used by dbt as source
    CREATE
    OR REPLACE EXTERNAL TABLE raw.resellers_ext (
        reseller_id NUMBER AS (value:reseller_id::NUMBER),
        name STRING AS (value:name::STRING),
        contact_email STRING AS (value:contact_email::STRING),
        country STRING AS (value:country::STRING),
        created_at TIMESTAMP_NTZ AS (value:created_at::TIMESTAMP_NTZ),
        updated_at TIMESTAMP_NTZ AS (value:updated_at::TIMESTAMP_NTZ)
    ) WITH LOCATION = @s3_ecommerce_stage/resellers/ FILE_FORMAT = (TYPE = PARQUET) AUTO_REFRESH = FALSE;
select
    *
from
    raw.resellers_ext
limit
    2;
-- Create an external table for products that will be used by dbt as source
    CREATE
    OR REPLACE EXTERNAL TABLE raw.products_ext (
        product_id NUMBER AS (value:product_id::NUMBER),
        name STRING AS (value:name::STRING),
        category STRING AS (value:category::STRING),
        price FLOAT AS (value:price::FLOAT),
        reseller_id NUMBER AS (value:reseller_id::NUMBER),
        created_at TIMESTAMP_NTZ AS (value:created_at::TIMESTAMP_NTZ),
        updated_at TIMESTAMP_NTZ AS (value:updated_at::TIMESTAMP_NTZ)
    ) WITH LOCATION = @s3_ecommerce_stage/products/ FILE_FORMAT = (TYPE = PARQUET) AUTO_REFRESH = FALSE;
select
    *
from
    raw.products_ext
limit
    2;
-- Create an external table for orders that will be used by dbt as source
    CREATE
    OR REPLACE EXTERNAL TABLE raw.orders_ext (
        order_id NUMBER AS (value:order_id::NUMBER),
        customer_id NUMBER AS (value:customer_id::NUMBER),
        order_date TIMESTAMP_NTZ AS (value:order_date::TIMESTAMP_NTZ),
        status STRING AS (value:status::STRING),
        total_amount FLOAT AS (value:total_amount::FLOAT),
        payment_method STRING AS (value:payment_method::STRING),
        coupon_code STRING AS (value:coupon_code::STRING),
        channel STRING AS (value:channel::STRING),
        created_at TIMESTAMP_NTZ AS (value:created_at::TIMESTAMP_NTZ),
        updated_at TIMESTAMP_NTZ AS (value:updated_at::TIMESTAMP_NTZ)
    ) WITH LOCATION = @s3_ecommerce_stage/orders/ FILE_FORMAT = (TYPE = PARQUET) AUTO_REFRESH = FALSE;
select
    *
from
    raw.orders_ext
limit
    2;
-- Create an external table for order items that will be used by dbt as source
    CREATE
    OR REPLACE EXTERNAL TABLE raw.order_items_ext (
        order_item_id NUMBER AS (value:order_item_id::NUMBER),
        order_id NUMBER AS (value:order_id::NUMBER),
        product_id NUMBER AS (value:product_id::NUMBER),
        quantity NUMBER AS (value:quantity::NUMBER),
        unit_price FLOAT AS (value:unit_price::FLOAT),
        discount FLOAT AS (value:discount::FLOAT),
        created_at TIMESTAMP_NTZ AS (value:created_at::TIMESTAMP_NTZ),
        updated_at TIMESTAMP_NTZ AS (value:updated_at::TIMESTAMP_NTZ)
    ) WITH LOCATION = @s3_ecommerce_stage/order_items/ FILE_FORMAT = (TYPE = PARQUET) AUTO_REFRESH = FALSE;
select
    *
from
    raw.order_items_ext
limit
    2;