select 
    customer_id,
    first_name as customer_name,
    last_name as customer_last,
    concat(first_name, ' ', last_name) as customer_full_name,
    email as customer_email,
    signup_date,
    marketing_opt_in,
    country as customer_country,
    created_at,
    updated_at

from {{ source('raw', 'customers_ext') }}

