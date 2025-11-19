select 
    customer_id,
    first_name,
    last_name,
    email,
    signup_date,
    marketing_opt_in,
    country,
    created_at,
    updated_at

from {{ source('raw', 'customers_ext') }}

