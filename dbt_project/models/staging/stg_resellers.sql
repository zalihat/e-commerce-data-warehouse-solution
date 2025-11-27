select
    reseller_id,
    name as reseller_name,
    country as reseller_country,
    created_at as reseller_join_date,
    contact_email as reseller_email,
    updated_at
from {{source('raw', 'resellers_ext')}}
