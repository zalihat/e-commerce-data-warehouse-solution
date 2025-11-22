select
    reseller_id,
    name as reselller_name,
    country as reseller_country,
    created_at as reseller_join_date,
    contact_email as reseller_email
from {{source('raw', 'resellers_ext')}}
