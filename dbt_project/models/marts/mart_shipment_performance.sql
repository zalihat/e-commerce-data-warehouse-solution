with shipment_info as (
    select 
        shipment_id,
        order_id,
        shipping_company,
        datediff('day', shipped_date, delivered_date) as delivery_days,
        datediff('hour', shipped_date, delivered_date) as delivery_hours
from {{ref('fact_shipments')}})

select 
    shipping_company, 
    count(distinct shipment_id) as shipments,
    avg(delivery_days) as avg_delivery_days,
    avg(delivery_hours) as avg_delivery_hours,
    -- successful shipments
    sum(case when o.order_status = 'DELIVERED' then 1 else 0 end) 
        as successful_shipments,
    -- success rate
    sum(case when o.order_status = 'DELIVERED' then 1 else 0 end)::float
        / count(*) as shipment_success_rate

from shipment_info as s
inner join {{ ref('fact_orders') }} as o
    on s.order_id = o.order_id
group by shipping_company
