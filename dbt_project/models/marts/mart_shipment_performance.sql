-- select 
--     shipping_company, 
--     count(distinct shipment_id) as shipments,
--     avg(delivery_days) as avg_delivery_days,
--     avg(delivery_hours) as avg_delivery_hours,
--     count(*) filter (where o.order_status = 'DELIVERED') as successful_shipments,
--     count(*) filter (where o.order_status = 'DELIVERED') 
--         / count(*)::float as shipment_success_rate
-- from {{ref('int_shipping_information')}} as s
-- inner join {{ref('stg_orders')}} as o
-- on s.order_id = o.order_id
-- group by shipping_company
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

from {{ ref('int_shipping_information') }} as s
inner join {{ ref('stg_orders') }} as o
    on s.order_id = o.order_id
group by shipping_company
