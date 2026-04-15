{{ config(tags=['daily']) }}

with source_order as (

    select * from {{ ref("fct_order_item") }}

),

source_manag as (

    select * from {{ ref('dim_manager') }}

),

source_dep as (

    select * from {{ ref('dim_department') }}

),

grouped_dep as (

    select
        id_department,
        count(id_manager) as managers_per_dep

    from source_manag

    group by id_department

)

select

    o.department,
    max(d.head_manager) as head_manager,
    max(g.managers_per_dep) as managers_per_dep,
    count(o.id_order_item) as num_of_items,
    sum(o.total) as total_sales,
    count_if(o.status in ('Shipped', 'Delivered')) as successful_orders,
    count_if(o.status in ('Cancelled', 'Returned', 'Error')) as unsuccessful_orders,
    count_if(o.payment_method = 'Credit Card') as credit_card,
    count_if(o.payment_method = 'PayPal') as paypal,
    count_if(o.payment_method = 'Bank Transfer') as bank_transfer,
    count_if(o.payment_method = 'Cash') as cash,
    count_if(o.shipping_method = 'Express') as express_shipping,
    count_if(o.shipping_method = 'Standard') as standard_shipping,
    count_if(o.shipping_method = 'Pickup') as pickup_shipping,
    count_if(o.discount != 0) as num_of_discounts

from source_order o

left join source_manag m on o.id_manager = m.id_manager

left join grouped_dep g on m.id_department = g.id_department

left join source_dep d on m.id_department = d.id_department

group by o.department

order by total_sales desc