{{ config(tags=['daily']) }}

with source_order as (

    select * from {{ ref("fct_order_item") }}

),

source_manag as (

    select * from {{ ref('dim_manager') }}

),

grouped as (

    select

        o.id_manager,
        min(o.manager_name) as manager_name,
        min(o.department) as department,
        min(m.phone_num) as phone_num,
        min(m.id_head) as head_manager,
        min(m.start_date) as start_date,
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

    group by o.id_manager

)

select
    rank() over (partition by department order by total_sales desc) as rating_by_sales_in_dep,
    *
from grouped