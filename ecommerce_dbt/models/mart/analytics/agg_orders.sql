{{ config(tags=['daily']) }}

with source as (

    select * from {{ ref("fct_order_item") }}

)

select

    id_order,
    count(id_order_item) as num_of_items,
    min(customer_name) as customer_name,
    min(phone_num) as phone_num,
    string_agg(manager_name, ', ') as manager,
    string_agg(department, '/') as department,
    string_agg(status, '/') as status,
    sum(total) as total_for_order

from source

where status not in ('Error', 'Cancelled', 'Returned')

group by id_order