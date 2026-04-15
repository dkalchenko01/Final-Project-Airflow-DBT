{{ config(tags=['daily']) }}

with source_order as (

    select * from {{ ref("fct_order_item") }}

),

source_prod as (

    select * from {{ ref('dim_product') }}

),

source_manag as (

    select * from {{ ref('dim_manager') }}

)

select

    p.name as product_name,
    count(o.id_order_item) as num_of_declines,
    string_agg(o.status, ', ') as status,
    min(p.description) as description,
    min(p.color) as color,
    string_agg(o.manager_name, ', ') as manager,
    string_agg(m.phone_num, ', ') as manager_phone,
    min(o.department) as department,
    sum(o.total) as total_loss

from source_order o

left join source_prod p on o.id_product = p.id_product

left join source_manag m on o.id_manager = m.id_manager

where o.status in ('Cancelled', 'Returned', 'Error')

group by product_name

order by num_of_declines desc