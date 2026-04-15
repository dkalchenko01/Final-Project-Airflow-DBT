{{ config(tags=['hourly']) }}

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

    o.id_order,
    o.id_manager,
    p.name as product_name,
    p.description,
    o.manager_name,
    m.phone_num as manager_phone,
    o.department,
    o.status

from source_order o

left join source_prod p on o.id_product = p.id_product

left join source_manag m on o.id_manager = m.id_manager

where o.left_in_stock = 0