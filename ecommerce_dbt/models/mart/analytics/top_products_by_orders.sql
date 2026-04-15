{{ config(tags=['daily']) }}

with source_order as (

    select * from {{ ref("fct_order_item") }}

),

source_prod as (

    select * from {{ ref('dim_product') }}

)

select

    p.name as product_name,
    count(o.id_order_item) as total_orders,
    sum(o.total) as total_sales,
    min(p.description) as description,
    min(p.color) as color,
    min(o.department) as department

from source_order o

left join source_prod p on o.id_product = p.id_product

group by product_name

order by total_orders desc