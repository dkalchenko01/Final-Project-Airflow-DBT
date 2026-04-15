{{ config(tags=['daily']) }}

with source as (

    select * from {{ ref("fct_order_item") }}

)

select
    date_part('day', purchased_at) as purchased_day,
    date_part('month', purchased_at) as purchased_month,
    count(id_order_item) as total_items,
    sum(total) as total_sales

from source

group by purchased_day, purchased_month