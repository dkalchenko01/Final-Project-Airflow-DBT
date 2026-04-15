{{ config(tags=['daily']) }}

with source as (

    select * from {{ ref("fct_order_item") }}

)

select
    dayname(purchased_at) as purchased_day_of_week,
    count(id_order_item) as total_items,
    sum(total) as total_sales

from source

group by purchased_day_of_week

order by total_sales desc