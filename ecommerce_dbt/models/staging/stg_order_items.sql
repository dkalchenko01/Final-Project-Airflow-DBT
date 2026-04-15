{{ config(tags=['hourly']) }}

with source as (

    select * from {{ source('external_source', 'raw_order_items') }}

),

cleaned as (

    select
        cast(id_order_item as integer) as id_order_item,
        cast(id_order as integer) as id_order,
        cast(id_product as integer) as id_product,
        cast(id_customer as integer) as id_customer,
        cast(id_manager as integer) as id_manager,
        cast(ifnull(quantity, 0) as integer) as quantity,
        cast(price as integer) as price,
        cast(purchase_date as date) as purchase_date,
        payment_method,
        shipping_method,
        cast(discount as integer) as discount

    from source

)

select * from cleaned
