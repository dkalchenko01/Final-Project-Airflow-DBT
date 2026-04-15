{{ config(tags=['daily']) }}

with source as (

    select * from {{ source('external_source', 'raw_products') }}

),

cleaned as (

    select
        cast(id as integer) as id_product,
        cast(id_warehouse as integer) as id_warehouse,
        warehouse_city,
        warehouse_address,
        cast(responsible_warehouse as integer) as responsible_warehouse,
        name,
        color,
        category,
        price,
        description,
        cast(ifnull(left_in_stock, 0) as integer) as left_in_stock,
        cast(updated_at as date) as updated_at

    from source

)

select * from cleaned
