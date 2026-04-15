{{ config(tags=['hourly']) }}

with source as (

    select * from {{ source('external_source', 'raw_status_logs') }}

),

cleaned as (

    select
        cast(id_order_item as integer) as id_order_item,
        cast(id_status as integer) as id_status,
        status,
        cast(status_timestamp as timestamp) as status_timestamp

    from source

)

select * from cleaned
