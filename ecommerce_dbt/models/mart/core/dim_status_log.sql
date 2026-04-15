{{ config(tags=['hourly']) }}

{{
    config(
        materialized = 'incremental',
        unique_key = 'id_status'
      )
}}

with source as (

    select * from {{ ref('stg_status_logs') }}

    {% if is_incremental() %}
        where status_timestamp > (select max(status_timestamp) from {{ this }})
    {% endif %}

),

shipped_time as (

    select
        id_order_item,
        max(status_timestamp) as shipped_at

    from source

    where status = 'Shipped'

    group by id_order_item

),

latest as (

    select
        s.id_order_item,
        s.id_status,
        s.status,
        s.status_timestamp,
        t.shipped_at

    from source s

    left join shipped_time t on s.id_order_item = t.id_order_item

    qualify row_number() over (partition by s.id_order_item order by s.status_timestamp desc) = 1

)

select * from latest
