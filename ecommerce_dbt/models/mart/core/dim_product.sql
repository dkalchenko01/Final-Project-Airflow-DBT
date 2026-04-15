{{ config(tags=['daily']) }}

{{
    config(
        materialized = 'incremental',
        unique_key = 'id_product'
      )
}}

with source as (

    select * from {{ ref('stg_products') }}

    {% if is_incremental() %}
        where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}

),

filtered as (

    select
        distinct id_product,
        id_warehouse,
        name,
        category,
        color,
        price,
        description,
        left_in_stock,
        updated_at

    from source

    order by id_product asc

)

select * from filtered
