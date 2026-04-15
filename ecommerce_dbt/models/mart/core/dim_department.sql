{{ config(tags=['daily']) }}

with source as (

    select * from {{ ref('stg_departments') }}

),

filtered as (

    select
        id_department,
        name,
        id_head as head_manager

    from source

)

select * from filtered
