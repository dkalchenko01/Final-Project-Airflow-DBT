{{ config(tags=['daily']) }}

with source as (

    select * from {{ ref('raw_departments') }}

),

renamed as (

    select
        cast(id as integer) as id_department,
        name,
        cast(head_id as integer) as id_head

    from source

)

select * from renamed
