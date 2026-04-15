{{ config(tags=['daily']) }}

with source as (

    select * from {{ source('external_source', 'raw_people') }}

),

cleaned as (

    select
        cast(id_person as integer) as id_person,
        role,
        name,
        phone_num,
        cast(ifnull(manager_id, 0) as integer) as id_manager,
        head,
        cast(start_date as date) as start_date

    from source

)

select * from cleaned
