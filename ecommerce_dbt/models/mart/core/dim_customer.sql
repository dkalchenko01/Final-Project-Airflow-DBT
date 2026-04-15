{{
    config(
        materialized = 'incremental',
        unique_key = 'id_customer',
        tags=['daily']
      )
}}

with source as (

    select * from {{ ref('stg_people') }}

    {% if is_incremental() %}
        where start_date > (select max(registration_date) from {{ this }})
    {% endif %}

),

filtered as (

    select
        id_person as id_customer,
        name,
        phone_num,
        start_date as registration_date

    from source

    where role = 'customer'

)

select * from filtered
