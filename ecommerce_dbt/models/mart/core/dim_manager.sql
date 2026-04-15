{{ config(tags=['daily']) }}

{{
    config(
        materialized = 'incremental',
        unique_key = 'id_manager'
      )
}}

with source_ppl as (

    select * from {{ ref('stg_people') }}

    {% if is_incremental() %}
        where start_date > (select max(start_date) from {{ this }})
    {% endif %}

),

source_dep as (

    select * from {{ ref('stg_departments') }}

),

filtered as (

    select
        p.id_person as id_manager,
        d.id_department,
        p.name,
        p.phone_num,
        p.head as id_head,
        p.start_date

    from source_ppl p

    left join source_dep d on p.head = d.id_head

    where (p.role == 'manager') or (p.role == 'top-manager')

)

select * replace (
    case
        when id_head = 0 then id_manager
        else id_head
    end as id_head
)
from filtered
