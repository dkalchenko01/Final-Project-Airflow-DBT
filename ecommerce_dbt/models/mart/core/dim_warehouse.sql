{{ config(tags=['daily']) }}

with products as (

    select * from {{ ref('stg_products') }}

),

geo as (

    select * from {{ ref('stg_geo_directory') }}

),

warehouse_raw as (

    select distinct
        id_warehouse,
        warehouse_city,
        warehouse_address,
        responsible_warehouse as id_responsible
    from products

),

final as (

    select
        w.id_warehouse,
        w.warehouse_address as address,
        w.id_responsible,
        g.city_name,
        g.region_name,
        g.geo_zone,
        g.timezone,
        g.population_class,
        g.zip_code

    from warehouse_raw w
    left join geo g 
        on w.warehouse_city = g.city_name

)

select * from final
order by id_warehouse asc