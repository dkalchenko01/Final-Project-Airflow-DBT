{{ config(tags=['daily']) }}

with source as (

    select * from {{ source('external_source', 'raw_geo_directory') }}

),

renamed as (

    select
        city as city_name,
        region as region_name,

        cast(zip_code as varchar) as zip_code,

        area_type,
        population_class,
        timezone,
        geo_zone,

        cast(processed_at as timestamp) as loaded_at

    from source

)

select * from renamed