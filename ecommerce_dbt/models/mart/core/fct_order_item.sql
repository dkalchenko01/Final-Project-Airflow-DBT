{{ config(tags=['hourly']) }}

{{
    config(
        materialized = 'incremental',
        unique_key = 'id_order_item',
        incremental_strategy = 'merge',
        incremental_predicates=["DBT_INTERNAL_DEST.purchased_at >= current_date - interval '7 days'"]
      )
}}

with source_order as (

    select * from {{ ref('stg_order_items') }}

    {% if is_incremental() %}
        where purchase_date > (select max(purchased_at) from {{ this }})
    {% endif %}

),

source_manag as (

    select * from {{ ref('dim_manager') }}

),

source_dep as (

    select * from {{ ref('dim_department') }}

),

source_prod as (

    select * from {{ ref('dim_product') }}

),

source_warehouse as (

    select * from {{ ref('dim_warehouse') }}

),

source_cust as (

    select * from {{ ref('dim_customer') }}

),

source_stat as (

    select * from {{ ref('dim_status_log') }}

),

joined as (

    select

        o.id_order_item,
        o.id_order,
        o.id_product,
        o.id_customer,
        o.id_manager,
        p.id_warehouse,
        c.name as customer_name,
        c.phone_num,
        m.name as manager_name,
        d.name as department,
        o.quantity,
        o.price,
        o.purchase_date as purchased_at,
        cast(s.shipped_at as date) as shipped_at,
        s.status,
        o.payment_method,
        o.shipping_method,
        o.discount,
        p.left_in_stock,
        ((o.quantity * o.price) - o.discount * 0.01) as total

    from source_order o

    left join source_manag m on o.id_manager = m.id_manager

    left join source_dep d on m.id_department = d.id_department

    left join source_prod p on o.id_product = p.id_product

    left join source_warehouse w on p.id_warehouse = w.id_warehouse

    left join source_cust c on o.id_customer = c.id_customer

    left join source_stat s on o.id_order_item = s.id_order_item

)

select
    * replace (
        case
            when quantity == 0 then 'Error'
            else status
        end as status
    )

from joined


