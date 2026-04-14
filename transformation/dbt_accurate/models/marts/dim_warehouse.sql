with stg_warehouse as (
    select * from {{ref('stg_warehouse')}}
),

final as (
    select 
        {{dbt_utils.generate_surrogate_key(['warehouse_id'])}} as id,
        warehouse_id as warehouse_nk,
        warehouse_name,
        city,
        province,
        country,
        zip_code,
        pic,
        case when scrap_warehouse then 'Scrap Warehouse'
            else 'Regular Warehouse'
        end as warehouse_type,
        case when suspended then 'Suspended'
            else 'Active'
        end as warehouse_status
    from stg_warehouse
)

select * from final