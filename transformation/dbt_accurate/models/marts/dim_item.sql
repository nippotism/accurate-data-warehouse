
with stg_item as (
    select * from {{ ref('stg_item')}}
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['item_id'])}} as id,

        -- Natural Key
        item_id as item_nk,
        item_number,
        item_name,
        item_type,
        item_category_name,
        unit_name,
        unit_price,
        vendor_price,
        case 
            when use_ppn = true then 'PPN Applied'
            else 'No PPN'
        end as ppn_status,
        sales_gl_account_no,
        cogs_gl_account_no,

        -- SCD Type 2
        extracted_at as effective_date,
        cast(null as date) as end_date,
        true as is_current
    from stg_item
)

select * from final