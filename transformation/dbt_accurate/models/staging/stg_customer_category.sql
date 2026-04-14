with source as (
    select * from {{ source('raw','customercategory')}}
),

final as (
    select
        id as customer_category_id,
        name,
        defaultcategory as default_category,
        parentname as parent_name,
        extractedat as extracted_at
    from source
)

select * from final