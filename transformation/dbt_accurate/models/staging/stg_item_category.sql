
with source as (
    select * from {{ source('raw','itemcategory')}}
),

final as (
    select 
        id as item_category_id,
        name,
        defaultcategory as default_category,
        parentname as parent_name,
        extractedat as extracted_at
    from source
)

select * from final