
with source as (
    select * from {{ source('raw','branch')}}
),

final as (
    select
        id as branch_id,
        name,  
        street,
        city,  
        province,
        country,
        zipcode as zip_code,
        extractedat as extracted_at
    from source
)

select * from final