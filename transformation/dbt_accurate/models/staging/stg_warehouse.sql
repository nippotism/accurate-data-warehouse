-- models/staging/stg_warehouse.sql
with source as (
    select * from {{ source('raw', 'warehouse') }}
),

renamed as (
    select
        id              as warehouse_id,
        name            as warehouse_name,
        street,
        city,
        province,
        country,
        zipcode         as zip_code,
        pic,
        scrapwarehouse  as scrap_warehouse,
        suspended,
        extractedat     as extracted_at
    from source
)

select * from renamed