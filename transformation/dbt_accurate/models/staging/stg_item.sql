-- models/staging/stg_item.sql
with source as (
    select * from {{ source('raw', 'item') }}
),

renamed as (
    select
        id                      as item_id,
        no                      as item_number,
        name                    as item_name,
        itemtype                as item_type,
        itemcategoryname        as item_category_name,
        upcno                   as upc_no,

        -- Pricing & Units
        unitprice               as unit_price,
        vendorprice             as vendor_price,
        unit1name               as unit_name,
        vendorunitname          as vendor_unit_name,
        defaultdiscount         as default_discount,

        -- Flags
        useppn                  as use_ppn,
        managesn                as manage_sn,
        controlquantity         as control_quantity,

        -- GL Accounts
        salesglaccountno        as sales_gl_account_no,
        cogsglaccountno         as cogs_gl_account_no,
        inventoryglaccountno    as inventory_gl_account_no,

        -- Physical
        weight,
        notes,
        extractedat             as extracted_at
    from source
)

select * from renamed