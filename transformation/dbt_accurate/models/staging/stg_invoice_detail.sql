
with source as (
    select * from {{ source('raw','invoicedetail')}}
),

final as (
    select
        detailid as detail_id,
        invoiceid as invoice_id,
        invoicenumber as invoice_number,

        --Item
        itemid as item_id,
        itemno as item_no,
        itemname as item_name,
        itemcategoryid as item_category_id,

        --Quantity & Unit
        quantity,
        unitid as unit_id,
        unitname as unit_name,
        unitratio as unit_ratio,
        unitprice as unit_price,
        grossamount as gross_amount,
        salesamount as sales_amount,

        --Warehouse
        warehouseid as warehouse_id,
        warehousename as warehouse_name,

        --Reference
        salesorderdetailid as sales_order_detail_id,
        deliveryorderdetailid as delivery_order_detail_id,

        seq,
        extractedat as extracted_at
    from source
)

select * from final