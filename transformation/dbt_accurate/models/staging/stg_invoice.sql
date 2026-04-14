
with source as (
    select * from {{ source('raw','invoice') }}
),

final as (
    select 
        invoiceid as invoice_id,
        invoicenumber as invoice_number,

        --Date
        transdate as trans_date,
        duedate as due_date,
        shipdate as ship_date,

        --Customer
        customerid as customer_id,
        customername as customer_name,

        subtotal as sub_total,
        totalamount as total_amount,
        outstanding,

        -- Status
        status,
        approvalstatus      as approval_status,
 
        -- Reference
        ponumber            as po_number,
        salesorderid        as sales_order_id,
        deliveryorderid     as delivery_order_id,
 
        -- Payment & Currency
        paymenttermid       as payment_term_id,
        paymenttermname     as payment_term_name,
        currencyid          as currency_id,
        currencycode        as currency_code,
        rate,
 
        -- Organization
        branchid            as branch_id,
        branchname          as branch_name,
 
        -- Metadata
        invoiceagedays      as invoice_age_days,
        createdby           as created_by,
        printedtime         as printed_time,
        extractedat         as extracted_at
    from source
)
 
select * from final