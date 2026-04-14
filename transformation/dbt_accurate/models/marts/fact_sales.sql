
with stg_invoice_detail as (
    select * from {{ ref('stg_invoice_detail')}}
),

stg_invoice as (
    select * from {{ ref('stg_invoice')}}
),

dim_item as (
    select id, item_nk
    from {{ ref('dim_item')}}
    where is_current = true
),

dim_customer as (
    select id, customer_nk
    from {{ ref('dim_customer')}}
    where is_current = true
),

dim_branch as (
    select id, branch_nk
    from {{ ref('dim_branch')}}
),

dim_warehouse as (
    select id, warehouse_nk
    from {{ ref('dim_warehouse')}}
),

dim_date as (
    select id, date_day as date
    from {{ ref('dim_date')}}
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['detail_id'])}} as id,

        --Natural Key
        detail_id as detail_nk,

        -- Foreign Keys

        -- Date
        dd_inv.id as inv_date_id,
        dd_due.id as due_date_id,
        dd_ship.id as ship_date_id,

        -- Item
        i.id as item_id,

        -- Customer
        c.id as customer_id,

        -- Branch
        b.id as branch_id,

        -- Warehouse
        w.id as warehouse_id,

        -- Measures
        d.quantity,
        d.unit_ratio,
        d.unit_price,
        d.gross_amount,
        d.sales_amount,

        -- Audit
        current_timestamp as created_at,
        current_timestamp as updated_at

    from
        stg_invoice_detail d

    -- Join with Invoice Header to get the date and branch information
    inner join stg_invoice h
        on d.invoice_id = h.invoice_id

    -- Join with Date Dimension via Invoice Header
    left join dim_date dd_inv
        on h.trans_date = dd_inv.date

    left join dim_date dd_due
        on h.due_date = dd_due.date

    left join dim_date dd_ship
        on h.ship_date = dd_ship.date


    -- Join with Dimension Tables via Invoice Detail
    left join dim_item i
        on d.item_id = i.item_nk

    -- Join with Customer Dimension via Invoice Header
    left join dim_customer c
        on h.customer_id = c.customer_nk
    
    -- Join with Branch Dimension via Invoice Header
    left join dim_branch b
        on h.branch_id = b.branch_nk
    
    -- Join with Warehouse Dimension via Invoice Detail
    left join dim_warehouse w
        on d.warehouse_id = w.warehouse_nk        
)

select * from final