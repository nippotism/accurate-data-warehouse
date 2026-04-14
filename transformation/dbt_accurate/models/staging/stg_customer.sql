-- models/staging/stg_customer.sql
with source as (
    select * from {{ source('raw', 'customer') }}
),

final as (
    select
        id                          as customer_id,
        name                        as customer_name,
        customerno                  as customer_no,
        categoryname                as category_name,
        email,
        mobilephone                 as mobile_phone,
        workphone                   as work_phone,
        npwpno                      as npwp_no,
        pkpno                       as pkp_no,

        -- Billing Address
        billstreet                  as bill_street,
        billcity                    as bill_city,
        billprovince                as bill_province,
        billcountry                 as bill_country,
        billzipcode                 as bill_zip_code,

        -- Financial Settings
        currencycode                as currency_code,
        termname                    as term_name,
        defaultinctax               as default_inc_tax,
        customerlimitage            as customer_limit_age,
        customerlimitagevalue       as customer_limit_age_value,
        customerlimitamount         as customer_limit_amount,
        customerlimitamountvalue    as customer_limit_amount_value,

        -- Categorization
        pricecategoryname           as price_category_name,
        discountcategoryname        as discount_category_name,
        branchid                    as branch_id,
        branchname                  as branch_name,

        extractedat                 as extracted_at
    from source
)

select * from final