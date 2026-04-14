
with stg_customer as (
    select * from {{ ref('stg_customer')}}
),

final as (
    select 
        {{dbt_utils.generate_surrogate_key(['customer_id'])}} as id,
        customer_id as customer_nk,
        customer_name,
        customer_no,
        category_name,
        npwp_no,
        pkp_no,
        email,
        mobile_phone,

        -- Address
        bill_city           as city,
        bill_province       as province,
        bill_country        as country,
        bill_zip_code       as zip_code,

        customer_limit_amount,
        customer_limit_age,
        default_inc_tax,
        term_name,
        currency_code,
        
        -- SCD Type 2
        extracted_at as effective_date,
        cast(null as date) as end_date,
        true as is_current
    from stg_customer
)

select * from final