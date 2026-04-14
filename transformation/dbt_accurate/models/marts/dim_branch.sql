
with stg_branch as (
    select * from {{ ref('stg_branch')}}
),

final as (
    select
        {{dbt_utils.generate_surrogate_key(['branch_id'])}} as id,
        branch_id as branch_nk,
        name as branch_name,  
        street,
        city,  
        province,
        country,
        zip_code
    from stg_branch
)

select * from final