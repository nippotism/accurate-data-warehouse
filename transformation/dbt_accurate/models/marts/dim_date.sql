-- models/marts/sales/dim_date.sql
{{
    config(
        materialized = 'table',
        tags = ['dim', 'date']
    )
}}

with date_spine as (

    {{
        dbt_utils.date_spine(
            datepart   = "day",
            start_date = "cast('" ~ var('date_spine_start') ~ "' as date)",
            end_date   = "cast('" ~ var('date_spine_end') ~ "' as date)"
        )
    }}

),

dates as (

    select
        cast(date_day as date) as date_day,

        -- ========================
        -- Day attributes
        -- ========================
        extract(dow from date_day)                      as day_of_week,        -- 0 = Sunday
        trim(to_char(date_day, 'Day'))                  as day_of_week_name,
        extract(day from date_day)                      as day_of_month,
        extract(doy from date_day)                      as day_of_year,

        -- ========================
        -- Week attributes
        -- ========================
        extract(week from date_day)                     as week_of_year,
        extract(isoyear from date_day) * 100
            + extract(week from date_day)               as iso_week_of_year,

        -- ========================
        -- Month attributes
        -- ========================
        extract(month from date_day)                    as month,
        trim(to_char(date_day, 'Month'))                as month_name,
        to_char(date_day, 'Mon')                        as month_name_short,

        -- ========================
        -- Quarter attributes
        -- ========================
        extract(quarter from date_day)                  as quarter,

        -- ========================
        -- Year attributes
        -- ========================
        extract(year from date_day)                     as year,

        -- ========================
        -- Flags
        -- ========================
        (extract(dow from date_day) in (0, 6))          as is_weekend,
        (extract(dow from date_day) not in (0, 6))      as is_weekday,

        (date_day = date_trunc('month', date_day))      as is_month_start,
        (date_day = (date_trunc('month', date_day)
            + interval '1 month - 1 day'))              as is_month_end,

        (date_day = date_trunc('quarter', date_day))    as is_quarter_start,
        (date_day = (date_trunc('quarter', date_day)
            + interval '3 months - 1 day'))             as is_quarter_end,

        (date_day = date_trunc('year', date_day))       as is_year_start,
        (date_day = (date_trunc('year', date_day)
            + interval '1 year - 1 day'))               as is_year_end

    from date_spine

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as id,
        *
    from dates

)

select * from final