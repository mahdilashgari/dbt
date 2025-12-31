{{
    config(
        alias = 'fct_jobs__apply_intentions_v2',
        tags = ["jobs__apply_intentions", "jobs__data_mart_dimension", "jobs__daily"]
    )
}}

with

apply_intentions as (
    select * from {{ ref('int_jobs__apply_intentions') }}
),

central_dim_calendar as (
    select * from {{ ref('central_dim_calendar') }}
),

final as (
    select
        ca.*,
        dc.year_week_id  as week_id,
        dc.year_month_id as month_id
    from apply_intentions as ca
        left join central_dim_calendar as dc
            on ca.created_at_utc::date = dc.date_id
)

select * from final
