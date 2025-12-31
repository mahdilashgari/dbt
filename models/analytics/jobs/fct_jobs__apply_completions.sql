{{
    config(
        alias = 'fct_jobs__apply_completions_v2',
        tags = ["jobs__apply_completions", "jobs__data_mart_dimension", "jobs__daily"],
        post_hook = "GRANT SELECT ON TABLE {{ this }} TO SHARE nwse.dwh.bi_prod_analytics_db_dis_share"
    )
}}

with

apply_completions as (
    select * from {{ ref('int_jobs__apply_completions') }}
),

central_dim_calendar as (
    select * from {{ ref('central_dim_calendar') }}
),

final as (
    select
        ca.*,
        dc.year_week_id  as week_id,
        dc.year_month_id as month_id
    from apply_completions as ca
        left join central_dim_calendar as dc
            on ca.created_at_utc::date = dc.date_id
)

select * from final
