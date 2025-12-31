{{
    config(
        snowflake_warehouse = 'xing_dbt_wh_large',
        tags = ["jobs__detail_views", "jobs__data_mart_dimension", "jobs__daily"]
    )
}}

with

central_fct_job_posting_views as (
    select * from {{ ref('int_jobs__detail_views') }}
),

central_dim_calendar as (
    select * from {{ ref('central_dim_calendar') }}
),

final as (
    select
        j.*,
        c.year_week_id  as week_id,
        c.year_month_id as month_id
    from central_fct_job_posting_views as j
        inner join central_dim_calendar as c
            on j.created_at_utc::date = c.date_id
)

select * from final
