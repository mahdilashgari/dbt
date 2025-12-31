{{ config(
    materialized = "table",
    tags = ["jobs__visible_postings", "jobs__data_mart_dimension", "jobs__daily"]
) }}

with visible_postings_daily as (
    select * from {{ ref('int_jobs__job_visible_daily') }}
),

final as (
    select * rename (posting_id as job_posting_id)
    from visible_postings_daily
)

select * from final
