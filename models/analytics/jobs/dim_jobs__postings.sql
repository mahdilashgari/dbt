{{ config(
    snowflake_warehouse = 'xing_dbt_wh_xxxlarge',
    tags = ["jobs__postings", "jobs__data_mart_dimension", "jobs__daily"]
) }}

with postings as (
    select *
    from {{ ref('int_jobs__postings') }}
),

final as (
    select *
    from postings
)

select * from final
