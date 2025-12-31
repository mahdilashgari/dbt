{{
    config(
        snowflake_warehouse = 'xing_dbt_wh_large',
        tags = ["jobs__data_mart_dimension", "jobs__daily"]
    )
}}

with searches as (
    select *
    from {{ ref('stg_user_tracking__job_search_events') }}
),

final as (
    select *
    from searches
)

select * from final
