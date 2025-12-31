{{ config(
    tags = ["jobs__industries", "jobs__data_mart_dimension", "jobs__daily"]
) }}

with industries as (
    select *
    from {{ ref('int_jobs__industries') }}
),

final as (
    select *
    from industries
)

select * from final
