{{
  config(
    tags = ["jobs__organizations", "jobs__data_mart_dimension", "jobs__daily"]
    )
}}

with organizations as (
    select *
    from {{ ref('stg_jobs__job_organisations') }}
),

final as (
    select *
    from organizations
)

select * from final
