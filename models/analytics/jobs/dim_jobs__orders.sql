{{
  config(
    tags = ["jobs__orders", "jobs__data_mart_dimension", "jobs__daily"]
    )
}}

with orders as (
    select *
    from {{ ref('int_jobs__orders') }}
),

final as (
    select *
    from orders
)

select * from final
