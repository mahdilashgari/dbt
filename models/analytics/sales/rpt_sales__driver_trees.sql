{{
  config(
    tags=["mart_sales"]
    )
}}

-- This is a reporting model for the Driver Trees
-- Individual int models calculate the required metrics and this model puts them together:

with customer_kpis as (select * from {{ ref('int_sales__driver_trees_customer_kpis') }}),

contract_values_kpis as (select * from {{ ref('int_sales__driver_trees_contract_value_kpis') }}),

accounts as (select * from {{ ref('int_sales__accounts') }}),

pre_final as (

    select * from customer_kpis
    union all
    select * from contract_values_kpis

),

final as (

    select
        pf.reporting_month_start_date,
        pf.customer_id,
        a.account_name        as customer_name,
        pf.application_name,
        pf.product_name,
        pf.kpi_measure,
        pf.kpi_category,
        '    ' || pf.kpi_name as kpi_name,
        pf.show_on_customer_level,
        pf.show_on_product_level,
        pf.kpi_value,
        case
            when pf.kpi_category = 'Active in Month' then 1
            when pf.kpi_category = 'BOP' then 2
            when pf.kpi_category = 'Gross Adds' then 3
            when pf.kpi_category = 'Gross Losses' then 4
            when pf.kpi_category = 'Retained' then 5
            when pf.kpi_category = 'EOP' then 6
            when pf.kpi_category = 'Free-to-...' then 7
            when pf.kpi_category = 'Actual Quits' then 8
            else 99
        end                   as kpi_category_table_order,
        case
            when pf.kpi_name like 'Customer%' then 1
            when pf.kpi_name like 'Product%' then 2
            else 99
        end                   as kpi_name_table_order
    from pre_final as pf
        left join accounts as a on pf.customer_id = a.account_id

)

select * from final
