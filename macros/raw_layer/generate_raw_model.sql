{#
-- This macro generates a dbt model for a single raw data table
-- it will select all the data from the corresponding snapshot
-- if history is set to true it will also contain the history from the snapshot
#}
{% macro generate_raw_model() %}
    {%- set schema_name = model.fqn[-2] -%}  {# model.fqn contains model name and path as list, index -2 is last folder #}
    {%- set raw_table_name = model.fqn[-1] | replace('_hst','') -%}  {# model.fqn contains model name and path as list, index -1 is model name #}
    {%- set snapshot_name =  raw_table_name ~ '_snapshot' -%}

    {{- config(
        schema=schema_name, 
        materialized='view',
        alias=model.fqn[-1] if target.name != 'prod' else model.fqn[-1].split("__")[1:] | join("__")
    ) 
    -}}

    with hst_view as (

            select
                {{ dbt_utils.star(ref(snapshot_name), except=['dbt_updated_at','dbt_valid_from','dbt_valid_to']) }},
                dbt_updated_at as "DBT_UPDATED_AT_UTC",
                dbt_valid_from as "DBT_VALID_FROM_UTC",
                coalesce("DBT_VALID_TO",'{{- var('max_date') -}}') as DBT_VALID_TO_UTC,
                
                iff("DBT_CHANGE_TYPE" = 'delete' and dbt_is_current, True, False) as "DBT_IS_DELETED",--add check for dbt_is_current once this change is finalized in Snapshots
                iff("DBT_IS_DELETED","DBT_VALID_TO",null) as "DBT_DELETED_AT_UTC"

            from {{ ref(snapshot_name) }}
        )
    
    select * from hst_view

{% endmacro %}