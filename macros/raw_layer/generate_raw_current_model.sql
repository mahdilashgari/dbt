{#
-- This macro generates a dbt model that contains the current data for a single raw data table
-- it will select all the data from the corresponding raw history model 
#}
{% macro generate_raw_current_model() %}
    {%- set schema_name = model.fqn[-2] -%}  {# model.fqn contains model name and path as list, index -2 is last folder #}
    {%- set raw_table_name = model.fqn[-1] | replace('_hst','') -%}  {# model.fqn contains model name and path as list, index -1 is model name #}
    {%- set snapshot_name =  raw_table_name ~ '_snapshot' -%}

    {{- config(
        schema=schema_name, 
        materialized='view',
        alias=model.fqn[-1].split("__")[1:] | join("__")
    ) 
    -}}

    {{ generate_raw_model() }}
    where dbt_is_current = true

{% endmacro %}