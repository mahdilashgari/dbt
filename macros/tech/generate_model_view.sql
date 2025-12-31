{#
-- This macro can be run as a post-hook
-- it creates a view of a model in the workarea db
-- this makes querying data in workarea easier
-- as you do not need do change between dbs
#}
{%- macro generate_model_view(
    database_name="analytics_workarea", schema_name="", view_name=""
) -%}
    {%- if execute and target.name == "prod" -%}

        {%- if schema_name == "" -%}
            {%- set schema_name = model.fqn[-2] -%}  {# model.fqn contains model name and path as list, -2 is last folder #}
        {% endif %}

        {%- if view_name == "" -%}
            {%- set view_name = model.fqn[-1] -%}  {# model.fqn contains model name and path as list, -1 is model name #}
        {%- endif -%}

        {%- set query -%}
            CREATE OR REPLACE VIEW {{database_name}}.{{schema_name}}.{{view_name}} AS
            SELECT 
            {{ dbt_utils.star(this) }}
            FROM {{this}};
        {%- endset -%}

        {% do return(query) %}

    {%- endif -%}
{%- endmacro -%}
