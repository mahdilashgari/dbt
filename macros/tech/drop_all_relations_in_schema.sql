{# This macro will drop all tables and views in your dev schema #}
{# usage: dbt run-operation drop_all_relations_in_schema #}


{% macro drop_all_relations_in_schema() %}
    {%- set username = target.user.split('@')[0] -%}
    {%- set name, surname = username.split('.') -%}
    {%- set target_schema = 'test_' ~ name|lower ~ '_' ~ surname|lower -%}

    {{ log('Target schema "' ~ target_schema ~ '". Proceeding to drop all relations.', info=True) }}

    {% set relations = snowflake__list_relations_without_caching(target_schema) %}

    {% for relation in relations %}
        {{ log('Dropping ' ~ relation.kind ~ ':  ' ~ relation.database_name ~ '.' ~ relation.schema_name ~ '.' ~ relation.name, info=True) }}
        {# fix relation object mismatch by adding the kind attribute #}
        {% set fixed_relation = adapter.get_relation(database=relation.database_name, schema=relation.schema_name, identifier=relation.name) %}
        {% do adapter.drop_relation(fixed_relation) %}
    {% endfor %}

{% endmacro %}

