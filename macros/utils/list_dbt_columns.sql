{# 
    This macro is to generate column list from provided table based on prefix filter.
    Use-case: list of columns to exclude in select * exclude(column1, column2, column3) from table_name
 #}
{% macro list_dbt_columns(relation, exclude=[], prefix='dbt_') %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set filtered_columns = [] %}
    {% for column in columns %}
        {% set column_name = column.column.lower() %}
        {% if not column_name in exclude and column_name.startswith(prefix.lower()) %}
            {% do filtered_columns.append(column_name) %}
        {% endif %}
    {% endfor %}
    {{ return(filtered_columns | join(', ')) }}
{% endmacro %}