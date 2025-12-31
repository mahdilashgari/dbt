{# 
    This macro takes a relation and an optional list of columns to exclude and returns a comma-separated list of columns in the relation.
    The exclude list is case-insensitive.
 #}
{% macro list_columns(relation, exclude=[]) %}
  
    {% set columns = adapter.get_columns_in_relation(relation) %}

    {% set filtered_columns = [] %}
    {% set exclude_list = [] %}

    {% for item in exclude %}
        {% do exclude_list.append(item.lower()) %}
    {% endfor %}
    
        {% for column in columns %}
    
            {% set column_name = column.column.lower() %}
        
            {% if not column_name in exclude_list %}
                {% do filtered_columns.append(column_name) %}
            {% endif %}
        
        {% endfor %}
    
        {{ return(filtered_columns) }}

{% endmacro %}