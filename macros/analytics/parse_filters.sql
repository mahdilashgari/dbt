{% macro parse_filters(filters_dict, table_name) %}
    {% set clauses = [] %}
    {% for operator, conditions in filters_dict.items() %}
        {% if operator in ['AND', 'OR'] %}
            {% set nested = [] %}
            {% for condition in conditions %}
                {% if condition.key and condition.operator and condition.value is not none %}
                    {% if condition.value is string %}
                        {% set cond = condition.key ~ " " ~ condition.operator ~ " '" ~ condition.value | replace("'", "''") ~ "'" %}
                    {% elif condition.value is iterable and condition.operator in ['IN', 'NOT IN'] %}
                        {% set value_list = condition.value | map(attribute='__str__') | list %}
                        {% set cond = condition.key ~ " " ~ condition.operator ~ " (" ~ (value_list | join(", ")) ~ ")" %}
                    {% elif condition.operator == 'BETWEEN' and condition.value | length == 2 %}
                        {% set cond = condition.key ~ " BETWEEN " ~ condition.value[0] ~ " AND " ~ condition.value[1] %}
                    {% else %}
                        {% set cond = condition.key ~ " " ~ condition.operator ~ " " ~ condition.value %}
                    {% endif %}
                    {% do nested.append(cond) %}
                {% elif condition.key is not none and condition.operator is not none and condition.value is not none %}
                    {# Handle nested logical operators recursively #}
                    {% set nested_cond = parse_filters(condition, table_name) %}
                    {% do nested.append(nested_cond) %}
                {% else %}
                    {% do exceptions.raise_compiler_error("Invalid filter condition in table " ~ table_name) %}
                {% endif %}
            {% endfor %}
            {% set combined = "(" ~ (nested | join(" " ~ operator ~ " ")) ~ ")" %}
            {% do clauses.append(combined) %}
        {% else %}
            {# Handle simple key-value pairs with default '=' operator #}
            {% set key = operator %}
            {% set value = conditions %}
            
            {% if value is string %}
                {% set condition = key ~ " = '" ~ value | replace("'", "''") ~ "'" %}
            {% else %}
                {% set condition = key ~ " = " ~ value %}
            {% endif %}
            
            {% do clauses.append(condition) %}
        {% endif %}
    {% endfor %}
    {{ return(clauses | join(" AND ")) }}
{% endmacro %}
