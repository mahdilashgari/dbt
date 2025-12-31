{# macro is used in tests to select same warehouse as configured for model #}
{% macro use_warehouse(model) %}
    {% if execute %}
        {% set node = (graph.nodes.values() | selectattr("alias", "equalto", model.name) | first) %}
        {% set warehouse = node.config.snowflake_warehouse | trim %}
        {% if warehouse != '' %}
            {% do run_query("use warehouse " ~ warehouse)  %}
        {% endif %}        
    {% endif %}
{% endmacro %}