{% macro create_ratio(kpi_name, value_a, value_b) %}
    (sum({{ value_a }}) / sum({{ value_b }})) as {{ kpi_name }}
{% endmacro %}