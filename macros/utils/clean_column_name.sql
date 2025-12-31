{% macro clean_column_name(column_name) %}
    {%- set re = modules.re -%}
    {%- set cleaned_parts = re.findall("[a-zA-Z_0-9]+", column_name) -%}
    {%- set cleaned_name = "_".join(cleaned_parts) | upper -%}
    {{- cleaned_name -}}
{% endmacro %}
