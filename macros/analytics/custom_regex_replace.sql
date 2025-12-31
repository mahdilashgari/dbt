{% macro custom_regex_replace(string, pattern, replacement) %}
    {# Import the Python 're' module #}
    {% set re = modules.re %}
    {# Perform regex substitution #}
    {% do return(re.sub(pattern, replacement, string)) %}
{% endmacro %}
