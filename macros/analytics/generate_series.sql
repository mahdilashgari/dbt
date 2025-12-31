{% macro generate_series(upper_bound) %}

    {% for n in range(1, upper_bound+1) %}
        select
            '{{ n }}' as cur_iter
        {% if not loop.last %} union all {% endif %}
    {% endfor %}

{% endmacro %}
