{% macro generate_pit_query(tables) %}
{% set unioned_selects = [] %}
with
{% for table in tables %}
{{ table.name }}_hst as (
    select
        {{ table.id_column }} as xing_user_id,
        dbt_scd_id,
        dbt_updated_at_utc::date as load_date
    from {{ table.source }}
    {% if table.where %}
    where {{ table.where }}
    {% endif %}
    qualify row_number() over (partition by xing_user_id, load_date order by dbt_updated_at_utc desc) = 1
),
{% set union_select = "select xing_user_id, load_date from " + table.name %}
{% do unioned_selects.append(union_select) %}
{% endfor %}

load_dates as (
    {{ unioned_selects | join(' union\n    ') }}
),

pit as (
    select
        load_dates.xing_user_id,
        load_dates.load_date as valid_from,
        lead(load_dates.load_date)
            over (partition by load_dates.xing_user_id order by load_dates.load_date)
            as valid_to,
        {% for table in tables %}
        first_value({{ table.name }}.dbt_scd_id) ignore nulls over (
            partition by load_dates.xing_user_id
            order by load_dates.load_date desc
            rows between current row and unbounded following
        ) as {{ table.name }}_scd_id
        {% if not loop.last %},{% endif %}
        {% endfor %}
    from load_dates
    {% for table in tables %}
    left join {{ table.name }}
        on load_dates.xing_user_id = {{ table.name }}.xing_user_id and load_dates.load_date = {{ table.name }}.load_date
    {% endfor %}
)

select * from pit
{% endmacro %}
