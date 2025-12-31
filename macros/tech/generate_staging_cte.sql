{#
    This macro generates select statement for the first CTE of staging models.
    The purpose is to make transformations of technical fields easy and standard.
#}
{% macro generate_staging_cte(snapshot_name) %}
    select
        * exclude (dbt_unique_sk, dbt_invocation_id, dbt_updated_at, dbt_valid_from, dbt_valid_to, dbt_change_type, dbt_is_current),
        dbt_valid_from                                as dbt_valid_from_utc,
        coalesce(dbt_valid_to, '9999-12-31 23:59:59') as dbt_valid_to_utc,
        dbt_is_current,
        dbt_change_type = 'delete' and dbt_is_current as dbt_is_deleted,
        iff(dbt_is_deleted, dbt_valid_to, null)       as dbt_deleted_at_utc,
        dbt_is_current and not dbt_is_deleted         as dbt_is_valid_and_current,
        dbt_updated_at                                as dbt_updated_at_utc
    from
        {{ ref(snapshot_name) }}
{% endmacro %}

{%- macro list_dbt_columns_for_staging(alias=None) -%}
    {%- if alias is not none -%}
        {{ alias }}.dbt_valid_from_utc,
        {{ alias }}.dbt_valid_to_utc,
        {{ alias }}.dbt_is_current,
        {{ alias }}.dbt_is_deleted,
        {{ alias }}.dbt_deleted_at_utc,
        {{ alias }}.dbt_is_valid_and_current,
        {{ alias }}.dbt_updated_at_utc,
    {%- else -%}
        dbt_valid_from_utc,
        dbt_valid_to_utc,
        dbt_is_current,
        dbt_is_deleted,
        dbt_deleted_at_utc,
        dbt_is_valid_and_current,
        dbt_updated_at_utc,
    {%- endif -%}
{%- endmacro -%}