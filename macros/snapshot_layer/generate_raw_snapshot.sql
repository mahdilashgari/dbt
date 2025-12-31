{#
-- This macro generates a dbt model for a single raw data table
-- it will select all the data from the corresponding snapshot
-- if history is set to true it will also contain the history from the snapshot
-- If ref_model is set, it will use the ref model instead of the source
#}
{%- macro generate_raw_snapshot(
    id_cols,
    hashdiff=True,
    exclude_from_hashdiff=[],
    ref_model = ''
) -%}

    {%- set snapshot_name = model.fqn[-1] -%}
    {%- set unique_cols = id_cols -%}
    {%if ref_model | length == 0 %}
        {%- set source_system_name = model.fqn[-3] -%}  {# model.fqn contains snapshot name and path as list, -3 is last folder #}
        {%- set source_table_name = model.fqn[-1].rsplit('_snapshot',1)[0] | replace(source_system_name ~ "__","") -%}

    {%- set check_cols = dbt_utils.get_filtered_columns_in_relation(from=source(source_system_name,source_table_name), except=unique_cols + exclude_from_hashdiff)|sort -%}

        {%- set model_output -%}

            select
                {{ dbt_utils.star(source(source_system_name, source_table_name)) -}},
                {{- new_work_se_analytics.generate_surrogate_key(unique_cols,quote_identifiers=True, uppercase=True) }} as dbt_unique_sk,
                '{{ invocation_id }}' as dbt_invocation_id
                {% if hashdiff %},{{ new_work_se_analytics.generate_surrogate_key(check_cols,quote_identifiers=True) }} as dbt_hashdiff {% endif %}                
            from {{ source(source_system_name, source_table_name) }}
        {%- endset -%}
    {% else %}
        {%- set check_cols = dbt_utils.get_filtered_columns_in_relation(from=ref(ref_model), except=unique_cols + exclude_from_hashdiff)|sort -%}
        {%- set model_output -%}
                select
                    {{ dbt_utils.star(ref(ref_model)) -}},
                    {{- new_work_se_analytics.generate_surrogate_key(unique_cols,quote_identifiers=True, uppercase=True) }} as dbt_unique_sk,
                    '{{ invocation_id }}' as dbt_invocation_id
                    {% if hashdiff %},{{ new_work_se_analytics.generate_surrogate_key(check_cols,quote_identifiers=True) }} as dbt_hashdiff {% endif %}                
                from {{ ref(ref_model) }}
        {%- endset -%}
    {% endif %}

        {% do return(model_output) %}

{%- endmacro -%}