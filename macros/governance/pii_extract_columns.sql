{% macro extract_pii_columns_from_table(
    database_name='landing_zone',
    schema_name='jobs',
    table_name='organizations') %}

{#
-- This macro will leverage snowflake's classifier to identify
-- columns that include personal data
#}

{# we check if the table_name is case sensitive or not#}
{% if table_name is upper or table_name is lower%}
    {% set table_name = table_name | upper %}
{% endif %}

{% set target = database_name + '.' + schema_name + '."' + table_name + '"'%}

{% if execute %}
    {{ log("calling snowflake function extract_semantic_categories on " ~ target, info=True) }}

    {% set query%}
        insert into internal.governance.snowflake_privacy_classification_results
            select
                sysdate() as classification_at,
                upper('{{database_name}}') as table_catalog,
                upper('{{schema_name}}') as table_schema,
                '{{table_name}}' as table_name,
                f.key::varchar as column_name,
                iff(f.value:recommendation:confidence::varchar = 'HIGH',f.value:recommendation:privacy_category::varchar,null) as privacy_category,
                iff(f.value:recommendation:confidence::varchar = 'HIGH',f.value:recommendation:semantic_category::varchar,null) as semantic_category,
                null as probability -- deprecated in the newer version of the classifier
                --coalesce(f.value:"privacy_category"::varchar, f.value:"extra_info":"alternates"[0]:"privacy_category"::varchar) as privacy_category,
                --coalesce(f.value:"semantic_category"::varchar, f.value:"extra_info":"alternates"[0]:"semantic_category"::varchar) as semantic_category,
                --coalesce(f.value:"extra_info":"probability"::number(10,2), f.value:"extra_info":"alternates"[0]:"probability"::number(10,2)) as probability
            FROM TABLE(FLATTEN(EXTRACT_SEMANTIC_CATEGORIES('{{target}}'))) AS f;
    {% endset%}

    {% set results = run_query(query) %}

    {% if results|length > 0 %}
        {{ log(results.columns[0].values()[0] ~ " column(s) classified", info=True) }}
        {{ log("results written to internal.governance.snowflake_privacy_classification_results", info=True) }}
    {% endif %}

{% endif %}

{% endmacro %}


{% macro extract_pii_columns_from_schema(
    database_name='landing_zone',
    schema_name='profilesbackend') %}

{#
-- This macro will leverage snowflake's classifier to identify
-- columns that include personal data
#}

{% if execute %}

    {% set get_tables_query%}
        select table_name from {{database_name}}.information_schema.tables where upper(table_schema) = upper('{{schema_name}}');
    {% endset%}

    {%- set tables = run_query(get_tables_query) -%}

    {% if tables|length > 0 %}
        {{ log(tables|length ~" tables found, classification could take a while", info=True) }}

        {% for table in tables.columns[0].values() -%}
            {% do extract_pii_columns_from_table(
                database_name=database_name,
                schema_name=schema_name,
                table_name=table) %}
        {% endfor %}
    {% else %}
        {{ log("no tables found in schema " ~ schema_name, info=True) }}
    {% endif %}
{% endif %}

{% endmacro %}

{% macro extract_pii_columns_from_unclassified_tables() %}

{#
-- This macro classifies each table that has not been classified yet
-- or that contains new columns
#}

{% if execute %}

    {% set get_tables_query%}
        select table_catalog,table_schema,table_name from internal.governance.landing_zone_tables_unclassified;
    {% endset%}

    {%- set tables = run_query(get_tables_query) -%}

    {% if tables|length > 0 %}
        {{ log(tables|length ~" tables found, classification could take a while", info=True) }}

        {% for table in tables.columns[0].values() -%}

            {% do extract_pii_columns_from_table(
                database_name=tables.columns[0].values()[loop.index0],
                schema_name=tables.columns[1].values()[loop.index0],
                table_name=tables.columns[2].values()[loop.index0]) %}
        {% endfor %}
    {% else %}
        {{ log("no tables left to classify", info=True) }}
    {% endif %}
{% endif %}

{% endmacro %}