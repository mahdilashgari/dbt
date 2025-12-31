{#
  A macro to generate sources yaml file that will contain all the tables from given database.
  Run:
      dbt --quiet run-operation generate_source_yml_for_database --args '{"database": "raw"}' >> models/central/_raw_sources.yml
      dbt --quiet run-operation generate_source_yml_for_database --args '{"database": "analytics", "schema": "central"}' >> models/central/_analytics_central_sources.yml
#}

{% macro generate_source_yml_for_database(database='raw', schema=None) %}
    {% set database = database | lower %}
    {% set schema = schema | lower %}
    {% set query %}
      SELECT table_schema, table_name
      FROM {{ database }}.information_schema.tables
      WHERE lower(table_schema) NOT IN ('information_schema', 'pg_catalog', 'public')
      {% if schema is not none %}
          AND lower(table_schema) = '{{ schema }}'
      {% endif %}
      ORDER BY table_schema, table_name
    {% endset %}

    {% set result = run_query(query) %}
  {% if execute %}
    {% set rows = result.rows %}

    {% set current_schema = namespace(value='') %}
    {% set sources_yaml = ['version: 2', '', 'sources:'] %}

    {% for row in rows %}
      {% set schema_name = row[0] %}
      {% set table_name = row[1] %}
      {% if schema_name != current_schema.value %}
        {% do sources_yaml.append('  - name: ' ~ (database | lower ~ '_' ~ schema_name | lower)) %}
        {% do sources_yaml.append('    database: ' ~ database | lower) %}
        {% do sources_yaml.append('    schema: ' ~ schema_name | lower) %}
        {% do sources_yaml.append('    tables:') %}
        {% set current_schema.value = schema_name %}
      {% endif %}
      {% do sources_yaml.append('      - name: ' ~ table_name | lower) %}
    {% endfor %}
  {% endif %}

    {% set joined = sources_yaml | join('\n') %}
    {{ print(joined) }}
    {#{ log(joined, info=True) }#}
    {% do return(joined) %}
{% endmacro %}
