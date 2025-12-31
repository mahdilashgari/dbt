{# 
Macro to generate SQL for KPI calculations based on configurations.
It generates a UNION ALL of SELECT statements for each KPI.
#}

{% macro generate_kpi_queries() %}


    {% set query %}
        SELECT
            TABLE_NAME,
            KPI_NAME,
            TIME_SERIES_COLUMN,
            GRANULARITY_LEVEL,
            FILTERS,
            JOINS
        FROM {{ ref('onlyfy_reporting_kpi_configurations') }}
        WHERE IS_DELETED = false
    {% endset %}

    {% set results = run_query(query) %}

    {% if execute %}
        {% set config_rows = results.rows %}
        {% set sql_statements = [] %}

        {% for row in config_rows %}
            {% set table_name = row['TABLE_NAME'] %}
            {% set kpi_name = row['KPI_NAME'] %}
            {% set time_series_column = row['TIME_SERIES_COLUMN'] %}
            {% set granularity_level = row['GRANULARITY_LEVEL'] %}
            {% set filters = row['FILTERS'] %}
            {% set joins = row['JOINS'] %}

            {# Initialize an empty list to hold individual filter conditions #}
            {% set filter_conditions = [] %}

            {# Parse and apply filters if any #}
            {% if filters is not none %}
                {% set filters_dict = fromjson(filters) %}
                {# Call the parse_filters macro defined externally #}
                {% set where_clause = parse_filters(filters_dict, table_name) %}
                {% set filter_conditions = filter_conditions + [where_clause] %}
            {% endif %}

            {# Combine all filter conditions with AND #}
            {% if filter_conditions | length > 0 %}
                {% set final_where = "WHERE " ~ (filter_conditions | join(" AND ")) %}
            {% else %}
                {% set final_where = "" %}
            {% endif %}

            {# Handle Joins if any #}
            {% if joins is not none %}
                {# Parse joins as JSON #}
                {% set joins_list = fromjson(joins) %}

                {# Initialize join clauses #}
                {% set join_clauses = [] %}
                {% for join in joins_list %}
                    {% set join_type = join.join_type | upper %}
                    {% set join_table = join.table %}
                    {% set join_alias = join.alias %}
                    {% set join_on = join.on %}

                    {% if join_alias %}
                        {% set join_clause = join_type ~ " JOIN " ~ join_table ~ " AS " ~ join_alias ~ " ON " ~ join_on %}
                    {% else %}
                        {% set join_clause = join_type ~ " JOIN " ~ join_table ~ " ON " ~ join_on %}
                    {% endif %}

                    {% do join_clauses.append(join_clause) %}
                {% endfor %}

                {% set joins_sql = join_clauses | join(" ") %}
            {% else %}
                {% set joins_sql = "" %}
            {% endif %}

            {# Build the SQL statement for this KPI configuration #}
            {% set sql = 
                "SELECT 
                    '" ~ table_name ~ "' AS TABLE_NAME,
                    '" ~ kpi_name ~ "' AS KPI_NAME, 
                    " ~ kpi_name ~ " AS KPI_VALUE, 
                    '" ~ granularity_level ~ "' AS GRANULARITY_LEVEL,
                    DATE_TRUNC('" ~ granularity_level | upper ~ "', " ~ time_series_column ~ ") AS TIME_SERIES_VALUE,
                    CURRENT_TIMESTAMP() as DATA_VERSION
                FROM " ~ table_name ~ " " ~ joins_sql ~ " " ~ final_where ~ "
                GROUP BY ALL"
            %}
            {% do sql_statements.append(sql) %}
            
        {% endfor %}

        {# Combine all SQL statements with UNION ALL #}
        {% if sql_statements | length > 0 %}
            {{ return(sql_statements | join(' UNION ALL ')) }}
        {% else %}
            {{ return('SELECT * FROM (SELECT NULL) WHERE FALSE') }}
        {% endif %}

    {% else %}
        {# Not executing; return an empty string or placeholder #}
        SELECT 1 AS placeholder WHERE FALSE
    {% endif %}

{% endmacro %}
