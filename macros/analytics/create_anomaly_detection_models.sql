{# 
Macro to create Snowflake Anomaly Detection models for each KPI.
Generates and executes SQL statements based on configurations.
Only processes KPIs that are due for anomaly detection based on granularity.
#}

{% macro create_anomaly_detection_models() %}

    {# Define granularity mapping as tuples of (date_part, number) #}
    {% set granularity_mapping = {
        'DAY': ('DAY', 1),
        'WEEK': ('WEEK', 1),
        'MONTH': ('MONTH', 1),
        'QUARTER': ('MONTH', 3),
        'YEAR': ('YEAR', 1)
    } %}

    {% set prod_schema %}
    USE SCHEMA ANALYTICS.ONLYFY;
    {% endset %}
    {% do run_query(prod_schema) %}

    {# Define the query to fetch KPIs due for anomaly detection #}
    {% set query %}
        SELECT
            TABLE_NAME,
            KPI_NAME,
            TIME_SERIES_COLUMN,
            GRANULARITY_LEVEL,
            FILTERS,
            JOINS,
            LABEL_COLNAME
        FROM {{ ref('onlyfy_reporting_kpi_configurations') }}
        WHERE IS_DELETED = false
          AND (
              next_anomaly_detection_at IS NULL
              OR next_anomaly_detection_at <= CURRENT_TIMESTAMP()
          )
    {% endset %}

    {# Execute the query and capture the results #}
    {% set results = run_query(query) %}

    {# Error handling for the initial query #}
    {% if results.status == 'error' %}
        {% do exceptions.raise_compiler_error("Error running KPI configurations query: " ~ results.message) %}
    {% endif %}

    {# Extract rows from the results #}
    {% set config_rows = results.rows %}
    {% set sql_statements = [] %}

    {# Iterate over each configuration row to generate SQL statements #}
    {% for row in config_rows %}
        {% set table_name = row['TABLE_NAME'] %}
        {% set kpi_name = row['KPI_NAME'] %}
        {% set time_series_column = row['TIME_SERIES_COLUMN'] %}
        {% set granularity_level = row['GRANULARITY_LEVEL'] %}
        {% set filters = row['FILTERS'] %}
        {% set joins = row['JOINS'] %}
        {% set label_colname = row['LABEL_COLNAME'] %}

        {# Check if granularity_level is defined in mapping #}
        {% if granularity_level not in granularity_mapping %}
            {% do exceptions.raise_compiler_error("Undefined granularity level: " ~ granularity_level ~ " for KPI: " ~ kpi_name) %}
        {% endif %}

        {% set granularity = granularity_mapping[granularity_level] %}
        {% do log("Processing KPI: " ~ kpi_name ~ " with granularity: " ~ granularity_level, info=True) %}
        {% do log("Granularity details - Date Part: " ~ granularity[0] ~ ", Number: " ~ granularity[1], info=True) %}

        {# Define the KPI expression directly from kpi_name #}
        {% set kpi_expression = kpi_name %}
        
        {# Validate kpi_expression is not null or empty #}
        {% if kpi_expression is none or kpi_expression == '' %}
            {% do exceptions.raise_compiler_error("kpi_name cannot be null or empty for table " ~ table_name) %}
        {% endif %}

        {# Sanitize identifiers to adhere to Snowflake naming conventions #}
        {% set sanitized_table_name = custom_regex_replace(table_name, '[^a-zA-Z0-9_]', '_') %}
        {% set sanitized_kpi_name = custom_regex_replace(kpi_name, '[^a-zA-Z0-9_]', '_') %}
        {% set sanitized_time_series_column = custom_regex_replace(time_series_column, '[^a-zA-Z0-9_]', '_') %}
        {% set sanitized_granularity_level = custom_regex_replace(granularity_level, '[^a-zA-Z0-9_]', '_') %}
        {% set sanitized_kpi_expression = custom_regex_replace(kpi_expression, '[^a-zA-Z0-9_]', '_') %}

        {# Define unique names for models #}
        {% set training_table = sanitized_table_name ~ '_' ~ sanitized_kpi_name ~ '_training_v1' %}
        {% set prediction_table = sanitized_table_name ~ '_' ~ sanitized_kpi_name ~ '_prediction_v1' %}
        {% set anomalies_table = sanitized_table_name ~ '_' ~ sanitized_kpi_name ~ '_anomalies' %}
        {% set anomaly_model = 'anomaly_detector_' ~ sanitized_table_name ~ '_' ~ sanitized_kpi_name %}

        {# Calculate Dynamic Cutoff Date Based on Granularity #}
        {% set cutoff_date = "DATE_TRUNC('" ~ granularity[0] ~ "', CURRENT_TIMESTAMP())" %}
        {% do log("Cutoff Date for KPI " ~ kpi_name ~ ": " ~ cutoff_date, info=True) %}

        {# Initialize lists for filters and joins #}
        {% set filter_conditions = [] %}
        {% set joins_sql = "" %}

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
        {% endif %}

        {# Step 1: Get Min and Max Dates #}
        {% set date_range_query %}
            SELECT
                MIN(TO_TIMESTAMP_NTZ(DATE_TRUNC('{{ granularity_level }}', {{ time_series_column }}))) AS MIN_DATE,
                MAX(TO_TIMESTAMP_NTZ(DATE_TRUNC('{{ granularity_level }}', {{ time_series_column }}))) AS MAX_DATE
            FROM {{ table_name }}
            {{ joins_sql }}
            {{ final_where }}
        {% endset %}
        {% do sql_statements.append(date_range_query) %}

        {% do log("Getting Min and Max Dates: " ~ date_range_query, info=True) %}
        {% set date_range_result = run_query(date_range_query) %}

        {% if date_range_result.status == 'error' %}
            {{ exceptions.raise_compiler_error("Error retrieving date range: " ~ date_range_result.message) }}
        {% endif %}

        {% set min_date = date_range_result.rows[0]['MIN_DATE'] %}
        {% set max_date = date_range_result.rows[0]['MAX_DATE'] %}

        {# Step 2: Calculate Total Periods #}
        {% set total_periods_query %}
            SELECT DATEDIFF('{{ granularity[0] }}', '{{ min_date }}', '{{ max_date }}') AS TOTAL_PERIODS
        {% endset %}
        {% do sql_statements.append(total_periods_query) %}

        {% do log("Calculating Total Periods: " ~ total_periods_query, info=True) %}
        {% set total_periods_result = run_query(total_periods_query) %}
        {% set total_periods = total_periods_result.rows[0]['TOTAL_PERIODS'] %}

        {# Calculate Training Periods (77%) #}
        {% set training_periods = (total_periods * 0.77) | int %}

        {# Calculate Cutoff Date #}
        {% set cutoff_date_query %}
            SELECT DATEADD('{{ granularity[0] }}', {{ training_periods }}, '{{ min_date }}') AS CUTOFF_DATE
        {% endset %}
        {% do sql_statements.append(cutoff_date_query) %}

        {% do log("Calculating Cutoff Date: " ~ cutoff_date_query, info=True) %}
        {% set cutoff_date_result = run_query(cutoff_date_query) %}
        {% set cutoff_date = cutoff_date_result.rows[0]['CUTOFF_DATE'] %}

        {# Step 3: Update WHERE Clauses #}

        {# Training Data WHERE Clause #}
        {% set training_where_clause %}
            AND {{ time_series_column }} >= '{{ min_date }}'
            AND {{ time_series_column }} < '{{ cutoff_date }}'
        {% endset %}

        {# Prediction Data WHERE Clause #}
        {% set prediction_where_clause %}
            AND {{ time_series_column }} >= '{{ cutoff_date }}'
            AND {{ time_series_column }} <= '{{ max_date }}'
        {% endset %}

        {# Create Training Data Table #}
        {% set training_table_sql %}
            CREATE OR REPLACE TABLE ANALYTICS_WORKAREA.ONLYFY.{{ training_table }} AS 
            SELECT
                TO_TIMESTAMP_NTZ(DATE_TRUNC('{{ granularity_level }}', {{ time_series_column }})) AS {{ time_series_column }}_v1,
                {{ kpi_expression }} AS KPI_VALUE
            FROM {{ table_name }}
            {{ joins_sql }}
            {{ final_where }}
            {{ training_where_clause }}
            GROUP BY ALL
            ORDER BY {{ time_series_column }}_v1;
        {% endset %}
        {% do sql_statements.append(training_table_sql) %}

        {% do log("Creating Training Data Table: " ~ training_table_sql, info=True) %}
        {% do run_query(training_table_sql) %}

        {# Create Prediction Data Table #}
        {% set prediction_table_sql %}
            CREATE OR REPLACE TABLE ANALYTICS_WORKAREA.ONLYFY.{{ prediction_table }} AS 
            SELECT
                TO_TIMESTAMP_NTZ(DATE_TRUNC('{{ granularity_level }}', {{ time_series_column }})) AS {{ time_series_column }}_v1,
                {{ kpi_expression }} AS KPI_VALUE
            FROM {{ table_name }}
            {{ joins_sql }}
            {{ final_where }}
            {{ prediction_where_clause }}
            GROUP BY ALL
            ORDER BY {{ time_series_column }}_v1;
        {% endset %}
        {% do sql_statements.append(prediction_table_sql) %}

        {% do log("Creating Prediction Data Table: " ~ prediction_table_sql, info=True) %}
        {% do run_query(prediction_table_sql) %}

        {# Create Anomaly Detection Model #}
        {% set anomaly_model_sql %}
            CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION {{ anomaly_model }} (
                INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'ANALYTICS_WORKAREA.ONLYFY.{{ training_table }}'),
                TIMESTAMP_COLNAME => '{{ time_series_column }}_v1',
                TARGET_COLNAME => 'KPI_VALUE',
                {% if label_colname %}
                LABEL_COLNAME => '{{ label_colname }}',
                {% else %}
                LABEL_COLNAME => '',
                {% endif %}
                CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
            );
        {% endset %}
        {% do sql_statements.append(anomaly_model_sql) %}

        {% do log("Creating Anomaly Detection Model: " ~ anomaly_model_sql, info=True) %}
        {% do run_query(anomaly_model_sql) %}

        {# Execute Anomaly Detection #}
        {% set anomaly_detection_call_sql %}
            CALL {{ anomaly_model }}!DETECT_ANOMALIES(
                INPUT_DATA => SYSTEM$REFERENCE('TABLE', 'ANALYTICS_WORKAREA.ONLYFY.{{ prediction_table }}'),
                TIMESTAMP_COLNAME => '{{ time_series_column }}_v1',
                TARGET_COLNAME => 'KPI_VALUE',
                CONFIG_OBJECT => { 'prediction_interval': 0.95 }
            );
        {% endset %}
        {% do sql_statements.append(anomaly_detection_call_sql) %}

        {% do log("Executing Anomaly Detection: " ~ anomaly_detection_call_sql, info=True) %}
        {% do run_query(anomaly_detection_call_sql) %}
        
        {# Capture the Last Query ID #}
        {% set get_last_query_id_sql %}
            SELECT LAST_QUERY_ID() AS QUERY_ID;
        {% endset %}
        {% do sql_statements.append(get_last_query_id_sql) %}

        {% set last_query_id_result = run_query(get_last_query_id_sql) %}
        
        {% if last_query_id_result.status == 'error' %}
            {{ exceptions.raise_compiler_error("Error retrieving LAST_QUERY_ID(): " ~ last_query_id_result.message) }}
        {% endif %}
        
        {% set last_query_id = last_query_id_result.rows[0]['QUERY_ID'] %}
        
        {# Create Anomalies Table from the Last Query's Result #}
        {% set anomalies_table_sql %}
            CREATE OR REPLACE TABLE ANALYTICS_WORKAREA.ONLYFY.{{ anomalies_table }} AS 
            SELECT * FROM TABLE(RESULT_SCAN('{{ last_query_id }}'));
        {% endset %}
        {% do sql_statements.append(anomalies_table_sql) %}
        
        {% do log("Creating Anomalies Table: " ~ anomalies_table_sql, info=True) %}
        {% do run_query(anomalies_table_sql) %}

    {% endfor %}

    {# Update the `next_anomaly_detection_at` for each KPI #}
    {% for row in config_rows %}
        {% set granularity_level = row['GRANULARITY_LEVEL'] %}
        {% set sanitized_table_name = custom_regex_replace(row['TABLE_NAME'], '[^a-zA-Z0-9_]', '_') %}
        {% set sanitized_kpi_name = custom_regex_replace(row['KPI_NAME'], '[^a-zA-Z0-9_]', '_') %}
        {% set anomaly_model = 'anomaly_detector_' ~ sanitized_table_name ~ '_' ~ sanitized_kpi_name %}

        {% set granularity = granularity_mapping[granularity_level] %}
        {% set next_detection = "DATEADD('" ~ granularity[0] ~ "', " ~ granularity[1] ~ ", CURRENT_TIMESTAMP())" %}

        {% set update_sql %}
            UPDATE analytics.onlyfy.reporting_kpi_configurations
            SET last_anomaly_detected_at = CURRENT_TIMESTAMP(),
                next_anomaly_detection_at = {{ next_detection }}
            WHERE table_name = '{{ row['TABLE_NAME'] }}'
            AND kpi_name = '{{ row['KPI_NAME'] }}'
            AND granularity_level = '{{ row['GRANULARITY_LEVEL'] }}';
        {% endset %}
        {% do sql_statements.append(update_sql) %}

        {% do log("Updating the `next_anomaly_detection_at` for each KPI : " ~ update_sql, info=True) %}
        {% do run_query(update_sql) %}
        
    {% endfor %}

    {# Optionally, return a success message #}
    {{ return("Anomaly detection models created and executed successfully.") }}
{% endmacro %}
