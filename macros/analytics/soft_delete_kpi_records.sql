{% macro soft_delete_kpi_records(email_address=None, kpi_name=None, granularity_level=None) %}
    {# 
        This macro performs a soft delete on `kpi_configurations` and `kpi_thresholds_generic` 
        based on provided parameters.

        Parameters:
            - email_address (string, optional): The email address to filter records.
            - kpi_name (string, optional): The KPI name to filter records.
            - granularity_level (string, optional): The granularity level to filter records.
                - If provided, both `kpi_thresholds_generic` and `kpi_configurations` will be soft deleted.
                - If not provided, only `kpi_thresholds_generic` will be soft deleted.

        Usage:
            # To delete only thresholds:
            dbt run-operation soft_delete_kpi_records --args '{"email_address": "user@example.com", "kpi_name": "Sales Growth"}'

            # To delete KPI and thresholds:
            dbt run-operation soft_delete_kpi_records --args '{"email_address": "user@example.com", "kpi_name": "Sales Growth", "granularity_level": "MONTH"}'

            # Daily run without parameters (no deletions will occur)
            dbt run-operation soft_delete_kpi_records
    #}

    {# ----------------------------
       Step 1: Check if Required Parameters are Provided
       ---------------------------- #}
    {% set has_thresholds_params = (email_address is not none) and (kpi_name is not none) %}
    {% set has_config_params = has_thresholds_params and (granularity_level is not none) %}

    {# Log the start of the macro execution #}
    {% if has_thresholds_params %}
        {% if has_config_params %}
            {% set log_message = "Soft deleting records in `kpi_thresholds_generic` and `kpi_configurations` for email_address: " ~ email_address ~ ", kpi_name: " ~ kpi_name ~ ", granularity_level: " ~ granularity_level %}
        {% else %}
            {% set log_message = "Soft deleting records in `kpi_thresholds_generic` for email_address: " ~ email_address ~ ", kpi_name: " ~ kpi_name %}
        {% endif %}
        {% do log(log_message, info=True) %}
    {% else %}
        {% do log("No parameters provided for soft deletion. Skipping delete operations.", info=True) %}
    {% endif %}

    {# ----------------------------
       Step 2: Soft Delete from kpi_thresholds_generic if Parameters are Provided
       ---------------------------- #}
    {% if has_thresholds_params %}
        {# Build WHERE conditions for kpi_thresholds_generic based on email_address and kpi_name #}
        {% set conditions_kpi_thresholds = [] %}
        {% set conditions_kpi_thresholds = conditions_kpi_thresholds + ["email_address = '" ~ email_address | replace("'", "''") ~ "'"] %}
        {% set conditions_kpi_thresholds = conditions_kpi_thresholds + ["kpi_name = '" ~ kpi_name | replace("'", "''") ~ "'"] %}

        {# Combine conditions using AND for kpi_thresholds_generic #}
        {% set where_clause_thresholds = conditions_kpi_thresholds | join(' AND ') %}

        {# Perform Soft Delete on kpi_thresholds_generic #}
        {% set threshold_query %}
            UPDATE {{ ref('onlyfy_int_kpi_thresholds') }}
            SET is_deleted = TRUE
            WHERE {{ where_clause_thresholds }};
        {% endset %}

        {# Execute the threshold_query and capture the results #}
        {% set results_thresholds = run_query(threshold_query) %}

        {# Error handling for the threshold_query #}
        {% if results_thresholds.status == 'error' %}
            {% do exceptions.raise_compiler_error("Error deleting records from `analytics.onlyfy.int_kpi_thresholds`: " ~ results_thresholds.message) %}
        {% endif %}
    {% endif %}

    {# ----------------------------
       Step 3: Conditionally Soft Delete from kpi_configurations if granularity_level is Provided
       ---------------------------- #}
    {% if has_config_params %}
        {# Build WHERE conditions for kpi_configurations based on kpi_name and granularity_level #}
        {% set conditions_kpi_configurations = [] %}
        {% set conditions_kpi_configurations = conditions_kpi_configurations + ["kpi_name = '" ~ kpi_name | replace("'", "''") ~ "'"] %}
        {% set conditions_kpi_configurations = conditions_kpi_configurations + ["granularity_level = '" ~ granularity_level | replace("'", "''") ~ "'"] %}

        {# Combine conditions using AND for kpi_configurations #}
        {% set where_clause_configurations = conditions_kpi_configurations | join(' AND ') %}

        {# Perform Soft Delete on kpi_configurations #}
        {% set kpi_query %}
            UPDATE {{ ref('onlyfy_reporting_kpi_configurations') }}
            SET is_deleted = TRUE
            WHERE {{ where_clause_configurations }};
        {% endset %}

        {# Execute the kpi_query and capture the results #}
        {% set results_kpi = run_query(kpi_query) %}

        {# Error handling for the kpi_query #}
        {% if results_kpi.status == 'error' %}
            {% do exceptions.raise_compiler_error("Error deleting records from `analytics.onlyfy.reporting_kpi_configurations`: " ~ results_kpi.message) %}
        {% endif %}
    {% endif %}
{% endmacro %}
