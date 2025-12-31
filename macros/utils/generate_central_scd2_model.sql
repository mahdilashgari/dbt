{#-
    Macro to generate a SQL query that removes unncecessary versions in an SCD2 model.

    USAGE:
        {{ generate_central_scd2_model(
            model_name,                 -- which source model to use
            id_col,                     -- id column in the source model
            hashdiff_columns_list,      -- hashdiff columns in the source model
            valid_from_col,             -- valid from column in the source model
            [deletion_col]              -- OPTIONAL ARGUMENT: deletion date column in the source model
        ) }}

    The generated SQL will depend on the presence of the optional deletion_col parameter.
    This parameter can be submitted when the data for the use case has a deletion date column.
    As an impact, VALID_TO field of the last row (that shows the deletion) in the history will be set to [deletion date + 1] instead of '9999-12-31'.

    e.g. considering the User Model and a sample user (xing_user_id = 59520790) which is deleted on 2025-01-11:
    - without the deletion_col parameter, the output will be like the following:

        VALID_FROM	VALID_TO	IS_CURRENT	XING_USER_ID	REGISTRATION_AT	            DELETION_AT	                IS_DELETED
        2023-07-25	2024-02-26	FALSE	    59520790	    2023-07-25 21:40:12.000	    	                        FALSE
        2024-02-26	2025-01-11	FALSE	    59520790	    2023-07-25 21:40:12.000	    	                        FALSE
        2025-01-11	9999-12-31	TRUE	    59520790	    2023-07-25 21:40:12.000	    2025-01-11 21:43:47.000	    TRUE

    - with the deletion_col parameter, the output will be like the following:

        VALID_FROM	VALID_TO	IS_CURRENT	XING_USER_ID	REGISTRATION_AT	            DELETION_AT	                IS_DELETED
        2023-07-25	2024-02-26	FALSE	    59520790	    2023-07-25 21:40:12.000	    	                        FALSE
        2024-02-26	2025-01-11	FALSE	    59520790	    2023-07-25 21:40:12.000	    	                        FALSE
        2025-01-11	2025-01-12	TRUE	    59520790	    2023-07-25 21:40:12.000	    2025-01-11 21:43:47.000	    TRUE

    ============================================================================================

    Macro 'generate_central_scd2_model' is working in two steps:
    1. 'scd2_detect_changelog' macro can be used to detect the change log between rows of a data set.
    2. 'scd2_reduce_data' macro can be used to remove unncecessary versions from the output of the first step.

    These steps are implemented in two separate macros to allow the user to use them separately if needed.
    e.g. 'scd2_detect_changelog' macro can be executed standalone to detect the change log between rows of a data set and write it to a table or view. This is useful for debugging purposes.
-#}


{#-
    Macro to detect the change log between rows of a data set.

    USAGE:
        {{ scd2_detect_changelog(
            model_name,                 -- which source model to use
            id_col,                     -- id column in the source model
            hashdiff_columns_list,      -- hashdiff columns in the source model
            valid_from_col              -- valid from column in the source model
        ) }}
-#}
{% macro scd2_detect_changelog(model_name, id_col, hashdiff_columns_list, valid_from_col) %}

    {%- set query -%}

        with event_log as (
            select
                *,
                {{ dbt_utils.generate_surrogate_key(hashdiff_columns_list) }} as hashdiff_col
            from
                {{ model_name }}
        ),

        event_log_with_prev_values as (
            select
                el.*,
                lag(el.hashdiff_col) over (partition by el.{{ id_col }} order by el.{{ valid_from_col }}) as prev_sk,
                lead(el.hashdiff_col) over (partition by el.{{ id_col }} order by el.valid_from) as next_sk
            from
                event_log el
        ),

        changes_detected as (
            select
                *,
                -- Check each property to determine if a change occurred
                case
                    when prev_sk is null then true
                    when hashdiff_col != prev_sk then true
                    /*
                    will be impemented & tested as new change detection logic
                    when prev_sk is null then true --pick the first row
                    when next_sk is null then true --pick the last row
                    when hashdiff_col <> next_sk then true --pick when the next row is different
                    */
                    -- Include conditions for other properties here
                    else false
                end as change_detected
            from
                event_log_with_prev_values
        )

        select * from changes_detected

    {% endset %}

    {{ return(query) }}
{% endmacro %}



{#-
    Macro to reduce the data set to only the necessary versions.

    USAGE:
        {{ scd2_reduce_data(
            model_name,                 -- which source model to use
            id_col,                     -- id column in the source model
            valid_from_col,             -- valid from column in the source model
            [deletion_col]              -- OPTIONAL ARGUMENT: deletion date column in the source model
        ) }}
-#}
{% macro scd2_reduce_data(model_name, id_col, valid_from_col, deletion_col) %}

    {%- set deletion_column = "argument doesn't exist" -%}  -- default value

    {%- if deletion_col -%}
        {%- set deletion_column = deletion_col~'::date' -%} -- set the deletion column
    {%- endif -%}

    {%- set query -%}

        with scd2_records as (
            select * from ({{ model_name }})
            where change_detected
        ),

        scd2_final as (
            select
                {% if deletion_col -%}
                /*
                    Add column is_last_row_after_deletion to the table.
                    The last row after deletion will be used as the last row of the history to aggregate changes after the deletion into one single row.
                    VALID_FROM of the last row after deletion will be set using the deletion date.
                    e.g. considering the User Model, xing_user_id = -15101100 had some data changes after the deletion date.
                    These changes are aggregated into one single row showing the latest state of the user.
                */
                coalesce({{ valid_from_col }} < {{ deletion_column }}::date, true) as is_row_before_deletion,
                case
                    when {{ valid_from_col }} >= {{ deletion_column }} and row_number() over(partition by {{ id_col }} order by {{ valid_from_col }} desc) = 1
                    then true
                    else false
                end as is_last_row_after_deletion,
                * replace (iff(is_last_row_after_deletion, {{ deletion_column }}, {{ valid_from_col }}) as {{ valid_from_col }})
                {%- else %}
                *
                {%- endif %}
            from
                scd2_records
        )

        /*
            Add VALID_TO and IS_CURRENT columns to the table.
            VALID_TO will be the next VALID_FROM value for the same id with an exception for deletion rows.
            It will be the deletion date + 1 for the deletion row to enable easy reporting.
            This is explained with the sample xing_user_id = 59520790 in the macro description.
        */
        select
            {{ valid_from_col }},
            coalesce(
                lead({{ valid_from_col }}) over (partition by {{ id_col }} order by {{ valid_from_col }}),
                {% if deletion_col %} {{ deletion_column }} + 1, {% endif %}
                '9999-12-31'::date
            ) as valid_to,
            row_number() over(partition by {{ id_col }} order by {{ valid_from_col }} desc) = 1 as is_current,
            * exclude (change_detected, prev_sk, next_sk, {{ valid_from_col }} {%- if deletion_col -%} , is_row_before_deletion, is_last_row_after_deletion {%- endif %})
        from
            scd2_final
        {% if deletion_col -%}
        where
            (is_row_before_deletion or is_last_row_after_deletion)
        {%- endif %}

    {% endset %}

    {{ return(query) }}
{% endmacro %}



{% macro generate_central_scd2_model(model_name, id_col, hashdiff_columns_list, valid_from_col, deletion_col) %}

    {%- set changelog_data -%}

        {{ scd2_detect_changelog(model_name, id_col, hashdiff_columns_list, valid_from_col) }}

    {% endset %}

    {%- set query -%}

        {{ scd2_reduce_data(changelog_data, id_col, valid_from_col, deletion_col) }}

    {% endset %}

    {{ return(query) }}
{% endmacro %}
