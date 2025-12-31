{% macro bulk_load_dm_user_visit_summary_yearly(p_activity_year) %}

{#
-- dbt run-operation bulk_load_dm_user_visit_summary_yearly --args '{p_activity_year: 2023}'
-- This macro loads bulk data into DM_XING_USER_VISIT_SUMMARY for a given year
-- All months of the given year are iterated within a loop and whole month is loaded at once during iteration
#}

{% do run_query("use warehouse BI_DBT_WH_XXXLARGE")  %}

{% if execute %}

    {{ log("creating table...", info=True) }}

    {% set query%}
        create TRANSIENT TABLE IF NOT EXISTS {{ ref('central_dm_xing_user_visit_summary') }} (
            XING_USER_VISIT_SUMMARY_SK VARCHAR(32) NOT NULL COMMENT 'Surrogate key providing the uniqueness of activities.\n\nCalculated by concatenating required columns and applying hash algorithm.\n\nSource columns: ''DATE_ID'', ''XING_USER_ID'', ''VISIT_ID'', ''REGION_NAME'', ''MEMBERSHIP_CATEGORY_NAME'', ''ACTIVITY_PLATFORM''',
            ETL_ROW_CREATE_DATE TIMESTAMP_LTZ(9),
            ETL_ROW_UPDATE_DATE TIMESTAMP_LTZ(9),
            DATE_ID DATE COMMENT 'Date of activity in UTC timezone.\n\nOriginal value in ''Europe/Berlin'' timezone is converted to UTC within staging transformation.\n\nSource column: DATE_TIME',
            XING_USER_ID NUMBER(18,0) COMMENT 'User id that can be linked to user table',
            VISIT_ID VARCHAR(16777216) COMMENT 'Business key providing the uniqueness of visits.\n\nCalculated by concatenating required columns.\n\nSource columns: POST_VISID_HIGH, POST_VISID_LOW, VISIT_NUM, VISIT_START_TIME_GMT',
            REGION_NAME VARCHAR(16777216) COMMENT 'Region of user calculated with a certain logic for this table.\nOriginal field is in ANALYTICS.CENTRAL.DIM_COUNTRIES.\n',
            MEMBERSHIP_CATEGORY_NAME VARCHAR(9) COMMENT 'Calculated field showing whether the user has a B2C Payer membership or Basic membership\n\nSample values: B2C Payer, Basic',
            ACTIVITY_PLATFORM VARCHAR(18) COMMENT 'Platform information calculated based on values in certain fields and calculations from Xing team.\n\nSample values: iOS, Android, Web',
            LOGIN_STATUS VARCHAR(10) COMMENT 'Login status information calculated based on values in certain fields.\n\nSample values: Logged In, Logged Out, Soft Logged In',
            NUMBER_OF_ACTIVITIES NUMBER(18,0) COMMENT 'Total number of activities for this aggregated row.',
            FIRST_ACTIVITY_DATETIME TIMESTAMP_NTZ(9) COMMENT 'Timestamp of first activity for this aggregated row.',
            LAST_ACTIVITY_DATETIME TIMESTAMP_NTZ(9) COMMENT 'Timestamp of last activity for this aggregated row.',
            DBT_UPDATED_AT_UTC TIMESTAMP_NTZ(9)
        )COMMENT='Datamart table for the visit based summary (broken down into some other dimension) of user activities'
        ;
    {% endset%}

    {% set results = run_query(query) %}
    {{ log(results.columns[0].values()[0], info=True) }}

    {% for month_id in range(1, 13, 1) %}

        {% if month_id < 10 %}
            {%- set month_id_str = '0' ~ month_id -%}
        {% else %}
            {%- set month_id_str = month_id -%}
        {% endif %}

        {%- set v_activity_month = p_activity_year ~ '-' ~ month_id_str -%}

        {%- set v_activity_date = v_activity_month ~ '-01' -%}

        {{ log(v_activity_month ~ ": executing...", info=True) }}

        {% set query%}

            merge into {{ ref('central_dm_xing_user_visit_summary') }} as DBT_INTERNAL_DEST
            using
            (
                with
                activity as (
                    select f.*
                    from
                        {{ ref('central_fct_adobe_tracking_events') }} f
                    where
                        trunc(f.created_date, 'MONTH') = '{{v_activity_date}}'

                ),

                contact_details_pivoted as (
                    select * from {{ ref('central_dim_xing_user_contact_details_hst') }}
                ),

                premium_memberships as (
                    select *
                    from
                        {{ ref('central_dim_xing_user_memberships') }}
                    where
                        membership in ('ProJobs', 'Premium B2C')
                ),

                visit_based as (
                    select
                        activity.created_date as date_id,
                        activity.xing_user_id,
                        activity.visit_id,
                        case
                            when contact_details_pivoted.region_business in ('DACH', 'Non-DACH') then contact_details_pivoted.region_business
                            when activity.geo_region in ('DACH', 'Non-DACH') then activity.geo_region
                            else 'Non-DACH'
                        end as region_name,
                        case when premium_memberships.xing_user_id is not null then 'B2C Payer' else 'Basic' end as membership_category_name,
                        activity.activity_platform,
                        case max(activity.login_status = 'Logged In') when 1 then 'Logged In' else 'Logged Out' end as login_status, --take logged-in as priority per visit
                        count(activity.adobe_tracking_event_sk) as number_of_activities,
                        min(activity.created_at) as first_activity_datetime,
                        max(activity.created_at) as last_activity_datetime,
                        max(activity.dbt_updated_at_utc) as dbt_updated_at_utc
                    from
                        activity
                        left join contact_details_pivoted on activity.created_date = contact_details_pivoted.date_id and activity.xing_user_id = contact_details_pivoted.xing_user_id
                        left join premium_memberships on activity.created_date = premium_memberships.date_id and activity.xing_user_id = premium_memberships.xing_user_id
                    where
                        true
                        and activity.activity_platform != 'email' --remove email tracking
                    group by
                        all
                )

                select
                    md5(cast(coalesce(cast(DATE_ID as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(XING_USER_ID as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(VISIT_ID as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(REGION_NAME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(MEMBERSHIP_CATEGORY_NAME as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(ACTIVITY_PLATFORM as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as xing_user_visit_summary_sk, --surrogate key
                    current_timestamp as etl_row_create_date,
                    current_timestamp as etl_row_update_date,
                    visit_based.*
                from
                    visit_based
            ) as DBT_INTERNAL_SOURCE
                    on (
                            DBT_INTERNAL_SOURCE.xing_user_visit_summary_sk = DBT_INTERNAL_DEST.xing_user_visit_summary_sk
                        )
            when matched then update set
                etl_row_update_date = DBT_INTERNAL_SOURCE.etl_row_update_date,
                login_status = DBT_INTERNAL_SOURCE.login_status,
                number_of_activities = DBT_INTERNAL_SOURCE.number_of_activities,
                first_activity_datetime = DBT_INTERNAL_SOURCE.first_activity_datetime,
                last_activity_datetime = DBT_INTERNAL_SOURCE.last_activity_datetime,
                dbt_updated_at_utc = DBT_INTERNAL_SOURCE.dbt_updated_at_utc
            when not matched then insert
                ("XING_USER_VISIT_SUMMARY_SK", "ETL_ROW_CREATE_DATE", "ETL_ROW_UPDATE_DATE", "DATE_ID", "XING_USER_ID", "VISIT_ID", "REGION_NAME", "MEMBERSHIP_CATEGORY_NAME", "ACTIVITY_PLATFORM", "LOGIN_STATUS", "NUMBER_OF_ACTIVITIES", "FIRST_ACTIVITY_DATETIME", "LAST_ACTIVITY_DATETIME", "DBT_UPDATED_AT_UTC")
                values
                ("XING_USER_VISIT_SUMMARY_SK", "ETL_ROW_CREATE_DATE", "ETL_ROW_UPDATE_DATE", "DATE_ID", "XING_USER_ID", "VISIT_ID", "REGION_NAME", "MEMBERSHIP_CATEGORY_NAME", "ACTIVITY_PLATFORM", "LOGIN_STATUS", "NUMBER_OF_ACTIVITIES", "FIRST_ACTIVITY_DATETIME", "LAST_ACTIVITY_DATETIME", "DBT_UPDATED_AT_UTC")
            ;
        {% endset%}

        {% set results = run_query(query) %}
        {{ log(v_activity_month ~ ": " ~ results.columns[0].values()[0] ~ " rows inserted.", info=True) }}
        {{ log(v_activity_month ~ ": " ~ results.columns[1].values()[0] ~ " rows updated.", info=True) }}

    {% endfor %}

{% endif %}

{% endmacro %}