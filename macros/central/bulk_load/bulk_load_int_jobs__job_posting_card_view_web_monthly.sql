-- noqa: disable=all
{% macro bulk_load_int_jobs__job_posting_card_view_web_monthly(p_activity_year, p_activity_month) %}

{#
-- dbt run-operation bulk_load_int_jobs__job_posting_card_view_web_monthly --args '{p_activity_year: 2024, p_activity_month: 1}'
-- This macro loads bulk data into int_jobs__job_posting_card_view_web for a given year and month
-- All days of the given year-month are iterated within a loop and whole month is loaded day by day during iterations
#}


{#
-- execute only with a valid month argument
#}
    {% if p_activity_month >= 1 and p_activity_month < 10 %}
        {%- set p_activity_month_str = '0' ~ p_activity_month -%}
    {% elif p_activity_month in (10,11,12) %}
        {%- set p_activity_month_str = p_activity_month -%}
    {% else %}
        {{ log(p_activity_year ~ '-' ~ p_activity_month ~ " is not a valid month, task will terminate.", info=True) }}
        {%- set execute = False -%}
    {% endif %}


    {% if execute %}


        {% do run_query("use warehouse BI_DBT_WH_XXXLARGE") %}


        {% for day_id in range(1, 32, 1) %}

            {% if day_id < 10 %}
                {%- set day_id_str = '0' ~ day_id -%}
            {% else %}
                {%- set day_id_str = day_id -%}
            {% endif %}

            {%- set v_activity_date = p_activity_year ~ '-' ~ p_activity_month_str ~ '-' ~ day_id_str -%}

        {#
        -- check the validness of date when day_id > 28
        #}
            {% if day_id > 28 %}

                {% set query %}
                select is_date(try_to_date('{{ v_activity_date }}')::variant);
            {% endset %}

                {% set results = run_query(query) %}
            
                {% if results.columns[0].values()[0] == True %}
                    {% set is_valid_date = True %}
                {% else %}
                    {% set is_valid_date = False %}
                {% endif %}

            {% else %}

                {% set is_valid_date = True %}

            {% endif %}


            {% if is_valid_date == True %}

                {{ log(v_activity_date ~ ": executing...", info=True) }}

                {% set query %}

                merge into INTERMEDIATE.JOBS.job_posting_card_view_web
             as DBT_INTERNAL_DEST
                using
                (
                    with
card_view_nwt as (
    select *
    from intermediate.jobs.job_posting_card_view_search_web

    where
    true
                            and to_date(created_at_utc) = '{{ v_activity_date }}'
                            and created_at_utc < current_date
),

adobe_datafeeds as (
    select *
    from intermediate.jobs.job_posting_card_view_adobe_web

)

select
    nwt.event_sk as card_view_sk,
    nwt.created_at_utc,
    nwt.job_posting_id,
    nwt.xing_user_id,
    nwt.login_status,
    nwt.event_sk,
    nwt.page_name,
    nwt.element_name,
    nwt.element_detail,
    nwt.notification_type,
    nwt.application_platform                   as activity_platform,
    df.mobile_device_type as device_type,
    df.visit_id,
    df.activity_id,
    df.visitor_id,
    df.geo_country_code                        as country_code,
    df.traffic_id_first_touch_visit_evar_visit as traffic_source_id,
    nwt.dbt_updated_at_utc                     as nwt_dbt_updated_at_utc,
    df.dbt_updated_at_utc                      as adobe_dbt_updated_at_utc
from
    card_view_nwt as nwt
    left join adobe_datafeeds as df
            on
            (lower(nwt.device_id) = lower(df.cust_visitor_id))
            and (
                nwt.created_at_utc between (df.min_date_time_utc - interval '1 MINUTE') and (
                    df.max_date_time_utc + interval '1 MINUTE'
                )
            )
where
    true
    
qualify
    row_number()
        over (
            partition by
               nwt.xing_user_id,
                nwt.device_id,
                nwt.created_at_utc,
                nwt.application_platform,
                nwt.notification_type,
                nwt.job_posting_id,
                nwt.event_sk
            order by df.min_date_time_utc asc, df.activity_id asc
        )
    = 1
                       
                ) as DBT_INTERNAL_SOURCE
                    on
                    (
                            DBT_INTERNAL_SOURCE.card_view_sk = DBT_INTERNAL_DEST.card_view_sk
                    )

                when matched then update set
                        "CARD_VIEW_SK" = DBT_INTERNAL_SOURCE."CARD_VIEW_SK","CREATED_AT_UTC" = DBT_INTERNAL_SOURCE."CREATED_AT_UTC","VISIT_ID" = DBT_INTERNAL_SOURCE."VISIT_ID","JOB_POSTING_ID" = DBT_INTERNAL_SOURCE."JOB_POSTING_ID","XING_USER_ID" = DBT_INTERNAL_SOURCE."XING_USER_ID","LOGIN_STATUS" = DBT_INTERNAL_SOURCE."LOGIN_STATUS","ACTIVITY_PLATFORM" = DBT_INTERNAL_SOURCE."ACTIVITY_PLATFORM","COUNTRY_CODE" = DBT_INTERNAL_SOURCE."COUNTRY_CODE","DEVICE_TYPE" = DBT_INTERNAL_SOURCE."DEVICE_TYPE","TRAFFIC_SOURCE_ID" = DBT_INTERNAL_SOURCE."TRAFFIC_SOURCE_ID","PAGE_NAME" = DBT_INTERNAL_SOURCE."PAGE_NAME","ELEMENT_NAME" = DBT_INTERNAL_SOURCE."ELEMENT_NAME","ELEMENT_DETAIL" = DBT_INTERNAL_SOURCE."ELEMENT_DETAIL","NWT_DBT_UPDATED_AT_UTC" = DBT_INTERNAL_SOURCE."NWT_DBT_UPDATED_AT_UTC","ADOBE_DBT_UPDATED_AT_UTC" = DBT_INTERNAL_SOURCE."ADOBE_DBT_UPDATED_AT_UTC"
                when not matched then insert
                        ("CARD_VIEW_SK", "CREATED_AT_UTC",  "VISIT_ID", "JOB_POSTING_ID", "XING_USER_ID", "LOGIN_STATUS", "ACTIVITY_PLATFORM", "COUNTRY_CODE", "DEVICE_TYPE", "TRAFFIC_SOURCE_ID", "PAGE_NAME", "ELEMENT_NAME", "ELEMENT_DETAIL", "NWT_DBT_UPDATED_AT_UTC", "ADOBE_DBT_UPDATED_AT_UTC")    values
                        ("CARD_VIEW_SK", "CREATED_AT_UTC",  "VISIT_ID", "JOB_POSTING_ID", "XING_USER_ID", "LOGIN_STATUS", "ACTIVITY_PLATFORM", "COUNTRY_CODE", "DEVICE_TYPE", "TRAFFIC_SOURCE_ID", "PAGE_NAME", "ELEMENT_NAME", "ELEMENT_DETAIL", "NWT_DBT_UPDATED_AT_UTC", "ADOBE_DBT_UPDATED_AT_UTC")
                ;

            {% endset %}

                {% set results = run_query(query) %}
                {{ log(v_activity_date ~ ": " ~ results.columns[0].values()[0] ~ " rows inserted.", info=True) }}
                {{ log(v_activity_date ~ ": " ~ results.columns[1].values()[0] ~ " rows updated.", info=True) }}

            {% else %}

                {{ log(v_activity_date ~ ": not a valid date, skipped.", info=True) }}

            {% endif %}

        {% endfor %}

    {% endif %}

{% endmacro %}


