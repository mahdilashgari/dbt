-- noqa: disable=all
{% macro bulk_load_int_card_view_native_monthly(p_activity_year, p_activity_month) %}

{#
-- dbt run-operation bulk_load_int_card_view_native_monthly --args '{p_activity_year: 2019, p_activity_month: 1}'
-- This macro loads bulk data into central_int_job__card_views_native_nwt for a given year and month
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

                merge into ANALYTICS.central_staging.int_job__card_views_native_nwt as DBT_INTERNAL_DEST
                using
                (
                    with
card_view_nwt as (
    select *
    from analytics.central_staging.int_job__cardview_native
    where
    true
                            and to_date(created_at_cet) = '{{ v_activity_date }}'
                            and created_at_cet < current_date
),

adobe_datafeeds as (
    select *
    from analytics.central_staging.int_job__adobe_datafeeds_native
),

mobile_device as (
    select * from analytics.central.dim_mobile_devices
)


select
     md5(cast(coalesce(cast(nwt.created_at_utc as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nwt.user_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nwt.job_posting_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nwt.element as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nwt.element_detail as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nwt.page as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nwt.event_timestamp as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(nwt.event_sk as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))
        as card_view_sk,
    nwt.created_at_utc,
    nwt.created_at_cet,
    df.visit_id,
    cast(nwt.job_posting_id as integer) as job_posting_id,
    nwt.user_id as xing_user_id,
    case when nwt.user_id > 0 then 'Logged In' else 'Logged Out' end as login_status,
    case
        when lower(nwt.application) in ('iosm', 'wbm_iosm') then 'iOS'
        when lower(nwt.application) in ('andm', 'wbm_andm') then 'Android'
        when nwt.application ilike 'wbm%' and mobile_device.mobile_device_type = 'Mobile Phone' then
            'Web - Small Screen'
        when nwt.application ilike 'wbm%' then 'Web - Big Screen'
        when nwt.application is not null then 'Other'
        else 'Unknown'
    end as activity_platform,
    df.geo_country_code as country_code,
    mobile_device.mobile_device_type as device_type,
    df.traffic_id_first_touch_visit_evar_visit as traffic_source_id,
    nwt.page,
    nwt.element,
    nwt.element_detail,
    nwt.dbt_updated_at_utc as nwt_dbt_updated_at_utc,
    df.dbt_updated_at_utc as adobe_dbt_updated_at_utc
from
    card_view_nwt as nwt
    left join adobe_datafeeds as df
            on
            (
                (nwt.user_id = df.xing_user_id)
                and (
                    nwt.created_at_utc between (df.min_date_time_utc - interval '10 MINUTE') and (
                        df.max_date_time_utc + interval '10 MINUTE'
                    )
                )
            )
    left join mobile_device on df.mobile_id = mobile_device.mobile_device_id
where
    true
    
qualify
    row_number()
        over (
            partition by
                nwt.user_id,
                nwt.device_id,
                nwt.created_at_utc,
                nwt.context,
                nwt.application,
                nwt.job_posting_id,
                nwt.event_timestamp,
                nwt.event_sk
            order by df.min_date_time_utc asc
        )
    = 1
                       
                ) as DBT_INTERNAL_SOURCE
                    on
                    (
                            DBT_INTERNAL_SOURCE.card_view_sk = DBT_INTERNAL_DEST.card_view_sk
                    )

                when matched then update set
                        "CARD_VIEW_SK" = DBT_INTERNAL_SOURCE."CARD_VIEW_SK","CREATED_AT_UTC" = DBT_INTERNAL_SOURCE."CREATED_AT_UTC","CREATED_AT_CET" = DBT_INTERNAL_SOURCE."CREATED_AT_CET","VISIT_ID" = DBT_INTERNAL_SOURCE."VISIT_ID","JOB_POSTING_ID" = DBT_INTERNAL_SOURCE."JOB_POSTING_ID","XING_USER_ID" = DBT_INTERNAL_SOURCE."XING_USER_ID","LOGIN_STATUS" = DBT_INTERNAL_SOURCE."LOGIN_STATUS","ACTIVITY_PLATFORM" = DBT_INTERNAL_SOURCE."ACTIVITY_PLATFORM","COUNTRY_CODE" = DBT_INTERNAL_SOURCE."COUNTRY_CODE","DEVICE_TYPE" = DBT_INTERNAL_SOURCE."DEVICE_TYPE","TRAFFIC_SOURCE_ID" = DBT_INTERNAL_SOURCE."TRAFFIC_SOURCE_ID","PAGE" = DBT_INTERNAL_SOURCE."PAGE","ELEMENT" = DBT_INTERNAL_SOURCE."ELEMENT","ELEMENT_DETAIL" = DBT_INTERNAL_SOURCE."ELEMENT_DETAIL","NWT_DBT_UPDATED_AT_UTC" = DBT_INTERNAL_SOURCE."NWT_DBT_UPDATED_AT_UTC","ADOBE_DBT_UPDATED_AT_UTC" = DBT_INTERNAL_SOURCE."ADOBE_DBT_UPDATED_AT_UTC"
                when not matched then insert
                        ("CARD_VIEW_SK", "CREATED_AT_UTC", "CREATED_AT_CET", "VISIT_ID", "JOB_POSTING_ID", "XING_USER_ID", "LOGIN_STATUS", "ACTIVITY_PLATFORM", "COUNTRY_CODE", "DEVICE_TYPE", "TRAFFIC_SOURCE_ID", "PAGE", "ELEMENT", "ELEMENT_DETAIL", "NWT_DBT_UPDATED_AT_UTC", "ADOBE_DBT_UPDATED_AT_UTC")    values
                        ("CARD_VIEW_SK", "CREATED_AT_UTC", "CREATED_AT_CET", "VISIT_ID", "JOB_POSTING_ID", "XING_USER_ID", "LOGIN_STATUS", "ACTIVITY_PLATFORM", "COUNTRY_CODE", "DEVICE_TYPE", "TRAFFIC_SOURCE_ID", "PAGE", "ELEMENT", "ELEMENT_DETAIL", "NWT_DBT_UPDATED_AT_UTC", "ADOBE_DBT_UPDATED_AT_UTC")
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
