--noqa:disable=all

{% macro bulk_load_fct_adobe_tracking_events_monthly(p_activity_year, p_activity_month) %}

{#
-- dbt run-operation bulk_load_fct_adobe_tracking_events_monthly --args '{p_activity_year: 2019, p_activity_month: 1}'
-- This macro loads bulk data into FCT_ADOBE_TRACKING_EVENTS for a given year and month
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


    {% do run_query("use warehouse BI_DBT_WH_XXXLARGE")  %}


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

            {% set query%}
                select is_date(try_to_date('{{v_activity_date}}')::variant);
            {% endset%}

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

            -- delete the existing data for the given date
            {% set delete_query%}
                delete from {{ ref("central_fct_adobe_tracking_events") }} where created_date = '{{v_activity_date}}'
                ;
            {% endset%}

            -- print the number of rows deleted
            {% set delete_results = run_query(delete_query) %}
            {{ log(v_activity_date ~ ": " ~ delete_results.columns[0].values()[0] ~ " rows deleted.", info=True) }}


            -- merge the data into the target table
            {% set merge_query%}

                merge into {{ ref("central_fct_adobe_tracking_events") }} as DBT_INTERNAL_DEST
                using
                (
                    with
                    events as (
                        select *
                        from
                            {{ ref("int_user_tracking__datafeeds_events") }}
                        where
                            true
                            and to_date(date_time_utc) = '{{v_activity_date}}'
                            and date_time_utc < current_date
                    ),

                    mobile_device as (
                        select * from {{ ref("central_dim_mobile_devices") }}
                    ),

                    geo_country as (
                        select * from {{ ref("central_dim_countries") }}
                    ),

                    user_hashes as (
                        select * from {{ ref("central_int_xing_user__hashes") }}
                    ),

                    browser as (
                        select * from {{ ref("nwt_datafeeds__browser") }}
                    ),

                    browser_type as (
                        select * from {{ ref("nwt_datafeeds__browser_type") }}
                    ),

                    operating_system_type as (
                        select * from {{ ref("nwt_datafeeds__operating_systems") }}
                    )

                    select
                        events.adobe_datafeeds_sk                     as adobe_tracking_event_sk,
                        to_date(events.date_time_utc)                 as created_date,
                        events.date_time_utc                          as created_at,
                        to_date(events.date_time)                     as created_date_local_time,
                        events.date_time                              as created_at_local_time,
                        events.activity_id,
                        events.visit_id,
                        events.visitor_id,
                        events.hashed_user_id_evar_never,
                        user_hashes.xing_user_id,
                        events.hashed_user_id_prop,
                        user_hashes_prop.xing_user_id                 as xing_user_id_prop,
                        case events.login_state_prop
                            when 'logged_in' then 'Logged In'
                            when 'soft_logged_in' then 'Soft Logged In'
                            when 'logged_out' then 'Logged Out'
                            else 'Unknown'
                        end::varchar(100)                             as login_status,
                        events.traffic_id_first_touch_visit_evar_visit,
                        events.application_prop,
                        case
                            when
                                events.application_prop is not null
                                and lower(events.application_prop) != 'unknown'
                                and regexp_instr(events.application_prop, '[^a-zA-Z0-9\_]') = 0 --only alphanumeric characters and underscores are valid
                                and regexp_instr(events.application_prop, '[a-zA-Z]') > 0       --there should be at least one letter in the platform name
                                and
                                (
                                    length(events.application_prop) <= 5                                                           --platform name should be either shorter than 6 characters
                                    or (length(events.application_prop) between 6 and 15 and regexp_instr(events.application_prop, '[\_]') > 0) --or up to 15 characters only with an underscore
                                )
                            then lower(events.application_prop)
                            else 'Unknown'
                        end                                           as application_name,
                        case
                            when events.application_prop = 'wbm' and mobile_device.mobile_device_type = 'Mobile Phone' then 'Web - Small Screen' -- noqa: LT05
                            when events.application_prop = 'wbm' then 'Web - Big Screen'
                            when events.application_prop in ('iosm', 'wbm_iosm') then 'iOS'
                            when events.application_prop in ('andm', 'wbm_andm') then 'Android'
                            when events.application_prop is not null then 'Other'
                            else 'Unknown'
                        end::varchar(100)                             as activity_platform,
                        browser.col1                                  as browser,
                        browser_type.col1                             as browser_type,
                        operating_system_type.col1                    as operating_system_type,
                        events.mobile_id,
                        events.mobile_action,
                        events.mobile_app_id,
                        events.mobile_device,
                        events.mobile_os_version,
                        events.mobile_resolution,
                        mobile_device.manufacturer                    as mobile_device_manufacturer,
                        mobile_device.mobile_device_name,
                        mobile_device.mobile_device_type,
                        mobile_device.mobile_os,
                        mobile_device.mobile_diagonal_screen_size,
                        mobile_device.mobile_screen_width,
                        mobile_device.mobile_screen_height,
                        events.pagename,
                        coalesce(events.pagename is not null, false)  as is_page_view,
                        coalesce(geo_country.region_name, 'Unknown')  as geo_region,
                        coalesce(geo_country.country_code, 'Unknown') as geo_country_code,
                        initcap(coalesce(events.geo_city, 'Unknown')) as geo_city,
                        events.geo_zip,
                        events.* exclude (
                            adobe_datafeeds_sk,
                            date_time_utc, date_time, activity_id, visit_id, visitor_id, hashed_user_id_evar_never, hashed_user_id_prop,
                            login_state_prop, traffic_id_first_touch_visit_evar_visit, application_prop,
                            mobile_id, mobile_action, mobile_app_id, mobile_device, mobile_os_version, mobile_resolution,
                            pagename, browser, os, geo_country_code, geo_region, geo_city, geo_zip,
                            dbt_updated_at_utc
                        ),
                        events.dbt_updated_at_utc
                    from
                        events
                        left join user_hashes on events.hashed_user_id_evar_never = user_hashes.webtracking_hash
                        left join user_hashes as user_hashes_prop on events.hashed_user_id_prop = user_hashes_prop.webtracking_hash -- noqa: AL06
                        left join mobile_device on events.mobile_id = mobile_device.mobile_device_id
                        left join geo_country on lower(events.geo_country_code) = lower(geo_country.country_alpha3_code)
                        left join browser on events.browser = browser.col0
                        left join browser_type on events.browser = browser_type.col0
                        left join operating_system_type on events.os = operating_system_type.col0
                ) as DBT_INTERNAL_SOURCE
                    on
                    (
                        DBT_INTERNAL_SOURCE.adobe_tracking_event_sk = DBT_INTERNAL_DEST.adobe_tracking_event_sk
                    )

                when matched then update set
                    "ADOBE_TRACKING_EVENT_SK" = DBT_INTERNAL_SOURCE."ADOBE_TRACKING_EVENT_SK","CREATED_DATE" = DBT_INTERNAL_SOURCE."CREATED_DATE","CREATED_AT" = DBT_INTERNAL_SOURCE."CREATED_AT","CREATED_AT_LOCAL_TIME" = DBT_INTERNAL_SOURCE."CREATED_AT_LOCAL_TIME","ACTIVITY_ID" = DBT_INTERNAL_SOURCE."ACTIVITY_ID","VISIT_ID" = DBT_INTERNAL_SOURCE."VISIT_ID","VISITOR_ID" = DBT_INTERNAL_SOURCE."VISITOR_ID","HASHED_USER_ID_EVAR_NEVER" = DBT_INTERNAL_SOURCE."HASHED_USER_ID_EVAR_NEVER","XING_USER_ID" = DBT_INTERNAL_SOURCE."XING_USER_ID","HASHED_USER_ID_PROP" = DBT_INTERNAL_SOURCE."HASHED_USER_ID_PROP","XING_USER_ID_PROP" = DBT_INTERNAL_SOURCE."XING_USER_ID_PROP","LOGIN_STATUS" = DBT_INTERNAL_SOURCE."LOGIN_STATUS","TRAFFIC_ID_FIRST_TOUCH_VISIT_EVAR_VISIT" = DBT_INTERNAL_SOURCE."TRAFFIC_ID_FIRST_TOUCH_VISIT_EVAR_VISIT","APPLICATION_PROP" = DBT_INTERNAL_SOURCE."APPLICATION_PROP","APPLICATION_NAME" = DBT_INTERNAL_SOURCE."APPLICATION_NAME","ACTIVITY_PLATFORM" = DBT_INTERNAL_SOURCE."ACTIVITY_PLATFORM","BROWSER" = DBT_INTERNAL_SOURCE."BROWSER","BROWSER_TYPE" = DBT_INTERNAL_SOURCE."BROWSER_TYPE","MOBILE_ID" = DBT_INTERNAL_SOURCE."MOBILE_ID","MOBILE_ACTION" = DBT_INTERNAL_SOURCE."MOBILE_ACTION","MOBILE_APP_ID" = DBT_INTERNAL_SOURCE."MOBILE_APP_ID","MOBILE_DEVICE" = DBT_INTERNAL_SOURCE."MOBILE_DEVICE","MOBILE_OS_VERSION" = DBT_INTERNAL_SOURCE."MOBILE_OS_VERSION","MOBILE_RESOLUTION" = DBT_INTERNAL_SOURCE."MOBILE_RESOLUTION","MOBILE_DEVICE_MANUFACTURER" = DBT_INTERNAL_SOURCE."MOBILE_DEVICE_MANUFACTURER","MOBILE_DEVICE_NAME" = DBT_INTERNAL_SOURCE."MOBILE_DEVICE_NAME","MOBILE_DEVICE_TYPE" = DBT_INTERNAL_SOURCE."MOBILE_DEVICE_TYPE","MOBILE_OS" = DBT_INTERNAL_SOURCE."MOBILE_OS","MOBILE_DIAGONAL_SCREEN_SIZE" = DBT_INTERNAL_SOURCE."MOBILE_DIAGONAL_SCREEN_SIZE","OPERATING_SYSTEM_TYPE" = DBT_INTERNAL_SOURCE."OPERATING_SYSTEM_TYPE","MOBILE_SCREEN_WIDTH" = DBT_INTERNAL_SOURCE."MOBILE_SCREEN_WIDTH","MOBILE_SCREEN_HEIGHT" = DBT_INTERNAL_SOURCE."MOBILE_SCREEN_HEIGHT","PAGENAME" = DBT_INTERNAL_SOURCE."PAGENAME","IS_PAGE_VIEW" = DBT_INTERNAL_SOURCE."IS_PAGE_VIEW","GEO_COUNTRY_CODE" = DBT_INTERNAL_SOURCE."GEO_COUNTRY_CODE","GEO_REGION" = DBT_INTERNAL_SOURCE."GEO_REGION","HIT_ID_HIGH" = DBT_INTERNAL_SOURCE."HIT_ID_HIGH","HIT_ID_LOW" = DBT_INTERNAL_SOURCE."HIT_ID_LOW","POST_VISID_HIGH" = DBT_INTERNAL_SOURCE."POST_VISID_HIGH","POST_VISID_LOW" = DBT_INTERNAL_SOURCE."POST_VISID_LOW","VISIT_NUM" = DBT_INTERNAL_SOURCE."VISIT_NUM","VISIT_START_TIME_GMT" = DBT_INTERNAL_SOURCE."VISIT_START_TIME_GMT","LAST_HIT_TIME_GMT" = DBT_INTERNAL_SOURCE."LAST_HIT_TIME_GMT","HIT_ORDER_IN_VISIT" = DBT_INTERNAL_SOURCE."HIT_ORDER_IN_VISIT","CUST_VISITOR_ID" = DBT_INTERNAL_SOURCE."CUST_VISITOR_ID","PROVIDER_NAME" = DBT_INTERNAL_SOURCE."PROVIDER_NAME","DUPLICATE_EVENTS" = DBT_INTERNAL_SOURCE."DUPLICATE_EVENTS","DUPLICATE_PURCHASE" = DBT_INTERNAL_SOURCE."DUPLICATE_PURCHASE","EXCLUDE_HIT" = DBT_INTERNAL_SOURCE."EXCLUDE_HIT","FIRST_HIT_PAGE_URL" = DBT_INTERNAL_SOURCE."FIRST_HIT_PAGE_URL","FIRST_HIT_PAGENAME" = DBT_INTERNAL_SOURCE."FIRST_HIT_PAGENAME","FIRST_HIT_REFERRER" = DBT_INTERNAL_SOURCE."FIRST_HIT_REFERRER","FIRST_HIT_TIME_GMT" = DBT_INTERNAL_SOURCE."FIRST_HIT_TIME_GMT","GEO_CITY" = DBT_INTERNAL_SOURCE."GEO_CITY","GEO_ZIP" = DBT_INTERNAL_SOURCE."GEO_ZIP","HIT_SOURCE" = DBT_INTERNAL_SOURCE."HIT_SOURCE","HIT_TIME_GMT" = DBT_INTERNAL_SOURCE."HIT_TIME_GMT","MARKETING_CLOUD_ID" = DBT_INTERNAL_SOURCE."MARKETING_CLOUD_ID","PAID_SEARCH" = DBT_INTERNAL_SOURCE."PAID_SEARCH","BROWSER_HEIGHT" = DBT_INTERNAL_SOURCE."BROWSER_HEIGHT","BROWSER_WIDTH" = DBT_INTERNAL_SOURCE."BROWSER_WIDTH","CAMPAIGN" = DBT_INTERNAL_SOURCE."CAMPAIGN","SITE_SECTION" = DBT_INTERNAL_SOURCE."SITE_SECTION","COOKIES" = DBT_INTERNAL_SOURCE."COOKIES","CURRENCY" = DBT_INTERNAL_SOURCE."CURRENCY","EVENT_LIST" = DBT_INTERNAL_SOURCE."EVENT_LIST","SEARCH_LIST_FILTER_LISTEVAR_VISIT" = DBT_INTERNAL_SOURCE."SEARCH_LIST_FILTER_LISTEVAR_VISIT","MODULES_LISTEVAR_HIT" = DBT_INTERNAL_SOURCE."MODULES_LISTEVAR_HIT","LIST_LIST_EVAR_HIT" = DBT_INTERNAL_SOURCE."LIST_LIST_EVAR_HIT","EVENT_NAME" = DBT_INTERNAL_SOURCE."EVENT_NAME","PAGE_URL" = DBT_INTERNAL_SOURCE."PAGE_URL","PERSISTENT_COOKIE" = DBT_INTERNAL_SOURCE."PERSISTENT_COOKIE","PRODUCT_LIST" = DBT_INTERNAL_SOURCE."PRODUCT_LIST","PURCHASE_ID" = DBT_INTERNAL_SOURCE."PURCHASE_ID","REFERRER" = DBT_INTERNAL_SOURCE."REFERRER","SEARCH_ENGINE" = DBT_INTERNAL_SOURCE."SEARCH_ENGINE","VISID_TYPE" = DBT_INTERNAL_SOURCE."VISID_TYPE","REFERRER_TYPE" = DBT_INTERNAL_SOURCE."REFERRER_TYPE","RESOLUTION" = DBT_INTERNAL_SOURCE."RESOLUTION","S_RESOLUTION" = DBT_INTERNAL_SOURCE."S_RESOLUTION","VISIT_REFERRER" = DBT_INTERNAL_SOURCE."VISIT_REFERRER","VISIT_START_PAGE_URL" = DBT_INTERNAL_SOURCE."VISIT_START_PAGE_URL","VISIT_START_PAGENAME" = DBT_INTERNAL_SOURCE."VISIT_START_PAGENAME","USER_BUSINESS_COUNTRY_EVAR_NEVER" = DBT_INTERNAL_SOURCE."USER_BUSINESS_COUNTRY_EVAR_NEVER","CHECKOUT_STEP_INFO_EVAR_HIT" = DBT_INTERNAL_SOURCE."CHECKOUT_STEP_INFO_EVAR_HIT","COMPANY_SIZE_EVAR_VISIT" = DBT_INTERNAL_SOURCE."COMPANY_SIZE_EVAR_VISIT","COMPANY_INDUSTRY_EVAR_VISIT" = DBT_INTERNAL_SOURCE."COMPANY_INDUSTRY_EVAR_VISIT","ACCOUNT_TYPE_EVAR_VISIT" = DBT_INTERNAL_SOURCE."ACCOUNT_TYPE_EVAR_VISIT","TEST_ACCOUNT_EVAR_VISIT" = DBT_INTERNAL_SOURCE."TEST_ACCOUNT_EVAR_VISIT","CAMPAIGN_CODE_ORIGIN_EVAR_HIT" = DBT_INTERNAL_SOURCE."CAMPAIGN_CODE_ORIGIN_EVAR_HIT","EXTERNAL_JOB_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."EXTERNAL_JOB_ID_EVAR_HIT","JOBS_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."JOBS_ID_EVAR_HIT","JOBS_APPLY_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."JOBS_APPLY_ID_EVAR_HIT","JOBS_SLOT_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."JOBS_SLOT_ID_EVAR_HIT","PAYMENT_METHOD_EVAR_VISIT" = DBT_INTERNAL_SOURCE."PAYMENT_METHOD_EVAR_VISIT","MESSENGER_CHAT_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."MESSENGER_CHAT_ID_EVAR_HIT","EXPERIMENT_EVAR_HIT" = DBT_INTERNAL_SOURCE."EXPERIMENT_EVAR_HIT","EXPERIMENT_INFO_EVAR_HIT" = DBT_INTERNAL_SOURCE."EXPERIMENT_INFO_EVAR_HIT","UPSELL_POINT_EVAR_VISIT" = DBT_INTERNAL_SOURCE."UPSELL_POINT_EVAR_VISIT","CAMPAIGN_EXTRA_STRING_EVAR_HIT" = DBT_INTERNAL_SOURCE."CAMPAIGN_EXTRA_STRING_EVAR_HIT","CAMPAIGN_EXTRA_STRING_FILTERED_EVAR_HIT" = DBT_INTERNAL_SOURCE."CAMPAIGN_EXTRA_STRING_FILTERED_EVAR_HIT","EXTERNAL_CLICK_ID1_EVAR_HIT" = DBT_INTERNAL_SOURCE."EXTERNAL_CLICK_ID1_EVAR_HIT","EXTERNAL_CLICK_ID2_EVAR_HIT" = DBT_INTERNAL_SOURCE."EXTERNAL_CLICK_ID2_EVAR_HIT","SEARCH_TERM_EVAR_VISIT" = DBT_INTERNAL_SOURCE."SEARCH_TERM_EVAR_VISIT","WIDGET_EVAR_VISIT" = DBT_INTERNAL_SOURCE."WIDGET_EVAR_VISIT","TRACK_ACTION_EVAR_VISIT" = DBT_INTERNAL_SOURCE."TRACK_ACTION_EVAR_VISIT","INTERNAL_SOURCE_EVAR_VISIT" = DBT_INTERNAL_SOURCE."INTERNAL_SOURCE_EVAR_VISIT","NEWS_PRODUCT_EVAR_VISIT" = DBT_INTERNAL_SOURCE."NEWS_PRODUCT_EVAR_VISIT","NEWS_PUBLISHER_ID_EVAR_VISIT" = DBT_INTERNAL_SOURCE."NEWS_PUBLISHER_ID_EVAR_VISIT","NEWS_ARTICLE_ID_EVAR_VISIT" = DBT_INTERNAL_SOURCE."NEWS_ARTICLE_ID_EVAR_VISIT","INTERACTION_TYPE_EVAR_VISIT" = DBT_INTERNAL_SOURCE."INTERACTION_TYPE_EVAR_VISIT","CONTEXT_DIMENSION5_EVAR_VISIT" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION5_EVAR_VISIT","CONTEXT_DIMENSION4_EVAR_VISIT" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION4_EVAR_VISIT","CONTEXT_DIMENSION1_EVAR_VISIT" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION1_EVAR_VISIT","CONTEXT_DIMENSION2_EVAR_VISIT" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION2_EVAR_VISIT","CONTEXT_DIMENSION3_EVAR_VISIT" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION3_EVAR_VISIT","ACTION_ORIGIN_EVAR_VISIT" = DBT_INTERNAL_SOURCE."ACTION_ORIGIN_EVAR_VISIT","JOBS_ORIGIN_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."JOBS_ORIGIN_ID_EVAR_HIT","EXTERNAL_AD_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."EXTERNAL_AD_ID_EVAR_HIT","EXTERNAL_DEVICE_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."EXTERNAL_DEVICE_ID_EVAR_HIT","CROSS_DOMAIN_VISITOR_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."CROSS_DOMAIN_VISITOR_ID_EVAR_HIT","AB_TEST_PROP" = DBT_INTERNAL_SOURCE."AB_TEST_PROP","APP_OPEN_EVAR_VISIT" = DBT_INTERNAL_SOURCE."APP_OPEN_EVAR_VISIT","DESTINATION_URL_EVAR_HIT" = DBT_INTERNAL_SOURCE."DESTINATION_URL_EVAR_HIT","FORM_FIELD_EVAR_HIT" = DBT_INTERNAL_SOURCE."FORM_FIELD_EVAR_HIT","REFERRING_DOMAIN_EVAR_VISIT" = DBT_INTERNAL_SOURCE."REFERRING_DOMAIN_EVAR_VISIT","ACTOR_TYPE_EVAR_VISIT" = DBT_INTERNAL_SOURCE."ACTOR_TYPE_EVAR_VISIT","ENTITY_PAGES_ID_EVAR_VISIT" = DBT_INTERNAL_SOURCE."ENTITY_PAGES_ID_EVAR_VISIT","LEAD_ID_EVAR_HIT" = DBT_INTERNAL_SOURCE."LEAD_ID_EVAR_HIT","REGISTRATION_BACKEND_CHANNEL_EVAR_NEVER" = DBT_INTERNAL_SOURCE."REGISTRATION_BACKEND_CHANNEL_EVAR_NEVER","REGISTRATION_SITE_SECTION_EVAR_VISIT" = DBT_INTERNAL_SOURCE."REGISTRATION_SITE_SECTION_EVAR_VISIT","UPSELL_SITE_SECTION_EVAR_PURCHASE" = DBT_INTERNAL_SOURCE."UPSELL_SITE_SECTION_EVAR_PURCHASE","GLOBAL_EXPERIMENT_VARIANT_EVAR_HIT" = DBT_INTERNAL_SOURCE."GLOBAL_EXPERIMENT_VARIANT_EVAR_HIT","CONTEXT_ADDITION_EVAR_VISIT" = DBT_INTERNAL_SOURCE."CONTEXT_ADDITION_EVAR_VISIT","GLOBAL_EXPERIMENT_EVAR_HIT" = DBT_INTERNAL_SOURCE."GLOBAL_EXPERIMENT_EVAR_HIT","LAST_PAGE_EVAR_HIT" = DBT_INTERNAL_SOURCE."LAST_PAGE_EVAR_HIT","NUMBER_CONTACTS_PROP" = DBT_INTERNAL_SOURCE."NUMBER_CONTACTS_PROP","MEMBERSHIPS_PROP" = DBT_INTERNAL_SOURCE."MEMBERSHIPS_PROP","APPLICATION_LANGUAGE_PROP" = DBT_INTERNAL_SOURCE."APPLICATION_LANGUAGE_PROP","ACCOUNT_DURATION_PROP" = DBT_INTERNAL_SOURCE."ACCOUNT_DURATION_PROP","PROFILE_ID_PROP" = DBT_INTERNAL_SOURCE."PROFILE_ID_PROP","URL_PARAMETER_NAME_PROP" = DBT_INTERNAL_SOURCE."URL_PARAMETER_NAME_PROP","BADGES_LIST_PROP" = DBT_INTERNAL_SOURCE."BADGES_LIST_PROP","SCROLLING_PROP" = DBT_INTERNAL_SOURCE."SCROLLING_PROP","PAGINATION_PROP" = DBT_INTERNAL_SOURCE."PAGINATION_PROP","SEARCH_TERM_PROP" = DBT_INTERNAL_SOURCE."SEARCH_TERM_PROP","SEARCH_LIST_FILTER_PROP" = DBT_INTERNAL_SOURCE."SEARCH_LIST_FILTER_PROP","SEARCH_POSITION_PROP" = DBT_INTERNAL_SOURCE."SEARCH_POSITION_PROP","SEARCH_RESULT_AMOUNT_PROP" = DBT_INTERNAL_SOURCE."SEARCH_RESULT_AMOUNT_PROP","SORT_ORDER_PROP" = DBT_INTERNAL_SOURCE."SORT_ORDER_PROP","SEARCH_CITY_PROP" = DBT_INTERNAL_SOURCE."SEARCH_CITY_PROP","GLOBAL_ACTION_PROP" = DBT_INTERNAL_SOURCE."GLOBAL_ACTION_PROP","EXTERNAL_CLICK_ID_PROP" = DBT_INTERNAL_SOURCE."EXTERNAL_CLICK_ID_PROP","PRIVACY_SETTING_PROP" = DBT_INTERNAL_SOURCE."PRIVACY_SETTING_PROP","NEWS_PRODUCT_PROP" = DBT_INTERNAL_SOURCE."NEWS_PRODUCT_PROP","NEWS_PUBLISHER_ID_PROP" = DBT_INTERNAL_SOURCE."NEWS_PUBLISHER_ID_PROP","NEWS_ARTICLE_ID_PROP" = DBT_INTERNAL_SOURCE."NEWS_ARTICLE_ID_PROP","LICENSE_LIST_PROP" = DBT_INTERNAL_SOURCE."LICENSE_LIST_PROP","CONTEXT_DIMENSION5_PROP" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION5_PROP","CONTEXT_DIMENSION4_PROP" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION4_PROP","CONTEXT_DIMENSION1_PROP" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION1_PROP","CONTEXT_DIMENSION2_PROP" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION2_PROP","CONTEXT_DIMENSION3_PROP" = DBT_INTERNAL_SOURCE."CONTEXT_DIMENSION3_PROP","TRACK_ACTION_LIST_PROP" = DBT_INTERNAL_SOURCE."TRACK_ACTION_LIST_PROP","TRACK_ACTION_PROP" = DBT_INTERNAL_SOURCE."TRACK_ACTION_PROP","CORRESPONDING_ID_PROP" = DBT_INTERNAL_SOURCE."CORRESPONDING_ID_PROP","POSTPONE_ACTION_PROP" = DBT_INTERNAL_SOURCE."POSTPONE_ACTION_PROP","EXTERNAL_USER_ID_PROP" = DBT_INTERNAL_SOURCE."EXTERNAL_USER_ID_PROP","ITEM_ID_PROP" = DBT_INTERNAL_SOURCE."ITEM_ID_PROP","FORM_LIST_EVAR_HIT" = DBT_INTERNAL_SOURCE."FORM_LIST_EVAR_HIT","MESSENGER_CONTEXT_PROP" = DBT_INTERNAL_SOURCE."MESSENGER_CONTEXT_PROP","DBT_UPDATED_AT_UTC" = DBT_INTERNAL_SOURCE."DBT_UPDATED_AT_UTC"

                when not matched then insert
                    ("ADOBE_TRACKING_EVENT_SK", "CREATED_DATE", "CREATED_AT", "CREATED_AT_LOCAL_TIME", "ACTIVITY_ID", "VISIT_ID", "VISITOR_ID", "HASHED_USER_ID_EVAR_NEVER", "XING_USER_ID", "HASHED_USER_ID_PROP", "XING_USER_ID_PROP", "LOGIN_STATUS", "TRAFFIC_ID_FIRST_TOUCH_VISIT_EVAR_VISIT", "APPLICATION_PROP", "APPLICATION_NAME", "ACTIVITY_PLATFORM", "BROWSER", "BROWSER_TYPE", "MOBILE_ID", "MOBILE_ACTION", "MOBILE_APP_ID", "MOBILE_DEVICE", "MOBILE_OS_VERSION", "MOBILE_RESOLUTION", "MOBILE_DEVICE_MANUFACTURER", "MOBILE_DEVICE_NAME", "MOBILE_DEVICE_TYPE", "MOBILE_OS", "MOBILE_DIAGONAL_SCREEN_SIZE", "OPERATING_SYSTEM_TYPE", "MOBILE_SCREEN_WIDTH", "MOBILE_SCREEN_HEIGHT", "PAGENAME", "IS_PAGE_VIEW", "GEO_COUNTRY_CODE", "GEO_REGION", "HIT_ID_HIGH", "HIT_ID_LOW", "POST_VISID_HIGH", "POST_VISID_LOW", "VISIT_NUM", "VISIT_START_TIME_GMT", "LAST_HIT_TIME_GMT", "HIT_ORDER_IN_VISIT", "CUST_VISITOR_ID", "PROVIDER_NAME", "DUPLICATE_EVENTS", "DUPLICATE_PURCHASE", "EXCLUDE_HIT", "FIRST_HIT_PAGE_URL", "FIRST_HIT_PAGENAME", "FIRST_HIT_REFERRER", "FIRST_HIT_TIME_GMT", "GEO_CITY", "GEO_ZIP", "HIT_SOURCE", "HIT_TIME_GMT", "MARKETING_CLOUD_ID", "PAID_SEARCH", "BROWSER_HEIGHT", "BROWSER_WIDTH", "CAMPAIGN", "SITE_SECTION", "COOKIES", "CURRENCY", "EVENT_LIST", "SEARCH_LIST_FILTER_LISTEVAR_VISIT", "MODULES_LISTEVAR_HIT", "LIST_LIST_EVAR_HIT", "EVENT_NAME", "PAGE_URL", "PERSISTENT_COOKIE", "PRODUCT_LIST", "PURCHASE_ID", "REFERRER", "SEARCH_ENGINE", "VISID_TYPE", "REFERRER_TYPE", "RESOLUTION", "S_RESOLUTION", "VISIT_REFERRER", "VISIT_START_PAGE_URL", "VISIT_START_PAGENAME", "USER_BUSINESS_COUNTRY_EVAR_NEVER", "CHECKOUT_STEP_INFO_EVAR_HIT", "COMPANY_SIZE_EVAR_VISIT", "COMPANY_INDUSTRY_EVAR_VISIT", "ACCOUNT_TYPE_EVAR_VISIT", "TEST_ACCOUNT_EVAR_VISIT", "CAMPAIGN_CODE_ORIGIN_EVAR_HIT", "EXTERNAL_JOB_ID_EVAR_HIT", "JOBS_ID_EVAR_HIT", "JOBS_APPLY_ID_EVAR_HIT", "JOBS_SLOT_ID_EVAR_HIT", "PAYMENT_METHOD_EVAR_VISIT", "MESSENGER_CHAT_ID_EVAR_HIT", "EXPERIMENT_EVAR_HIT", "EXPERIMENT_INFO_EVAR_HIT", "UPSELL_POINT_EVAR_VISIT", "CAMPAIGN_EXTRA_STRING_EVAR_HIT", "CAMPAIGN_EXTRA_STRING_FILTERED_EVAR_HIT", "EXTERNAL_CLICK_ID1_EVAR_HIT", "EXTERNAL_CLICK_ID2_EVAR_HIT", "SEARCH_TERM_EVAR_VISIT", "WIDGET_EVAR_VISIT", "TRACK_ACTION_EVAR_VISIT", "INTERNAL_SOURCE_EVAR_VISIT", "NEWS_PRODUCT_EVAR_VISIT", "NEWS_PUBLISHER_ID_EVAR_VISIT", "NEWS_ARTICLE_ID_EVAR_VISIT", "INTERACTION_TYPE_EVAR_VISIT", "CONTEXT_DIMENSION5_EVAR_VISIT", "CONTEXT_DIMENSION4_EVAR_VISIT", "CONTEXT_DIMENSION1_EVAR_VISIT", "CONTEXT_DIMENSION2_EVAR_VISIT", "CONTEXT_DIMENSION3_EVAR_VISIT", "ACTION_ORIGIN_EVAR_VISIT", "JOBS_ORIGIN_ID_EVAR_HIT", "EXTERNAL_AD_ID_EVAR_HIT", "EXTERNAL_DEVICE_ID_EVAR_HIT", "CROSS_DOMAIN_VISITOR_ID_EVAR_HIT", "AB_TEST_PROP", "APP_OPEN_EVAR_VISIT", "DESTINATION_URL_EVAR_HIT", "FORM_FIELD_EVAR_HIT", "REFERRING_DOMAIN_EVAR_VISIT", "ACTOR_TYPE_EVAR_VISIT", "ENTITY_PAGES_ID_EVAR_VISIT", "LEAD_ID_EVAR_HIT", "REGISTRATION_BACKEND_CHANNEL_EVAR_NEVER", "REGISTRATION_SITE_SECTION_EVAR_VISIT", "UPSELL_SITE_SECTION_EVAR_PURCHASE", "GLOBAL_EXPERIMENT_VARIANT_EVAR_HIT", "CONTEXT_ADDITION_EVAR_VISIT", "GLOBAL_EXPERIMENT_EVAR_HIT", "LAST_PAGE_EVAR_HIT", "NUMBER_CONTACTS_PROP", "MEMBERSHIPS_PROP", "APPLICATION_LANGUAGE_PROP", "ACCOUNT_DURATION_PROP", "PROFILE_ID_PROP", "URL_PARAMETER_NAME_PROP", "BADGES_LIST_PROP", "SCROLLING_PROP", "PAGINATION_PROP", "SEARCH_TERM_PROP", "SEARCH_LIST_FILTER_PROP", "SEARCH_POSITION_PROP", "SEARCH_RESULT_AMOUNT_PROP", "SORT_ORDER_PROP", "SEARCH_CITY_PROP", "GLOBAL_ACTION_PROP", "EXTERNAL_CLICK_ID_PROP", "PRIVACY_SETTING_PROP", "NEWS_PRODUCT_PROP", "NEWS_PUBLISHER_ID_PROP", "NEWS_ARTICLE_ID_PROP", "LICENSE_LIST_PROP", "CONTEXT_DIMENSION5_PROP", "CONTEXT_DIMENSION4_PROP", "CONTEXT_DIMENSION1_PROP", "CONTEXT_DIMENSION2_PROP", "CONTEXT_DIMENSION3_PROP", "TRACK_ACTION_LIST_PROP", "TRACK_ACTION_PROP", "CORRESPONDING_ID_PROP", "POSTPONE_ACTION_PROP", "EXTERNAL_USER_ID_PROP", "ITEM_ID_PROP", "FORM_LIST_EVAR_HIT", "MESSENGER_CONTEXT_PROP", "DBT_UPDATED_AT_UTC")
                values
                    ("ADOBE_TRACKING_EVENT_SK", "CREATED_DATE", "CREATED_AT", "CREATED_AT_LOCAL_TIME", "ACTIVITY_ID", "VISIT_ID", "VISITOR_ID", "HASHED_USER_ID_EVAR_NEVER", "XING_USER_ID", "HASHED_USER_ID_PROP", "XING_USER_ID_PROP", "LOGIN_STATUS", "TRAFFIC_ID_FIRST_TOUCH_VISIT_EVAR_VISIT", "APPLICATION_PROP", "APPLICATION_NAME", "ACTIVITY_PLATFORM", "BROWSER", "BROWSER_TYPE", "MOBILE_ID", "MOBILE_ACTION", "MOBILE_APP_ID", "MOBILE_DEVICE", "MOBILE_OS_VERSION", "MOBILE_RESOLUTION", "MOBILE_DEVICE_MANUFACTURER", "MOBILE_DEVICE_NAME", "MOBILE_DEVICE_TYPE", "MOBILE_OS", "MOBILE_DIAGONAL_SCREEN_SIZE", "OPERATING_SYSTEM_TYPE", "MOBILE_SCREEN_WIDTH", "MOBILE_SCREEN_HEIGHT", "PAGENAME", "IS_PAGE_VIEW", "GEO_COUNTRY_CODE", "GEO_REGION", "HIT_ID_HIGH", "HIT_ID_LOW", "POST_VISID_HIGH", "POST_VISID_LOW", "VISIT_NUM", "VISIT_START_TIME_GMT", "LAST_HIT_TIME_GMT", "HIT_ORDER_IN_VISIT", "CUST_VISITOR_ID", "PROVIDER_NAME", "DUPLICATE_EVENTS", "DUPLICATE_PURCHASE", "EXCLUDE_HIT", "FIRST_HIT_PAGE_URL", "FIRST_HIT_PAGENAME", "FIRST_HIT_REFERRER", "FIRST_HIT_TIME_GMT", "GEO_CITY", "GEO_ZIP", "HIT_SOURCE", "HIT_TIME_GMT", "MARKETING_CLOUD_ID", "PAID_SEARCH", "BROWSER_HEIGHT", "BROWSER_WIDTH", "CAMPAIGN", "SITE_SECTION", "COOKIES", "CURRENCY", "EVENT_LIST", "SEARCH_LIST_FILTER_LISTEVAR_VISIT", "MODULES_LISTEVAR_HIT", "LIST_LIST_EVAR_HIT", "EVENT_NAME", "PAGE_URL", "PERSISTENT_COOKIE", "PRODUCT_LIST", "PURCHASE_ID", "REFERRER", "SEARCH_ENGINE", "VISID_TYPE", "REFERRER_TYPE", "RESOLUTION", "S_RESOLUTION", "VISIT_REFERRER", "VISIT_START_PAGE_URL", "VISIT_START_PAGENAME", "USER_BUSINESS_COUNTRY_EVAR_NEVER", "CHECKOUT_STEP_INFO_EVAR_HIT", "COMPANY_SIZE_EVAR_VISIT", "COMPANY_INDUSTRY_EVAR_VISIT", "ACCOUNT_TYPE_EVAR_VISIT", "TEST_ACCOUNT_EVAR_VISIT", "CAMPAIGN_CODE_ORIGIN_EVAR_HIT", "EXTERNAL_JOB_ID_EVAR_HIT", "JOBS_ID_EVAR_HIT", "JOBS_APPLY_ID_EVAR_HIT", "JOBS_SLOT_ID_EVAR_HIT", "PAYMENT_METHOD_EVAR_VISIT", "MESSENGER_CHAT_ID_EVAR_HIT", "EXPERIMENT_EVAR_HIT", "EXPERIMENT_INFO_EVAR_HIT", "UPSELL_POINT_EVAR_VISIT", "CAMPAIGN_EXTRA_STRING_EVAR_HIT", "CAMPAIGN_EXTRA_STRING_FILTERED_EVAR_HIT", "EXTERNAL_CLICK_ID1_EVAR_HIT", "EXTERNAL_CLICK_ID2_EVAR_HIT", "SEARCH_TERM_EVAR_VISIT", "WIDGET_EVAR_VISIT", "TRACK_ACTION_EVAR_VISIT", "INTERNAL_SOURCE_EVAR_VISIT", "NEWS_PRODUCT_EVAR_VISIT", "NEWS_PUBLISHER_ID_EVAR_VISIT", "NEWS_ARTICLE_ID_EVAR_VISIT", "INTERACTION_TYPE_EVAR_VISIT", "CONTEXT_DIMENSION5_EVAR_VISIT", "CONTEXT_DIMENSION4_EVAR_VISIT", "CONTEXT_DIMENSION1_EVAR_VISIT", "CONTEXT_DIMENSION2_EVAR_VISIT", "CONTEXT_DIMENSION3_EVAR_VISIT", "ACTION_ORIGIN_EVAR_VISIT", "JOBS_ORIGIN_ID_EVAR_HIT", "EXTERNAL_AD_ID_EVAR_HIT", "EXTERNAL_DEVICE_ID_EVAR_HIT", "CROSS_DOMAIN_VISITOR_ID_EVAR_HIT", "AB_TEST_PROP", "APP_OPEN_EVAR_VISIT", "DESTINATION_URL_EVAR_HIT", "FORM_FIELD_EVAR_HIT", "REFERRING_DOMAIN_EVAR_VISIT", "ACTOR_TYPE_EVAR_VISIT", "ENTITY_PAGES_ID_EVAR_VISIT", "LEAD_ID_EVAR_HIT", "REGISTRATION_BACKEND_CHANNEL_EVAR_NEVER", "REGISTRATION_SITE_SECTION_EVAR_VISIT", "UPSELL_SITE_SECTION_EVAR_PURCHASE", "GLOBAL_EXPERIMENT_VARIANT_EVAR_HIT", "CONTEXT_ADDITION_EVAR_VISIT", "GLOBAL_EXPERIMENT_EVAR_HIT", "LAST_PAGE_EVAR_HIT", "NUMBER_CONTACTS_PROP", "MEMBERSHIPS_PROP", "APPLICATION_LANGUAGE_PROP", "ACCOUNT_DURATION_PROP", "PROFILE_ID_PROP", "URL_PARAMETER_NAME_PROP", "BADGES_LIST_PROP", "SCROLLING_PROP", "PAGINATION_PROP", "SEARCH_TERM_PROP", "SEARCH_LIST_FILTER_PROP", "SEARCH_POSITION_PROP", "SEARCH_RESULT_AMOUNT_PROP", "SORT_ORDER_PROP", "SEARCH_CITY_PROP", "GLOBAL_ACTION_PROP", "EXTERNAL_CLICK_ID_PROP", "PRIVACY_SETTING_PROP", "NEWS_PRODUCT_PROP", "NEWS_PUBLISHER_ID_PROP", "NEWS_ARTICLE_ID_PROP", "LICENSE_LIST_PROP", "CONTEXT_DIMENSION5_PROP", "CONTEXT_DIMENSION4_PROP", "CONTEXT_DIMENSION1_PROP", "CONTEXT_DIMENSION2_PROP", "CONTEXT_DIMENSION3_PROP", "TRACK_ACTION_LIST_PROP", "TRACK_ACTION_PROP", "CORRESPONDING_ID_PROP", "POSTPONE_ACTION_PROP", "EXTERNAL_USER_ID_PROP", "ITEM_ID_PROP", "FORM_LIST_EVAR_HIT", "MESSENGER_CONTEXT_PROP", "DBT_UPDATED_AT_UTC")
                ;

            {% endset%}

            -- print the number of rows inserted and updated
            {% set merge_results = run_query(merge_query) %}
            {{ log(v_activity_date ~ ": " ~ merge_results.columns[0].values()[0] ~ " rows inserted.", info=True) }}
            {{ log(v_activity_date ~ ": " ~ merge_results.columns[1].values()[0] ~ " rows updated.", info=True) }}

        {% else %}

            {{ log(v_activity_date ~ ": not a valid date, skipped.", info=True) }}

        {% endif %}

    {% endfor %}

{% endif %}

{% endmacro %}