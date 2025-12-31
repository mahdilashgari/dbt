{% macro bulk_load_int_datafeeds__post_evar87_metadata_monthly(p_year, p_month) %}

{#
-- ! Is used to load only 2017-04 since the data is huge for this period
-- dbt run-operation bulk_load_int_datafeeds__post_evar87_metadata_monthly --args '{p_year: 2017, p_month: 4}'
-- This macro loads bulk data into INT_DATAFEEDS__POST_EVAR87_METADATA for a given year and month
-- All days of the given year-month are iterated within a loop and whole month is loaded day by day during iterations
#}


{#
-- execute only with a valid month argument
#}
{% if p_month >= 1 and p_month < 10 %}
    {%- set p_month_str = '0' ~ p_month -%}
{% elif p_month in (10,11,12) %}
    {%- set p_month_str = p_month -%}
{% else %}
    {{ log(p_year ~ '-' ~ p_month ~ " is not a valid month, task will terminate.", info=True) }}
    {%- set execute = False -%}
{% endif %}


{% if execute %}

    {% do run_query("use warehouse BI_DBT_WH_XXXLARGE")  %}

    {% for day_id in range(11, 13, 1) %}

        {% if day_id < 10 %}
            {%- set day_id_str = '0' ~ day_id -%}
        {% else %}
            {%- set day_id_str = day_id -%}
        {% endif %}

        {%- set v_date = p_year ~ '-' ~ p_month_str ~ '-' ~ day_id_str -%}

        {#
        -- check the validness of date when day_id > 28
        #}
        {% if day_id > 28 %}

            {% set query%}
                select is_date(try_to_date('{{v_date}}')::variant);
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

            {{ log(v_date ~ " ==> executing...", info=True) }}

            {% set query %}

                merge into {{ ref('central_int_datafeeds__post_evar87_metadata') }} as DBT_INTERNAL_DEST
                using
                (
                    with
                    v87_from_daily_visits as (
                        select * from {{ ref('central_int_datafeeds__post_evar87_list') }}
                        where
                            true
                            and year(max_date_time_utc) = 2017 and month(max_date_time_utc) = 4 --this macro is used to load only 2017-04 since the data is huge for this period
                            and to_date(max_date_time_utc) = '{{v_date}}'
                    ),

                    tracking_codes as (
                        select * from {{ ref('central_int_marketing__tracking_codes') }}
                        where tracking_code != '0'
                    ),

                    joined as (
                        select
                            v87_from_daily_visits.*,
                            tracking_codes.tracking_code as tracking_code_from_mapping,
                            tracking_codes.traffic_channel,
                            tracking_codes.partner as v87_partner,
                            tracking_codes.product as v87_product,
                            tracking_codes.executing_bu as v87_executing_bu,
                            tracking_codes.requesting_bu as v87_requesting_bu,
                            tracking_codes.communication_name as v87_communication_name,
                            tracking_codes.campaign_name as v87_campaign_name,
                            tracking_codes.camptool_targeting_level as v87_camptool_targeting_level,
                            tracking_codes.braze_tag_type as v87_braze_tag_type,
                            tracking_codes.braze_placement as v87_braze_placement,
                            tracking_codes.format as v87_format,
                            tracking_codes.agency as v87_agency
                        from
                            v87_from_daily_visits
                            left join tracking_codes on position(lower(tracking_codes.tracking_code) in lower(v87_from_daily_visits.tracking_code)) > 0
                    ),

                    final as (
                        select
                            joined.*,
                            md5(cast(coalesce(cast(POST_EVAR87 as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(TRACKING_CODE_FROM_MAPPING as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as post_evar87_metadata_sk
                        from joined
                    )

                    select * from final

                ) as DBT_INTERNAL_SOURCE
                on (
                        DBT_INTERNAL_SOURCE.post_evar87_metadata_sk = DBT_INTERNAL_DEST.post_evar87_metadata_sk
                    )
                    when matched then update set
                        "POST_EVAR87" = DBT_INTERNAL_SOURCE."POST_EVAR87","TRACKING_CODE" = DBT_INTERNAL_SOURCE."TRACKING_CODE","REFERRER" = DBT_INTERNAL_SOURCE."REFERRER","MAX_DATE_TIME_UTC" = DBT_INTERNAL_SOURCE."MAX_DATE_TIME_UTC","DBT_UPDATED_AT_UTC" = DBT_INTERNAL_SOURCE."DBT_UPDATED_AT_UTC","TRACKING_CODE_FROM_MAPPING" = DBT_INTERNAL_SOURCE."TRACKING_CODE_FROM_MAPPING","TRAFFIC_CHANNEL" = DBT_INTERNAL_SOURCE."TRAFFIC_CHANNEL","V87_PARTNER" = DBT_INTERNAL_SOURCE."V87_PARTNER","V87_PRODUCT" = DBT_INTERNAL_SOURCE."V87_PRODUCT","V87_EXECUTING_BU" = DBT_INTERNAL_SOURCE."V87_EXECUTING_BU","V87_REQUESTING_BU" = DBT_INTERNAL_SOURCE."V87_REQUESTING_BU","V87_COMMUNICATION_NAME" = DBT_INTERNAL_SOURCE."V87_COMMUNICATION_NAME","V87_CAMPAIGN_NAME" = DBT_INTERNAL_SOURCE."V87_CAMPAIGN_NAME","V87_CAMPTOOL_TARGETING_LEVEL" = DBT_INTERNAL_SOURCE."V87_CAMPTOOL_TARGETING_LEVEL","V87_BRAZE_TAG_TYPE" = DBT_INTERNAL_SOURCE."V87_BRAZE_TAG_TYPE","V87_BRAZE_PLACEMENT" = DBT_INTERNAL_SOURCE."V87_BRAZE_PLACEMENT","V87_FORMAT" = DBT_INTERNAL_SOURCE."V87_FORMAT","V87_AGENCY" = DBT_INTERNAL_SOURCE."V87_AGENCY","POST_EVAR87_METADATA_SK" = DBT_INTERNAL_SOURCE."POST_EVAR87_METADATA_SK"
                    when not matched then insert
                        ("POST_EVAR87", "TRACKING_CODE", "REFERRER", "MAX_DATE_TIME_UTC", "DBT_UPDATED_AT_UTC", "TRACKING_CODE_FROM_MAPPING", "TRAFFIC_CHANNEL", "V87_PARTNER", "V87_PRODUCT", "V87_EXECUTING_BU", "V87_REQUESTING_BU", "V87_COMMUNICATION_NAME", "V87_CAMPAIGN_NAME", "V87_CAMPTOOL_TARGETING_LEVEL", "V87_BRAZE_TAG_TYPE", "V87_BRAZE_PLACEMENT", "V87_FORMAT", "V87_AGENCY", "POST_EVAR87_METADATA_SK")
                        values
                        ("POST_EVAR87", "TRACKING_CODE", "REFERRER", "MAX_DATE_TIME_UTC", "DBT_UPDATED_AT_UTC", "TRACKING_CODE_FROM_MAPPING", "TRAFFIC_CHANNEL", "V87_PARTNER", "V87_PRODUCT", "V87_EXECUTING_BU", "V87_REQUESTING_BU", "V87_COMMUNICATION_NAME", "V87_CAMPAIGN_NAME", "V87_CAMPTOOL_TARGETING_LEVEL", "V87_BRAZE_TAG_TYPE", "V87_BRAZE_PLACEMENT", "V87_FORMAT", "V87_AGENCY", "POST_EVAR87_METADATA_SK")
                ;
            {% endset %}

            {% set results = run_query(query) %}
            {{ log(v_date ~ ": " ~ results.columns[0].values()[0] ~ " rows inserted.", info=True) }}
            {{ log(v_date ~ ": " ~ results.columns[1].values()[0] ~ " rows updated.", info=True) }}

        {% else %}

            {{ log(v_date ~ " ==> not a valid date, skipped.", info=True) }}

        {% endif %}

    {% endfor %}

{% endif %}

{% endmacro %}