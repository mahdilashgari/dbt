{% macro bulk_load_int_datafeeds__post_evar87_metadata_yearly(p_year) %}

{#
-- ! Is used to load data starting from 2017-05, 2017-04 data is loaded using monthly version of this macro since the data is huge for this period
-- dbt run-operation bulk_load_int_datafeeds__post_evar87_metadata_yearly --args '{p_year: 2017}'
-- This macro loads bulk data into INT_DATAFEEDS__POST_EVAR87_METADATA for a given year
-- All months of the given year are iterated within a loop and whole month is loaded at once during iteration
#}

{% if execute %}

    {% do run_query("use warehouse BI_DBT_WH_XXXLARGE")  %}

    {%- set v_year = p_year -%}

    {% for month_id in range(1, 13, 1) %}

        {%- set v_month = month_id -%}

        {{ log("YEAR = " ~ v_year ~ ", MONTH = " ~ v_month ~ " ==> executing...", info=True) }}

        {% set query %}

            merge into {{ ref('central_int_datafeeds__post_evar87_metadata') }} as DBT_INTERNAL_DEST
            using
            (
                with
                v87_from_daily_visits as (
                    select * from {{ ref('central_int_datafeeds__post_evar87_list') }}
                    where
                        true
                        and to_date(max_date_time_utc) >= '2017-05-01' --this macro is used to load data starting from 2017-05, 2017-04 data is loaded using monthly version of this macro since the data is huge for this period
                        and year(max_date_time_utc) = {{ v_year }}
                        and month(max_date_time_utc) = {{ v_month }}
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
        {{ log("YEAR = " ~ v_year ~ ", MONTH = " ~ v_month ~ " ==> " ~ results.columns[0].values()[0] ~ " rows inserted.", info=True) }}
        {{ log("YEAR = " ~ v_year ~ ", MONTH = " ~ v_month ~ " ==> " ~ results.columns[1].values()[0] ~ " rows updated.", info=True) }}

    {% endfor %}

{% endif %}

{% endmacro %}