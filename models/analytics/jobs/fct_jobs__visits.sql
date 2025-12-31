{{
    config(
        materialized = 'incremental',
        unique_key = 'visit_and_platform_id',
        snowflake_warehouse = 'xing_dbt_wh_xlarge',
        tags = ["jobs__visits", "jobs__data_mart_dimension", "jobs__daily"],
        on_schema_change = 'append_new_columns'
    )
}}

with jobs_visits as (
    select
        visit_id,
        visitor_id,
        case when lower(platform) = 'ios' then 'iOS' else platform end   as activity_platform_aggregated, -- in the source it is specified as Ios
        list_of_platforms,
        visit_and_platform_id,
        geo_country_code                                                 as country_code,
        device                                                           as device_type,
        campaign_information,
        visit_start_at,
        visit_start_date,
        xing_user_ids,
        xing_user_id,
        number_of_applications_in_visit,
        is_dach = 1                                                      as is_dach,
        region,
        case when login_state = 1 then 'Logged In' else 'Logged Out' end as login_status,
        membership_status,
        number_of_user_ids_with_jdv,
        user_ids_with_jdv,
        number_of_job_detail_views,
        has_jdv,
        jdv_week_id,
        jdv_month_id,
        traffic_channel,
        traffic_channel_group,
        traffic_channel_class
    from {{ ref('dim_xing_network__visits') }}
    where
        true
        and array_contains('jobs'::variant, visited_sections)

        {% if is_incremental() %}
            and visit_start_date >= (
                select dateadd('day', -42, max(t.visit_start_date)) as max_date
                from {{ this }} as t
            )
        {% endif %}
)

select *
from jobs_visits
