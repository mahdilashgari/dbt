{{
    config(
        materialized = 'incremental',
        unique_key = 'visit_and_platform_id',
        snowflake_warehouse = 'XING_DBT_WH_XLARGE',
        tags=['xing-network', 'daily'],
        on_schema_change = 'append_new_columns'
    )
}}

with
adobe_data as (
    select * from {{ ref('xing_stg_central__adobe_tracking_visits') }}

    {% if is_incremental() %}
        where visit_start_date >= (
            select dateadd('day', -42, max(th.visit_start_date)) as max_date
            from {{ this }} as th
        )
    {% endif %}
),

visits_with_jdv as (
    select * from {{ ref('int_xing_network__visits_with_jdv') }}
),

campaign_metadata as (
    select distinct * from {{ ref('xing_stg_jobs__external_traffic_channel') }}
),

users_history as (
    select * from {{ ref('central_dim_xing_users_hst') }}
),

user_memberships as (
    select * from {{ ref('central_dim_xing_user_memberships') }}
),

users_highest_membership as (
    select
        date_id,
        xing_user_id,
        max(
            case
                when membership = 'ProBusiness' then 3
                when membership = 'ProJobs' then 2
                when membership = 'Premium B2C' then 1
                else 0
            end
        ) as membership_type_id
    from user_memberships
    group by all
),

adobe_visits_agg as (
    select
        visit_id,
        visitor_id,
        split(initcap(platform), '_')[0]::varchar                   as platform,
        visit_and_platform_id,
        coalesce(max(
            case
                when geo_country_code = 'Unknown'
                then null else geo_country_code
            end
        ), 'Unknown')                                               as geo_country_code,
        max(device)                                                 as device,
        array_agg(distinct platform)                                as list_of_platforms,
        max(campaign_information)                                   as campaign_information,
        min(visit_start_at)                                         as visit_start_at,
        min(visit_start_date)                                       as visit_start_date,
        array_agg(distinct xing_user_id)                            as xing_user_ids,
        max(xing_user_id)                                           as xing_user_id,
        count(distinct app)                                         as number_of_applications_in_visit,
        max(case when is_dach = 1 then 1 else 0 end)                as is_dach,
        max(case when login_status = 'Logged In' then 1 else 0 end) as login_state,
        array_agg(distinct visited_section) within group (
            order by visited_section
        )                                                           as visited_sections
    from adobe_data
    group by all
)

select
    av.*,
    nja.number_of_user_ids                              as number_of_user_ids_with_jdv,
    nja.user_ids                                        as user_ids_with_jdv,
    nja.number_of_job_detail_views,
    coalesce(nja.number_of_job_detail_views > 0, false) as has_jdv,
    nja.week_id                                         as jdv_week_id,
    nja.month_id                                        as jdv_month_id,
    case
        when av.is_dach = 1 then 'DACH' else
            coalesce(uh.region_business, 'Non-DACH')
    end                                                 as region,
    case
        when um.membership_type_id = 3 then 'ProBusiness'
        when um.membership_type_id = 2 then 'ProJobs'
        when um.membership_type_id = 1 then 'Premium B2C'
        when um.membership_type_id = 0 then 'Basic'
        else 'Basic'
    end                                                 as membership_status,
    coalesce(cm.traffic_channel, 'Unknown')             as traffic_channel,
    coalesce(cm.traffic_channel_group, 'Unknown')       as traffic_channel_group,
    coalesce(cm.traffic_channel_class, 'Unknown')       as traffic_channel_class
from adobe_visits_agg as av
    left join visits_with_jdv as nja
        on av.visit_id = nja.visit_id
    left join users_history as uh
        on
            av.xing_user_id = uh.xing_user_id
            and av.visit_start_date >= uh.valid_from and av.visit_start_date < uh.valid_to
    left join users_highest_membership as um
        on
            av.xing_user_id = um.xing_user_id
            and av.visit_start_date = um.date_id
    left join campaign_metadata as cm
        on av.campaign_information = cm.traffic_id_first_touch_visit_evar_visit
