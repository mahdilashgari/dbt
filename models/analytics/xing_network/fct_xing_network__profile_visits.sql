{{
    config(
        materialized = 'table',
        unique_key = 'user_and_date_id',
        snowflake_warehouse = 'XING_DBT_WH_MEDIUM',
        tags=['analytics_layer', 'daily', 'xing-network', 'profile', 'XA-7557']
    )
}}

with
xing_network_profile_visits as (
    select * from {{ ref('int_xing_network__profile_visits') }}
)

select
    xing_user_id_date_id_sk as user_and_date_id,
    date_id,
    xing_user_id,
    profile_visits,
    profile_visits_web,
    profile_visits_ios,
    profile_visits_android,
    profile_visits_by_recruiter,
    profile_visits_web_by_recruiter,
    profile_visits_ios_by_recruiter,
    profile_visits_android_by_recruiter
from xing_network_profile_visits
