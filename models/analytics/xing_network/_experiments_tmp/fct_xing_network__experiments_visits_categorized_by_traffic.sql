{{
    config(
        materialized = 'incremental', 
        unique_key = ['experiment_traffic_sk'],        
        snowflake_warehouse = 'XING_DBT_WH_MEDIUM',
        tags=['analytics_layer', 'daily', 'experiments', 'visits-by-traffic', 'XA-7555']
    )
}}

with
fct_xing_network__experiments_visits_categorized_by_traffic as (
    select * from {{ ref('int_xing_network__experiments_visits_categorized_by_traffic') }}
)

select * from fct_xing_network__experiments_visits_categorized_by_traffic
where
    true
    and adobe_segment_name in
    (
        -- 00_welcome --
        'trf_00a_welcome_login_nwt_XA-7217_H',
        'trf_00b_welcome_signup_nwt_XA-7217_H',
        'trf_00c_welcome_start_nwt_XA-7217_H',
        'trf_00d_onboarding_nwt_XA-7217_H',
        'trf_00e_welcome_logout_nwt_XA-7217_H',
        -- 01_jobs --
        'trf_01a_jobs_nwt_XA-7217_H',
        'trf_01b_jobs_search_nwt_XA-7217_H',
        'trf_01c_jobs_easy_create_nwt_XA-7217_H',
        -- 02_people --
        'trf_02a_profile_other_nwt_XA-7217_H',
        'trf_02b_messenger_nwt_XA-7217_H',
        'trf_02c_members_search_nwt_XA-7217_H',
        'trf_02d_people_directory_nwt_XA-7217_H',
        'trf_02e_network_nwt_XA-7217_H',
        -- 03_profile --	
        'trf_03a_profile_self_nwt_XA-7217_H',
        'trf_03b_preferences_nwt_XA-7217_H',
        'trf_03c_lebenslauf_nwt_XA-7217_H',
        'trf_03d_settings_nwt_XA-7217_H',
        -- 04_content --		
        'trf_04a_insights_nwt_XA-7217_H',
        'trf_04b_news_nwt_XA-7217_H',
        'trf_04c_entity_pages_nwt_XA-7217_H',
        'trf_04d_companies_nwt_XA-7217_H',
        'trf_04e_companies_search_nwt_XA-7217_H',
        -- 05_notifications --		
        'trf_05a_notifications_nwt_XA-7217_H',
        'trf_05b_activity_center_nwt_XA-7217_H',
        -- 06_premium_and_projobs --			
        'trf_06a_premium_nwt_XA-7217_H',
        'trf_06b_projobs_nwt_XA-7217_H',
        'trf_06c_purchase_funnel_nwt_XA-7217_H',
        -- 07 b2b --
        'trf_07a_business_solutions_nwt_XA-7217_H',
        'trf_07b_advertising_nwt_XA-7217_H',
        -- 08 other --		
        'trf_08a_help_nwt_XA-7217_H',
        'trf_08b_terms_nwt_XA-7217_H'
    -- 'trf_99_other:'
    )
    and date_id >= '2025-10-01'
group by all
order by 1, 2
