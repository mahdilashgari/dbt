{{
    config(
        materialized = 'incremental', 
        unique_key = ['experiment_conversion_sk'],        
        snowflake_warehouse = 'XING_DBT_WH_MEDIUM',
        tags=['analytics_layer', 'daily', 'experiments', 'visits-by-conversion', 'XA-7533']
    )
}}

with
    fct_xing_network__experiments_visits_categorized_by_conversion as (
    select * from {{ ref('int_xing_network__experiments_visits_categorized_by_conversion') }}
)
select * from fct_xing_network__experiments_visits_categorized_by_conversion
where true
	and adobe_segment_name in
	(
	'conv_00a_welcome_registered_nwt_XA-7217_H',
	'conv_01a_jobs_jpdv_nwt_XA-7217_H',
	'conv_01b_jobs_apply_intention_nwt_XA-7217_H',
	'conv_01c_jobs_search_performed_nwt_XA-7217_H',
	'conv_02a_profile_other_nwt_XA-7217_H',
	'conv_02b_messenger_opened_nwt_XA-7217_H',
	'conv_02c_contact_requested_nwt_XA-7217_H',
	'conv_02d_members_search_performed_nwt_XA-7217_H',
	'conv_02e_contact_request_accepted_nwt_XA-7217_H',
	'conv_03a_profile_editing_saved_nwt_XA-7217_H',
	'conv_03b_preferences_submitted_nwt_XA-7217_H',
	'conv_04a_content_article_viewed_nwt_XA-7217_H',
	'conv_04b_generic_followed_nwt_XA-7217_H',
	'conv_04c_unfollowed_followed_nwt_XA-7217_H',
	'conv_04d_companies_search_performed_nwt_XA-7217_H',
	'conv_05a_notifications_opened_nwt_XA-7217_H',
	'conv_06a_premium_cancelled-or-revoked_nwt_XA-7217_H',
	'conv_06b_projobs_cancelled-or-revoked_nwt_XA-7217_H',
	'conv_06c1_premium_purchase_funnel_product_viewed_nwt_XA-7217_H',
	'conv_06c2_premium_purchase_funnel_checkout_step_viewed_nwt_XA-7217_H',
	'conv_06c3_premium_purchase_funnel_purchased_nwt_XA-7217_H',
	'conv_06d1_projobs_purchase_funnel_product_viewed_nwt_XA-7217_H',
	'conv_06d2_projobs_purchase_funnel_checkout_step_viewed_nwt_XA-7217_H',
	'conv_06d3_projobs_purchase_funnel_purchased_nwt_XA-7217_H'
	)
	and date_id >= '2025-10-01'
group by all
order by 1,2
