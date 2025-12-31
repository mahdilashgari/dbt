{{ config(
    snowflake_warehouse = 'xing_dbt_wh_xxxlarge',
    tags = ["jobs__bookmarks", "jobs__data_mart_dimension", "jobs__daily"]
) }}

-- up until 2024 data from DWH_ANALYTICS.F_JOB_POSTING_BOOKMARK is used; starting with 01.01.2024 data from NWT will be used
--noqa: disable=PRS, AM04
with
all_bookmarks as (
{{
    dbt_utils.union_relations(
        relations=[
            ref('int_jobs__bookmarks_native_nwt'),
            ref('int_jobs__bookmarks_web_nwt'),
            ref('int_jobs__bookmarks_exa')
            ]
    )
}}
),

final as (
    select
        bookmark_sk,
        created_at_utc,
        job_posting_id as posting_id,
        xing_user_id,
        login_status,
        event_sk,
        page_name,
        notification_type,
        activity_platform,
        visit_id,
        device_type,
        activity_id,
        visitor_id,
        country_code,
        traffic_source_id,
        nwt_dbt_updated_at_utc,
        adobe_dbt_updated_at_utc
    from all_bookmarks
)

select * from final
