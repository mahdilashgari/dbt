{{ config(
    snowflake_warehouse = 'xing_dbt_wh_xxxlarge',
    tags = ["jobs__card_views", "jobs__data_mart_dimension", "jobs__daily"]
) }}

-- starting with 01.01.2024 data from NWT will be used
--noqa: disable=PRS, AM04
with
all_cardviews as (
    {{
        dbt_utils.union_relations(
            relations=[
                ref('int_jobs__card_views_native_nwt'),
                ref('int_jobs__card_views_web_nwt'),
                ref('int_jobs__card_views_exasol')
                ]
        )
    }}
),

final as (
    select
        card_view_sk,
        created_at_utc,
        job_posting_id as posting_id,
        xing_user_id,
        login_status,
        event_sk,
        page_name,
        element_name,
        element_detail,
        notification_type,
        activity_platform,
        visit_id,
        activity_id,
        visitor_id,
        device_type,
        country_code,
        traffic_source_id,
        nwt_dbt_updated_at_utc,
        adobe_dbt_updated_at_utc
    from all_cardviews
)

select * from final
