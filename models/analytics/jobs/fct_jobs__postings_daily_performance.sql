{{
    config(
        materialized = 'incremental',
        unique_key = ['posting_id','date_id'],
        snowflake_warehouse = 'xing_dbt_wh_large',
        tags = ["jobs__b2b_performance_metrics", "jobs__daily"]
    )
}}

{% set start_date = '2022-01-01' %}

with

-- Incremental strategy
{% if is_incremental() %}
    incremental_cte as (
        select distinct posting_id
        from {{ ref('fct_jobs__card_views') }}
        where to_date(convert_timezone('UTC', 'Europe/Berlin', created_at_utc)) >= dateadd('day', -30, current_date)
        union distinct
        select distinct posting_id
        from {{ ref('fct_jobs__bookmarks') }}
        where to_date(convert_timezone('UTC', 'Europe/Berlin', created_at_utc)) >= dateadd('day', -30, current_date)
        union distinct
        select distinct posting_id
        from {{ ref('fct_jobs__detail_views') }}
        where to_date(convert_timezone('UTC', 'Europe/Berlin', created_at_utc)) >= dateadd('day', -30, current_date)
        union distinct
        select distinct posting_id
        from {{ ref('fct_jobs__apply_intentions') }}
        where to_date(convert_timezone('UTC', 'Europe/Berlin', created_at_utc)) >= dateadd('day', -30, current_date)
        union distinct
        select distinct posting_id
        from {{ ref('fct_jobs__apply_completions') }}
        where to_date(convert_timezone('UTC', 'Europe/Berlin', created_at_utc)) >= dateadd('day', -30, current_date)
    ),
{% endif %}

postings as (
    select
        v.posting_id,
        v.date_id
    from {{ ref('int_jobs__job_visible_daily') }} as v
        {% if is_incremental() %}
            inner join incremental_cte as i
                on v.posting_id = i.posting_id
        {% endif %}
    group by all
    having min(v.date_id) >= '{{ start_date }}'
),

-- Card Views
job_impressions_postings_daily as (
    select
        c.posting_id,
        to_date(convert_timezone('UTC', 'Europe/Berlin', c.created_at_utc)) as date_id,
        count(distinct c.card_view_sk)                                      as job_impression_counter
    from {{ ref('fct_jobs__card_views') }} as c
        {% if is_incremental() %}
            inner join incremental_cte as i
                on c.posting_id = i.posting_id
        {% endif %}
    where date_id >= '{{ start_date }}'
    group by all
),

-- Bookmarks
bookmarks_postings_daily as (
    select
        b.posting_id,
        to_date(convert_timezone('UTC', 'Europe/Berlin', b.created_at_utc)) as date_id,
        count(distinct b.bookmark_sk)                                       as bookmark_counter
    from {{ ref('fct_jobs__bookmarks') }} as b
        {% if is_incremental() %}
            inner join incremental_cte as i
                on b.posting_id = i.posting_id
        {% endif %}
    where date_id >= '{{ start_date }}'
    group by all
),

-- Job Detail Views and Unique Visitors
job_detail_views as (
    select
        j.detail_view_id,
        j.posting_id,
        to_date(convert_timezone('UTC', 'Europe/Berlin', j.created_at_utc)) as date_id,
        iff(coalesce(j.xing_user_id, 0) = 0, j.visitor_id, j.xing_user_id)  as user_or_visitor_id
    from {{ ref('fct_jobs__detail_views') }} as j
        {% if is_incremental() %}
            inner join incremental_cte as i
                on j.posting_id = i.posting_id
        {% endif %}
    where date_id >= '{{ start_date }}'
    -- and bot_name is null --as NWT automatically excludes all bot clicks and we don't store the bot information anymore, differentiation not necessary for data after 2024
),

job_detail_views_postings_daily as (
    select
        posting_id,
        date_id,
        count(distinct detail_view_id)     as job_detail_views,
        count(distinct user_or_visitor_id) as unique_visitors
    from job_detail_views
    group by all
),

unique_visitors as (
    select
        posting_id,
        date_id,
        user_or_visitor_id
    from job_detail_views
    -- where bot_name is null
    group by all
    having min(date_id) >= '{{ start_date }}'
    qualify row_number() over (
        partition by posting_id, user_or_visitor_id
        order by date_id asc
    ) = 1
),

unique_visitors_count as (
    select
        posting_id,
        date_id,
        count(user_or_visitor_id) as unique_visitors
    from unique_visitors
    group by all
),

-- Apply Intentions
apply_intentions_postings_daily as (
    select
        a.posting_id,
        to_date(convert_timezone('UTC', 'Europe/Berlin', a.created_at_utc))                        as date_id,
        count(distinct a.apply_intention_id)                                                       as ai_counter,
        count(distinct case when a.job_apply_type = 'Url' then a.apply_intention_id end)           as ai_url_apply_counter,
        count(distinct case when a.job_apply_type = 'Instant Apply' then a.apply_intention_id end) as ai_instant_apply_counter
    from {{ ref('fct_jobs__apply_intentions') }} as a
        {% if is_incremental() %}
            inner join incremental_cte as i
                on a.posting_id = i.posting_id
        {% endif %}
    where date_id >= '{{ start_date }}'
    group by all
),

-- Apply Completions
apply_completions as (
    select
        a.posting_id,
        to_date(convert_timezone('UTC', 'Europe/Berlin', a.created_at_utc)) as date_id,
        a.apply_completion_unique_id
    from {{ ref('fct_jobs__apply_completions') }} as a
        {% if is_incremental() %}
            inner join incremental_cte as i
                on a.posting_id = i.posting_id
        {% endif %}
    where date_id >= '{{ start_date }}'
),

unique_apply_completions_aux as (
    select
        posting_id,
        date_id,
        apply_completion_unique_id
    from apply_completions
    group by all
    having min(date_id) >= '{{ start_date }}'
    qualify row_number() over (
        partition by posting_id, apply_completion_unique_id
        order by date_id asc
    ) = 1
),

unique_apply_completions as (
    select
        posting_id,
        date_id,
        count(apply_completion_unique_id) as unique_applications
    from unique_apply_completions_aux
    group by all
),

apply_completions_daily as (
    select
        posting_id,
        date_id,
        count(distinct apply_completion_unique_id) as daily_applications --Needed as there are duplicates for some users
    from apply_completions
    group by all
),

-- All this is done so as to fix the missing dates in the visible postings table (ensures all relevant dates are included)
visible_postings as (
    select *
    from postings
    union distinct
    select
        posting_id,
        date_id
    from job_detail_views_postings_daily
    union distinct
    select
        posting_id,
        date_id
    from bookmarks_postings_daily
    union distinct
    select
        posting_id,
        date_id
    from apply_intentions_postings_daily
    union distinct
    select
        posting_id,
        date_id
    from apply_completions_daily
    union distinct
    select
        posting_id,
        date_id
    from job_impressions_postings_daily
),

final as (
    select
        p.date_id,
        p.posting_id,
        coalesce(jdv.job_detail_views, 0)        as job_detail_views,               -- 1. Number of Visits (A.K.A Job clicks)
        coalesce(bm.bookmark_counter, 0)         as bookmarks,                      -- 2. Number of users who bookmarked the job
        coalesce(ai.ai_counter, 0)               as apply_intentions,               -- 3. Number of apply intentions
        coalesce(uvc.unique_visitors, 0)         as unique_visitors,                -- 4. Number of unique visitors (Jobs detail page)
        coalesce(jdv.unique_visitors, 0)         as daily_unique_visitors,          -- Daily unique visitors
        coalesce(ai.ai_url_apply_counter, 0)     as apply_intentions_url_apply,     -- 5. Number of button clicks for external job application forms
        coalesce(ai.ai_instant_apply_counter, 0) as apply_intentions_instant_apply, -- 8. Number of apply button clicks for instant apply
        coalesce(uac.unique_applications, 0)     as applications,                   -- 6. Number of applications for instant apply
        coalesce(ac.daily_applications, 0)       as daily_applications,             -- Daily applications
        coalesce(jcv.job_impression_counter, 0)  as impressions                     -- 7. Job Impressions | modelled data not available at the moment
    from
        visible_postings as p
        left join job_detail_views_postings_daily as jdv
            on p.posting_id = jdv.posting_id and p.date_id = jdv.date_id
        left join unique_visitors_count as uvc
            on p.posting_id = uvc.posting_id and p.date_id = uvc.date_id
        left join bookmarks_postings_daily as bm
            on p.posting_id = bm.posting_id and p.date_id = bm.date_id
        left join apply_intentions_postings_daily as ai
            on p.posting_id = ai.posting_id and p.date_id = ai.date_id
        left join apply_completions_daily as ac
            on p.posting_id = ac.posting_id and p.date_id = ac.date_id
        left join unique_apply_completions as uac
            on p.posting_id = uac.posting_id and p.date_id = uac.date_id
        left join job_impressions_postings_daily as jcv
            on p.posting_id = jcv.posting_id and p.date_id = jcv.date_id
    order by p.posting_id, p.date_id
)

select *
from final
