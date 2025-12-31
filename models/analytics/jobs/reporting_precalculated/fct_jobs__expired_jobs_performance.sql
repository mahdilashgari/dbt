with

postings as (
    select
        p.posting_id,
        p.apply_type,
        p.discipline_name_de,
        p.discipline_name_en,
        case
            when p.role_reference_position > 0 then 'Other'
            else p.role_name_de
        end                                                                 as role_name_de,
        case
            when p.role_reference_position > 0 then 'Other'
            else p.role_name_en
        end                                                                 as role_name_en,
        coalesce(s.skill_level, 'Unknown')                                  as skill_level,
        case
            when p.product_type in ('Core-360', 'Old products (0): Core-360') then 'Core-360'
            when p.product_type = 'New: Core15 (5)' then 'Core15'
            when p.product_type = 'New: Essential (4)' then 'Essential'
            when p.product_type = 'New: Pro (2)' then 'Pro'
            when p.product_type = 'New: Ultimate (3)' then 'Ultimate'
            when p.product_type = 'Basic' then p.product_type
            else 'Other'
        end                                                                 as product_type, -- requested products
        p.is_third_party_jobs_partner, -- CPC flag
        to_date(convert_timezone('UTC', 'Europe/Berlin', p.expired_at_utc)) as expired_date
    from {{ ref('dim_jobs__postings') }} as p
        left join {{ ref('int_jobs__postings_skill_level') }} as s
            on p.posting_id = s.posting_id
    where
        true
        and expired_date between date_trunc('MM', current_date - interval '1 YEAR') and date_trunc('MM', current_date) - interval '1 DAY'
),

job_detail_views as (
    select
        posting_id,
        count(distinct detail_view_id) as job_detail_views
    from {{ ref('fct_jobs__detail_views') }}
    group by all
),

apply_intentions as (
    select
        posting_id,
        count(distinct apply_intention_id)                                                                    as apply_intentions,
        count(
            distinct case when job_apply_type in ('Instant Apply', 'Easy Apply') then apply_intention_id end
        )                                                                                                     as instant_apply_intentions,
        count(distinct case when traffic_channel_class in ('Organic', 'Unknown') then apply_intention_id end) as organic_apply_intentions,
        count(distinct case when traffic_channel_class in ('Sponsored') then apply_intention_id end)          as paid_apply_intentions
    from {{ ref('fct_jobs__apply_intentions') }}
    group by all
),

apply_completions as (
    select
        posting_id,
        count(
            distinct case when job_apply_type in ('Quick Instant Apply', 'Instant Apply', 'Easy Apply') then apply_completion_unique_id end
        ) as instant_apply_completions
    from {{ ref('fct_jobs__apply_completions') }}
    group by all
),

postings_performance as (
    select
        p.*,
        coalesce(jdv.job_detail_views, 0)                                                  as job_detail_views,
        coalesce(ai.apply_intentions, 0)                                                   as apply_intentions,
        coalesce(ai.instant_apply_intentions, 0)                                           as instant_apply_intentions,
        coalesce(ai.organic_apply_intentions, 0)                                           as organic_apply_intentions,
        coalesce(ai.paid_apply_intentions, 0)                                              as paid_apply_intentions,
        coalesce(ac.instant_apply_completions, 0)                                          as instant_apply_completions,
        p.apply_type in ('Instant Apply', 'Easy Apply') or ai.instant_apply_intentions > 0 as posting_has_instant_apply
    from postings as p
        left join job_detail_views as jdv
            on p.posting_id = jdv.posting_id
        left join apply_intentions as ai
            on p.posting_id = ai.posting_id
        left join apply_completions as ac
            on p.posting_id = ac.posting_id
),

final as (
    select
        posting_id,
        apply_type,
        product_type,
        discipline_name_de,
        discipline_name_en,
        role_name_en,
        role_name_de,
        skill_level,
        job_detail_views,
        apply_intentions,
        instant_apply_intentions,
        organic_apply_intentions,
        paid_apply_intentions,
        instant_apply_completions,
        posting_has_instant_apply,
        expired_date
    from postings_performance
    where product_type != 'Other'

    union all

    select
        posting_id,
        apply_type,
        'Core-CPC' as product_type,
        discipline_name_de,
        discipline_name_en,
        role_name_en,
        role_name_de,
        skill_level,
        job_detail_views,
        apply_intentions,
        instant_apply_intentions,
        organic_apply_intentions,
        paid_apply_intentions,
        instant_apply_completions,
        posting_has_instant_apply,
        expired_date
    from postings_performance
    where is_third_party_jobs_partner
)

select * from final
