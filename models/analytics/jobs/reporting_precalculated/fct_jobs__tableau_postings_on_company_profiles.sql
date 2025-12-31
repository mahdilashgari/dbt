{{ 
    config(
        snowflake_warehouse = 'xing_dbt_wh_large',
        tags = ["jobs__adhoc_reports", "jobs__monthly"]

) }}

with visible_postings_with_org_and_performance as (
    select *
    from {{ ref('xing_stg_jobs__visible_postings_in_company_profiles_extended') }}
    where
        true
        -- only showing postings in the last 12 months
        and first_visible_date >= date_trunc('month', dateadd(month, -12, current_date()))
        and last_visible_date <= last_day(dateadd(month, -1, current_date()))
),

all_info as (
    select
        paid_type
            as dim_paid_type,
        is_instant_posting
            as instant_posting_category,
        'all'
            as company_id,
        'all'
            as company_name,
        'all'
            as organization_id,
        'all'
            as organization_name,
        'all'
            as account_id,
        'all'
            as account_name,
        count(distinct posting_id)
            as postings,
        sum(job_detail_views)
            as job_detail_views,
        sum(instant_apply_intentions)
            as instant_apply_intentions,
        sum(non_instant_apply_intentions)
            as non_instant_apply_intentions,
        case when paid_type = 'Paid' then postings else 0 end
            as paid_postings,
        case when paid_type = 'Unpaid' then postings else 0 end
            as unpaid_postings,
        case when is_instant_posting = 1 then postings else 0 end
            as instant_postings,
        iff(paid_type = 'Paid', 'Paid', 'Unpaid')
            as company_paid_posting_type,
        0
            as unpaid_to_paid_ratio
    from visible_postings_with_org_and_performance
    group by all
),

all_companies as (
    select
        'all'
            as dim_paid_type,
        0
            as instant_posting_category,
        company_name,
        count(distinct posting_id)
            as postings,
        sum(job_detail_views)
            as job_detail_views,
        sum(instant_apply_intentions)
            as instant_apply_intentions,
        sum(non_instant_apply_intentions)
            as non_instant_apply_intentions,
        sum(case when paid_type = 'Paid' then 1 else 0 end)
            as paid_postings,
        sum(case when paid_type = 'Unpaid' then 1 else 0 end)
            as unpaid_postings,
        sum(is_instant_posting)
            as instant_postings,
        case
            when paid_postings > 0 and unpaid_postings > 0 then 'Both'
            when paid_postings > 0 and unpaid_postings = 0 then 'Paid'
            when unpaid_postings > 0 then 'Unpaid'
        end
            as company_paid_posting_type,
        case when paid_postings = 0 then 0 else unpaid_postings / paid_postings end
            as unpaid_to_paid_ratio
    from visible_postings_with_org_and_performance
    group by all
),

distinct_companies_with_info_aggregated as (
    select
        ac.company_name,
        listagg(distinct to_char(
            case
                when
                    (ac.company_paid_posting_type = 'Both' and p.paid_type = 'Paid')
                    or ac.company_paid_posting_type != 'Both'
                then p.company_id
            end
        ), ', ')
            as company_id,
        listagg(distinct to_char(
            case
                when
                    (ac.company_paid_posting_type = 'Both' and p.paid_type = 'Paid')
                    or ac.company_paid_posting_type != 'Both'
                then p.organization_id
            end
        ), ', ')
            as organization_id,
        listagg(distinct case
            when
                (ac.company_paid_posting_type = 'Both' and p.paid_type = 'Paid')
                or ac.company_paid_posting_type != 'Both'
            then p.organization_name
        end, ', ')
            as organization_name,
        listagg(distinct to_char(
            case
                when
                    (ac.company_paid_posting_type = 'Both' and p.paid_type = 'Paid')
                    or ac.company_paid_posting_type != 'Both'
                then p.account_id
            end
        ), ', ')
            as account_id,
        listagg(distinct case
            when
                (ac.company_paid_posting_type = 'Both' and p.paid_type = 'Paid')
                or ac.company_paid_posting_type != 'Both'
            then p.account_name
        end, ', ')
            as account_name
    from all_companies as ac
        left join visible_postings_with_org_and_performance as p
            on ac.company_name = p.company_name
    group by all
),

all_companies_extended as (
    select
        ac.dim_paid_type,
        ac.instant_posting_category,
        aa.company_id,
        ac.company_name,
        aa.organization_id,
        aa.organization_name,
        aa.account_id,
        aa.account_name,
        ac.postings,
        ac.job_detail_views,
        ac.instant_apply_intentions,
        ac.non_instant_apply_intentions,
        ac.paid_postings,
        ac.unpaid_postings,
        ac.instant_postings,
        ac.company_paid_posting_type,
        ac.unpaid_to_paid_ratio
    from all_companies as ac
        left join distinct_companies_with_info_aggregated as aa
            on ac.company_name = aa.company_name
),

combined_summary_and_companies as (
    select *
    from all_info

    union all

    select *
    from all_companies_extended
),

final as (
    select
        *,
        date_trunc('month', dateadd(month, -12, current_date()))
            as minimum_visible_date_report,
        last_day(dateadd(month, -1, current_date()))
            as maximum_visible_date_report
    from combined_summary_and_companies
)

select * from final
