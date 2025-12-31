with deleted_xing_accounts as (

    select
        'xing_account'     as pii_id,
        account_id::string as deleted_id,
        deleted_at
    from {{ ref('mysql_accounts__deleted_accounts') }}

),

deleted_xing_webtracking_hashes as (

    select
        'xing_webtracking_hash' as pii_id,
        h.webtracking_hash      as deleted_id,
        d.deleted_at
    from {{ ref('mysql_accounts__deleted_accounts') }} as d
        inner join {{ ref('stg_user__hashes') }} as h
            on d.account_id = h.xing_user_id

),

soft_deleted_salesforce_onlyfy_leads as (

    select distinct
        'salesforce_onlyfy_lead' as pii_id,
        id                       as deleted_id,
        null                     as deleted_at
    from {{ ref('salesforce_onlyfy__lead_hst') }}
    where isdeleted

),

hard_deleted_salesforce_onlyfy_leads as (

    select
        'salesforce_onlyfy_lead' as pii_id,
        id                       as deleted_id,
        null                     as deleted_at
    from {{ ref('salesforce_onlyfy__lead') }}
    where dbt_is_deleted

),

soft_deleted_salesforce_onlyfy_contacts as (

    select
        'salesforce_onlyfy_contact' as pii_id,
        id                          as deleted_id,
        null                        as deleted_at
    from {{ ref('salesforce_onlyfy__contact') }}
    where isdeleted

),

hard_deleted_salesforce_onlyfy_contacts as (

    select
        'salesforce_onlyfy_contact' as pii_id,
        id                          as deleted_id,
        null                        as deleted_at
    from {{ ref('salesforce_onlyfy__contact') }}
    where dbt_is_deleted

),

hard_deleted_braze_onlyfy_user_profiles as (

    select
        'braze_onlyfy_user_profile' as pii_id,
        external_id                 as deleted_id,
        null                        as deleted_at
    from {{ ref('braze_api_onlyfy__user_profiles') }}
    where dbt_is_deleted
),

hard_deleted_braze_xing_user_profiles as (

    select
        'braze_xing_user_profile' as pii_id,
        external_id               as deleted_id,
        null                      as deleted_at
    from {{ ref('braze_api_xing__user_profiles') }}
    where dbt_is_deleted
),

soft_deleted_prescreen_candidates as (

    select
        'prescreen_candidate' as pii_id,
        id::string            as deleted_id,
        deleted_at
    from {{ ref('mariadb_prescreen__candidate') }}
    where deleted_at is not null

),

hard_deleted_prescreen_candidates as (

    select
        'prescreen_candidate' as pii_id,
        id::string            as deleted_id,
        null                  as deleted_at
    from {{ ref('mariadb_prescreen__candidate') }}
    where dbt_is_deleted

),

soft_deleted_prescreen_users as (

    select
        'prescreen_user'   as pii_id,
        id::string         as deleted_id,
        data_anonymized_at as deleted_at
    from {{ ref('mariadb_prescreen__user') }}
    where data_anonymized_at is not null

),

hard_deleted_prescreen_users as (

    select
        'prescreen_user' as pii_id,
        id::string       as deleted_id,
        null             as deleted_at
    from {{ ref('mariadb_prescreen__user') }}
    where dbt_is_deleted

),

soft_deleted_prescreen_job_applications as (

    select
        'prescreen_job_application' as pii_id,
        ja.id::string               as deleted_id,
        ca.deleted_at
    from {{ ref('mariadb_prescreen__job_application') }} as ja
        inner join {{ ref('mariadb_prescreen__candidate') }} as ca
            on ja.candidate_id = ca.id
    where ca.deleted_at is not null

),

hard_deleted_prescreen_job_applications as (

    select
        'prescreen_job_application' as pii_id,
        id::string                  as deleted_id,
        null                        as deleted_at
    from {{ ref('mariadb_prescreen__job_application') }}
    where dbt_is_deleted

),

soft_deleted_informix_db_cra_resource as (

    select
        'nwse_resourceid'  as pii_id,
        resourceid::string as deleted_id,
        dateinactive       as deleted_at
    from {{ ref('informix_db_cra__resource') }}
    where active = false

),

hard_deleted_informix_db_cra_resource as (

    select
        'nwse_resourceid'  as pii_id,
        resourceid::string as deleted_id,
        null               as deleted_at
    from {{ ref('informix_db_cra__resource') }}
    where dbt_is_deleted

),

soft_deleted_zendesk_xing_users as (

    select
        'zendesk_xing_userid' as pii_id,
        id::string            as deleted_id,
        updated_at            as deleted_at
    from {{ ref('zendesk_api__zendesk_xing_tickets') }}
    where status = 'deleted'

),

hard_deleted_zendesk_xing_users as (

    select
        'zendesk_xing_userid' as pii_id,
        id::string            as deleted_id,
        null                  as deleted_at
    from {{ ref('zendesk_api__zendesk_xing_tickets') }}
    where dbt_is_deleted

),

hard_deleted_novomind_addressbook as (

    select
        'novomind_addressbookid' as pii_id,
        id::string               as deleted_id,
        null                     as deleted_at
    from {{ ref('ftp_novomind__addressbook') }}
    where dbt_is_deleted
),

hard_deleted_novomind_email as (

    select
        'novomind_emailid' as pii_id,
        address_id::string as deleted_id,
        null               as deleted_at
    from {{ ref('ftp_novomind__email') }}
    where dbt_is_deleted
),

hard_deleted_talenthub_invitation as (

    select
        'talenthub_invitationid' as pii_id,
        id::string               as deleted_id,
        null                     as deleted_at
    from {{ ref('mariadb_talenthub__invitation') }}
    where dbt_is_deleted
),

hard_deleted_talenthub_profile as (

    select
        'talenthub_profileid' as pii_id,
        id::string            as deleted_id,
        null                  as deleted_at
    from {{ ref('mariadb_talenthub__profile') }}
    where dbt_is_deleted
),

hard_deleted_talenthub_talent as (

    select
        'talenthub_talentid' as pii_id,
        id::string           as deleted_id,
        null                 as deleted_at
    from {{ ref('mariadb_talenthub__talent') }}
    where dbt_is_deleted
),

hard_deleted_ecom_billingdetails as (

    select
        'ecom_billingdetailsid' as pii_id,
        id::string              as deleted_id,
        null                    as deleted_at
    from {{ ref('mariadb_ecom__billing_details') }}
    where dbt_is_deleted

)

select * from deleted_xing_accounts
union all
select * from deleted_xing_webtracking_hashes
union all
select * from hard_deleted_salesforce_onlyfy_leads
union all
select * from soft_deleted_salesforce_onlyfy_leads
union all
select * from hard_deleted_salesforce_onlyfy_contacts
union all
select * from soft_deleted_salesforce_onlyfy_contacts
union all
select * from hard_deleted_braze_onlyfy_user_profiles
union all
select * from hard_deleted_braze_xing_user_profiles
union all
select * from soft_deleted_prescreen_candidates
union all
select * from hard_deleted_prescreen_candidates
union all
select * from soft_deleted_prescreen_users
union all
select * from hard_deleted_prescreen_users
union all
select * from soft_deleted_prescreen_job_applications
union all
select * from hard_deleted_prescreen_job_applications
union all
select * from soft_deleted_informix_db_cra_resource
union all
select * from hard_deleted_informix_db_cra_resource
union all
select * from soft_deleted_zendesk_xing_users
union all
select * from hard_deleted_zendesk_xing_users
union all
select * from hard_deleted_novomind_addressbook
union all
select * from hard_deleted_novomind_email
union all
select * from hard_deleted_talenthub_invitation
union all
select * from hard_deleted_talenthub_profile
union all
select * from hard_deleted_talenthub_talent
union all
select * from hard_deleted_ecom_billingdetails
