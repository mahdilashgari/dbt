{{
    config(
        materialized = 'table',
        unique_key = 'user_and_date_id',
        snowflake_warehouse = 'XING_DBT_WH_MEDIUM',
        tags=['analytics_layer', 'daily', 'xing-network', 'contact', 'XA-7534']
    )
}}

with
xing_network_contact_requests_declined as (
    select * from {{ ref('int_xing_network__contact_requests_declined') }}
)

select
    xing_user_id_date_id_sk as user_and_date_id,
    date_id,
    xing_user_id,
    contact_requests_declined
from xing_network_contact_requests_declined
