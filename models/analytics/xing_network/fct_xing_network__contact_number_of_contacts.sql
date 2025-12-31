{{
    config(
        materialized = 'table',
        unique_key = 'xing_user_id',
        snowflake_warehouse = 'XING_DBT_WH_MEDIUM',
        tags=['analytics_layer', 'weekly', 'xing-network', 'contact', 'XA-7552']
    )
}}

with
xing_network_contact_number_of_contacts as (
    select * from {{ ref('int_xing_network__contact_number_of_contacts') }}
)

select
    xing_user_id,
    contact_number_of_contacts,
    last_contact_event_date
from xing_network_contact_number_of_contacts
