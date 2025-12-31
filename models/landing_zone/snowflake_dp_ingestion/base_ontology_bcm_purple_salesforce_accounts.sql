{{
  config(
    materialized = 'view',
    unique_key = 'id',
    tags= ['snowflake_data_sharing'],
    schema = 'snowflake_dp_ingestion',
    )
}}

select *
from {{ source('public_ontology', 'bcm_purple_salesforce_accounts') }}
