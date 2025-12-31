{{
  config(
    materialized = 'view',
    unique_key = 'urn',
    tags= ['snowflake_data_sharing'],
    schema = 'snowflake_dp_ingestion',
    )
}}

select *
from {{ source('public_xing_data_legacy', 'interactions_counter') }}
