{{
  config(
    materialized = 'view',
    unique_key = 'user_id',
    tags= ['snowflake_data_sharing'],
    schema = 'snowflake_dp_ingestion',
    )
}}

select *
from {{ source('public_xing_xas', 'aud_users_with_predictions_pub') }}
where
    1 = 1
