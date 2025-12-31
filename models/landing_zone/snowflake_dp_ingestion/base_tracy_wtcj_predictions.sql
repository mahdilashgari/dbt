{{
  config(
    materialized = 'view',
    unique_key = 'user_id',
    tags= ['snowflake_data_sharing'],
    schema = 'snowflake_dp_ingestion',
    )
}}

select
    user_id,
    reasons,
    prediction_date,
    probability,
    wtcj_class
from {{ source('public_tracy', 'wtcj_predictions') }}
where
    1 = 1
