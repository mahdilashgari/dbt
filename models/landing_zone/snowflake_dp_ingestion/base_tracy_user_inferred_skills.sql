{{
  config(
    materialized = 'view',
    unique_key = ['profile_id', 'skill_id'],
    tags= ['snowflake_data_sharing'],
    schema = 'snowflake_dp_ingestion',
    )
}}

select
    profile_id,
    skill_id
from {{ source('public_tracy', 'tracy_user_inferred_skills') }}
where
    1 = 1
