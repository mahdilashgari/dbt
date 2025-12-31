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
    level_id::number              as level_id,
    level_id_fine_grained::number as level_id_fine_grained,
from {{ source('public_tracy', 'tracy_user_career_level') }}
where
    1 = 1
