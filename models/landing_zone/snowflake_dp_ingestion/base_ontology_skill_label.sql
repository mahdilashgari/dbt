{{
  config(
    materialized = 'view',
    unique_key = ['skill', 'label', 'lang'],
    tags= ['snowflake_data_sharing'],
    schema = 'snowflake_dp_ingestion',
    )
}}

select *
from {{ source('public_ontology', 'ontology_skill_label') }}
