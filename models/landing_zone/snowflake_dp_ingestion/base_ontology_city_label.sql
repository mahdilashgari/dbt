{{
  config(
    materialized = 'view',
    unique_key = ['city', 'label', 'lang'],
    tags= ['snowflake_data_sharing'],
    schema = 'snowflake_dp_ingestion',
    )
}}

select *
from {{ source('public_ontology', 'ontology_city_label') }}
