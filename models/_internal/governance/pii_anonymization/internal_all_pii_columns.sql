with tag_references as (

    select
        object_database,
        object_schema,
        object_name,
        column_name,
        tag_value
    from {{ source('snowflake_account_usage', 'tag_references') }}
    where true
    and tag_name = 'PII_PRIVACY_CATEGORY'
    and object_database in ('ANALYTICS','SNAPSHOTS')
    and object_schema not like 'TEST_%'

)

select distinct
    upper(object_database || '.' || object_schema || '.' || object_name) as table_name,
    column_name,
    tag_value as privacy_category
from tag_references