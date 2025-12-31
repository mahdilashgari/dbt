with snapshot_tag_references as (

    select
        object_database,
        object_schema,
        object_name,
        tag_name,
        tag_value
    from {{ source('snowflake_account_usage', 'tag_references') }}
    where true
    and tag_name = 'PRODUCTION_STATUS'
    and object_database in ('RAW')
    and domain in ('TABLE','VIEW')
    and object_deleted is null
)

select distinct
    upper(object_database || '.' || object_schema || '.' || object_name) as table_name,
    tag_name,
    tag_value as production_status
from snapshot_tag_references