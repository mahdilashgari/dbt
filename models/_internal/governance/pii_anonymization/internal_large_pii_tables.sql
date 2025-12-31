with tag_references as (

    select
        object_database,
        object_schema,
        object_name,
        column_name,
        tag_value
    from {{ source('snowflake_account_usage', 'tag_references') }}
    where true
    and tag_name = 'PII_ID'
    and object_database in ('ANALYTICS','SNAPSHOTS')
    and object_schema not like 'TEST_%'

),

large_pii_tables as (

    select distinct
        upper(object_database || '.' || object_schema || '.' || object_name) as table_name,
        column_name as key_column,
        tag_value as pii_id
    from tag_references

),

last_anonymizations as (

    select
        table_name,
        max(anonymization_at) as last_anonymization_at
    from {{ source('governance', 'large_table_anonymizations') }}
    group by all

),

table_sizes as (

    select 
        upper(table_catalog || '.' || table_schema || '.' || table_name) as table_name,
        row_count,
        round(bytes / pow(1024,3),6) as table_size_in_gb
    from {{ source('snowflake_account_usage', 'tables') }}
    where true
    and deleted is null
    and table_catalog in ('SNAPSHOTS', 'ANALYTICS')
    and table_schema not like 'TEST_%'
)

select
    lpt.table_name,
    lpt.key_column,
    lpt.pii_id,
    ts.table_size_in_gb,
    iff(la.last_anonymization_at is null, False, True) as is_anonymized,
    la.last_anonymization_at,
    coalesce(datediff(day, la.last_anonymization_at, sysdate()),999) as days_since_last_anonymization
from large_pii_tables lpt
left join last_anonymizations la
on lpt.table_name = la.table_name
left join table_sizes ts
on lpt.table_name = ts.table_name