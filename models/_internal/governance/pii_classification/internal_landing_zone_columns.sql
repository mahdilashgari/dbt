with landing_zone_columns as (

    select
        table_catalog,
        table_schema,
        table_name,
        column_name,
        data_type,
        ordinal_position
    from {{ source('landing_zone_information_schema', 'columns') }}

),

landing_zone_tables as (

    select
        table_catalog,
        table_schema,
        table_name
    from {{ source('landing_zone_information_schema', 'tables') }}
    where true
    and table_schema != 'INFORMATION_SCHEMA'
    and table_type != 'VIEW'
    and not startswith(lower(table_name),'dl_') 
    and not startswith(lower(table_name),'tmp_')
    and not startswith(lower(table_name),'bck_') 
    and not startswith(lower(table_name),'bak_')

)

select
    cols.table_catalog,
    cols.table_schema,
    cols.table_name,
    cols.column_name,
    cols.data_type 
from landing_zone_columns cols
join landing_zone_tables tabs
on cols.table_catalog = tabs.table_catalog
and cols.table_schema = tabs.table_schema
and cols.table_name = tabs.table_name
order by table_catalog, table_schema, table_name, ordinal_position