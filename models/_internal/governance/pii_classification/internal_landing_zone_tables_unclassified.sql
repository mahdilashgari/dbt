with landing_zone_columns as (

    select
        table_catalog,
        table_schema,
        table_name,
        column_name
    from {{ ref('internal_landing_zone_columns') }}


),

all_classifications as (

    select
        table_catalog,
        table_schema,
        table_name,
        column_name,
        classification_at
    from {{ ref('internal_landing_zone_pii_classifications') }}

),

tables_containing_unclassified_columns as (

    select distinct 
        meta.table_catalog, 
        meta.table_schema, 
        meta.table_name
    from landing_zone_columns meta
    left join all_classifications clf
    on upper(meta.table_catalog) = upper(clf.table_catalog)
    and upper(meta.table_schema) = upper(clf.table_schema)
    and upper(meta.table_name) = upper(clf.table_name)
    and upper(meta.column_name) = upper(clf.column_name)
    where true
    and clf.classification_at is null

)

select * from tables_containing_unclassified_columns
order by table_catalog, table_schema,table_name