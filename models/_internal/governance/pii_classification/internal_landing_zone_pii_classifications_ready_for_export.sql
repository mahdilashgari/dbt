with classifications_already_written_to_yml as (

    select
        table_catalog,
        table_schema,
        table_name,
        column_name
    from {{ source('governance', 'landing_zone_pii_classifications_exported_to_source') }}

),

all_classifications as (

    select
        table_catalog,
        table_schema,
        table_name,
        column_name,
        privacy_category,
        semantic_category,
        classification_at
    from {{ ref('internal_landing_zone_pii_classifications') }}

)

select 
    *
from all_classifications clf
where not exists (
                    select 1 
                    from classifications_already_written_to_yml log
                    where clf.table_catalog = log.table_catalog 
                      and clf.table_schema = log.table_schema
                      and clf.table_name = log.table_name
                      and clf.column_name = log.column_name
                 )