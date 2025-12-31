with mapped_classification_categories as (

    select
        snowflake_privacy_category,
        snowflake_semantic_category,
        mapped_privacy_category,
        mapped_semantic_category
    from {{ source('governance', 'snowflake_privacy_classification_mapping') }}

),

ranked_classification_results as (

    select 
        upper(table_catalog) as table_catalog,
        upper(table_schema) as table_schema,
        upper(table_name) as table_name,
        upper(column_name) as column_name,
        privacy_category,
        semantic_category,
        classification_at,
        probability,
        rank() over (partition by table_catalog, table_schema,table_name, column_name ORDER BY classification_at DESC) as classification_rank
    from {{ source('governance', 'snowflake_privacy_classification_results') }}

), 

latest_classifications as (

    select
        table_catalog,
        table_schema,
        table_name,
        column_name,
        privacy_category,
        semantic_category,
        probability,
        classification_at
    from ranked_classification_results
    where classification_rank = 1

)

select
    table_catalog,
    table_schema,
    table_name,
    column_name,
    coalesce(mapping.mapped_privacy_category,'NO_PII') as privacy_category,
    coalesce(mapping.mapped_semantic_category, 'NO_CATEGORY') as semantic_category,
    probability,
    classification_at
from latest_classifications latest
left join mapped_classification_categories mapping
on latest.privacy_category = mapping.snowflake_privacy_category 
and latest.semantic_category = mapping.snowflake_semantic_category