-- You can use this file to generate sources yaml for your model area
-- Find more details in docs https://github.com/dbt-labs/dbt-codegen/tree/0.10.0/#generate_source-source
{{ 
    generate_source_yml_for_database(database='raw') 
}}