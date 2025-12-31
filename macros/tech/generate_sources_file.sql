{#
-- This macro uses the codegen.generate_source macro to
-- generate a sources file for a specific source system
#}


{%- 
    macro generate_sources_file(
        source_system,
        database_name='landing_zone',
        exclude='',
        table_names=None
    )
-%}
    -- This needs to be specified for every source due to lack of general config: https://github.com/dbt-labs/dbt-core/issues/3662
    {%- set database_string = "database: landing_zone" -%}

    {%- set yaml_output = codegen.generate_source(
        schema_name=source_system, 
        database_name=database_name, 
        generate_columns=True,
        exclude='', 
        name=source_system, 
        table_names=None)
    -%}



    {% set replaced = yaml_output | replace('database: landing_zone',database_string) %}
    {{ log(replaced, info=True) }}
    {% do return(replaced) %}


{%- endmacro -%}