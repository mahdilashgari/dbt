{#
-- This macro is based the codegen.generate_model_yaml macro
-- it generates a list of required models for a source system
#}

{%- 
    macro generate_models_file(
        source_system,
        database_name='landing_zone'
    ) 
-%}
    {% set tables=codegen.get_tables_in_schema(source_system, database_name) %}
    {% set model_yaml=[] %}

    {% do model_yaml.append('version: 2') %}
    {% do model_yaml.append('') %}
    {% do model_yaml.append('models:') %}

    {% for table in tables %}
        {% do model_yaml.append('  - name: ' ~ table | lower) %}
        {% do model_yaml.append('    description: ""') %}
        {% do model_yaml.append('  - name: ' ~ table | lower ~ '_hst') %}
        {% do model_yaml.append('    description: ""') %}
    {% endfor %}


        {% set joined = model_yaml | join ('\n') %}
        {{ log(joined, info=True) }}
        {% do return(joined) %}


{%- endmacro -%}