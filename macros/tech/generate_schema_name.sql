{% macro generate_schema_name(custom_schema_name, node) -%}

    {# create TEST_<NAME>_<SURNAME> schema pattern manually #}
    {%- set username = target.user -%} {# ERDINC.ULUTURK@NEW-WORK.SE #}
    {%- set user_name_surname = username.split('@')[0] -%} {# ERDINC.ULUTURK #}
    {%- set name = user_name_surname.split('.')[0] -%} {# ERDINC #}
    {%- set surname = user_name_surname.split('.')[1] -%} {# ULUTURK #}
    {%- set test_schema_name = 'test_'~name~'_'~surname -%} {# ERDINC_ULUTURK #}


    {# set default_schema for production and development environments #}
    {%- if target.name == 'prod' -%}  {# production environment -> dbt #}

        {%- set default_schema = target.schema -%}

    {%- else -%} {# development environment -> test_<name>_<surname> #}

        {%- set default_schema = test_schema_name -%}

    {%- endif -%}

    {# generate schema name #}
    {%- if custom_schema_name in ['dbt_metadata'] -%}  {# metadata storage #}

        {{- custom_schema_name | trim -}}

    {%- elif target.name == 'prod' and custom_schema_name != none -%}  {# production environment customized schema #}

        {{- custom_schema_name | trim -}} {# analytics.<custom_schema_name>.<table_name> #}

    {%- else -%} {# production environment default schema or development environment #}

        {{- default_schema | trim -}} {# analytics.dbt.<table_name> or analytics.test_<name>_<surname>.<table_name> #}

    {%- endif -%}

{%- endmacro %}