{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {%- if custom_database_name in ['landing_zone'] -%}  {# metadata storage #}

        {{- custom_database_name | trim -}}

    {%- elif target.name == 'prod' and custom_database_name != none -%}  {# production environment customized database #}

        {{- custom_database_name | trim -}}

    {%- else -%} {# production environment default database or development environment #}

        {{- default_database | trim -}}

    {%- endif -%}

{%- endmacro %}