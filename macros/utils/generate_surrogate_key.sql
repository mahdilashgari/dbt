{%- macro generate_surrogate_key(field_list,quote_identifiers=False, uppercase=False) -%}

{%- if var('surrogate_key_treat_nulls_as_empty_strings', False) -%}
    {%- set default_null_value = "" -%}
{%- else -%}
    {%- set default_null_value = '_dbt_utils_surrogate_key_null_' -%}
{%- endif -%}

{%- set fields = [] -%}

{%- for field in field_list -%}
    {%- set col = field | upper if uppercase else field -%}

    {%- if quote_identifiers-%}
    {%- do fields.append(
        "coalesce(cast(\"" ~ col ~ "\" as " ~ dbt.type_string() ~ "), '" ~ default_null_value  ~"')"
    ) -%}
    {%- else -%}
    {%- do fields.append(
        "coalesce(cast(" ~ col ~ " as " ~ dbt.type_string() ~ "), '" ~ default_null_value  ~"')"
    ) -%}
    {%- endif -%}

    {%- if not loop.last %}
        {%- do fields.append("'-'") -%}
    {%- endif -%}

{%- endfor -%}

{{ dbt.hash(dbt.concat(fields)) }}

{%- endmacro -%}