{#-
    Macro to generate a SQL query that groups by all possible combinations of a set of columns.
    It's a workaround for the limitation of max 7 columns in the GROUP BY clause in Snowflake.
    https://docs.snowflake.com/en/sql-reference/constructs/group-by-cube

    Limiting to max 9 columns to avoid performance issues.

    USAGE:
        {{ group_by_cube(
            ['date'],                              -- list of columns to group by
            ['col_a','col_b','col_c','col_d'],     -- list of columns to cube, will be converted to varchar and Total value will be added, nulls will be converted to Unknown
            'sum(value) as value',                 -- aggregation separated by comma
            ref('xing_seed_frg_testfile_basic_2')  -- table name
        ) }}
-#}

{% macro group_by_cube(group_columns, cube_columns, aggregation, table_name) %}

    {#- check all arguments  -#}
    {% set missing_args = [] %}
    {% if not group_columns %}
        {% set _ = missing_args.append("group_columns") %}
    {% endif %}
    {% if not cube_columns %} 
        {% set _ = missing_args.append("cube_columns") %}
    {% endif %}
    {% if not aggregation %}
        {% set _ = missing_args.append("aggregation") %}
    {% endif %}
    {% if not table_name %}
        {% set _ = missing_args.append("table_name") %}
    {% endif %}
    {% if missing_args | length > 0 %}
        {% do exceptions.raise_compiler_error( missing_args | join(", ") ~ ' not provided to group_by_cube') %}
    {% endif %}

    {% if not (group_columns is iterable and (group_columns is not string and group_columns is not mapping)) %}
        {% do exceptions.raise_compiler_error("group_columns must be a list") %}
    {% endif %}

    {% if not (cube_columns is iterable and (cube_columns is not string and cube_columns is not mapping)) %}
        {% do exceptions.raise_compiler_error("cube_columns must be a list") %}
    {% endif %}

    {% if not (aggregation is string) %}
        {% do exceptions.raise_compiler_error("aggregation must be a string") %}
    {% endif %}

    {% set cube_limit = 9 %}
    {% if cube_columns | length > cube_limit %}
        {{ exceptions.raise_compiler_error("Too many cube columns, reduce to " ~ cube_limit ~ ". Got: " ~ cube_columns | length) }}
    {% endif %}

    {#- logic starts here -#}

    {% set cube_column_combinations = [] %}

    {# --- REPLACEMENT FOR itertools.combinations --- #}
    {% set n = cube_columns | length %}
    {# generate all non-empty subsets via bit masks 1..(2^n - 1) #}
    {% for mask in range(1, 2 ** n) %}
        {% set combo = [] %}
        {% for idx in range(0, n) %}
            {% if ((mask // (2 ** idx)) % 2) == 1 %}
                {% set _ = combo.append(cube_columns[idx]) %}
            {% endif %}
        {% endfor %}
        {% do cube_column_combinations.append(combo) %}
    {% endfor %}
    {# ---------------------------------------------- #}

    {% set queries = [] %}

    {% for combination in cube_column_combinations %}

        {% set select_clauses = [] %}
        {% set group_clauses = [] %}

        {% for col in cube_columns %}
            {% if col in combination %}
                {% do select_clauses.append('ifnull(' ~ col ~ ', \'Unknown\')' ~ '::varchar as ' ~ col ) %}
                {% do group_clauses.append(col) %}
            {% else %}
                {% do select_clauses.append('\'Total\' as ' ~ col) %}
            {% endif %}
        {% endfor %}
        
        {% set query %}
            SELECT
                {{ group_columns | join(",") }},
                {{ select_clauses | join(", ") }},
                {{ aggregation }}
            FROM {{ table_name }}
            GROUP BY
                {{ group_columns | join(", ") }},
                {{ group_clauses | join(", ") }}
        {% endset %}
        {% do queries.append(query) %}
    {% endfor %}
    {% set query %}
        SELECT
            {{ group_columns | join(",") }},
            {%- for col in cube_columns -%}
              {{ '\'Total\' as ' ~ col }},
            {%- endfor -%}
            {{ aggregation }}
        FROM {{ table_name }}
        GROUP BY
            {{ group_columns | join(", ") }}
    {% endset %}
    {% do queries.append(query) %}
    {% set union_query = queries | join(" union all ") %}

    {{ return(union_query) }}
{% endmacro %}
