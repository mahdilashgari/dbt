{#
-- This macro uses the snowflake_utils.clone_database macro to
-- sync the raw and snapshots databases to the raw_test and snapshots_test databases
-- it also grants usage on the test databases to all roles that have usage on the production databases
-- this is useful for testing dbt models in a test environment
-- Ownership of the test databases is granted to the db_create role which is only assigned to the dbt_pipeline and bi_full roles
-- this means that only the airflow dag and bi data engineers can refresh the test databases
#}


{% macro refresh_raw_test() %}

    {{ log("Cloning existing database raw into database raw_test", info=True) }}
    {% do run_query("CREATE OR REPLACE DATABASE raw_test CLONE raw;")  %}

    {% do run_query("grant ownership on database raw_test to role db_create;")  %}

    {% do run_query("show grants on database raw;")  %}
    {%- set roles = dbt_utils.get_query_results_as_dict('select distinct "grantee_name" from table(result_scan(last_query_id()));') -%}

    {% for role in roles['grantee_name'] -%}
        {% do run_query("grant usage on database raw_test to \"" ~ role ~"\"")  %}
        {{ log("granted usage on raw_test to " ~ role, info=True) }}
    {% endfor %}

    {{ log("Cloning existing database snapshots into database snapshots_test", info=True) }}
    {% do run_query("CREATE OR REPLACE DATABASE snapshots_test CLONE snapshots;")  %}

    {% do run_query("grant ownership on database snapshots_test to role db_create;")  %}

    {% do run_query("show grants on database snapshots;")  %}
    {%- set roles = dbt_utils.get_query_results_as_dict('select distinct "grantee_name" from table(result_scan(last_query_id())) where "granted_to" !=\'SHARE\';') -%}

    {% for role in roles['grantee_name'] -%}
        {% do run_query("grant usage on database snapshots_test to \"" ~ role ~"\"")  %}
        {{ log("granted usage on snapshots_test to " ~ role, info=True) }}
    {% endfor %}

{% endmacro %}