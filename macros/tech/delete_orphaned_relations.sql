/*
    This macro is triggered on dbt Cloud environment via the following job:
        Job Name: Update Production State
        Job URL : https://emea.dbt.com/deploy/74/projects/309/jobs/18987

    arguments:
     - dry_run:
        when True, performs a dry run which means the drop commands are not executed. useful for testin purposes.
        when False, executes drop commands
        default value is True, set to False in dbt job
     - block_high_number_deletion:
        there is a logic in the macro; if there are more than 50 entities to be dropped, an alert is raised and entities are not dropped.
        this is introduced as a safety measure during the first implementation.
        when this alert is raised, table list is inserted into table and should be manually checked.
        if it is ok to delete the entities, macro should be called with block_high_number_deletion parameter set to False.
        with this, relevant IF clause will work in a way to skip the alert and drop the entities.
        -> dbt run-operation delete_orphaned_relations --args "{dry_run: False, block_high_number_deletion: False}"
        following job can be executed in such cases:
            Job Name: Delete Orphaned Relations
            Job URL : https://emea.dbt.com/deploy/74/projects/309/jobs/120986/settings

    default execution script in "Update Production State" job:
    -> dbt run-operation delete_orphaned_relations --args "{dry_run: False, block_high_number_deletion: True}"
*/

/*
--Creation script for the log table
--This table is created manually, since it is not part of the dbt project
create or replace transient table INTERNAL.GOVERNANCE.DELETED_ORPHAN_RELATIONS (
	DATABASE_NAME VARCHAR(16777216),
	SCHEMA_NAME VARCHAR(16777216),
	RELATION_NAME VARCHAR(16777216),
	RELATION_TYPE VARCHAR(16777216),
	IS_DRY_RUN BOOLEAN,
	EXECUTED_AT_UTC TIMESTAMP_NTZ(9),
	DETAIL VARCHAR(16777216)
)
COMMENT='Source table with list of relations that were deleted in dbt but still exist in the database'
;
*/

{% macro delete_orphaned_relations(dry_run=True, block_high_number_deletion=True) %}

{% if execute %}

    {% do log("##### dry_run: " ~ dry_run ~ " #####", True) %}
    {% do log("Searching for orphaned tables/views...", True) %}
    {% do log("Using target profile: " ~ target.name ~ " (database: " ~ target.database ~ ").", True) %}

    {# list of folders to check models #}
    {% set model_folders_to_check = [
            'central',
            'onlyfy',
            'xing',
            'xms',

            'staging',
            'intermediate',

            'sales',

            'sales_kununu'
        ]
    %}

    {#
        list of schemas to check in analytics database.
        not all schemas are listed here, only the ones that are subject to be checked for orphaned relations.
    #}
    {% set analytics_schemas_to_check = [
            'CENTRAL',
            'CENTRAL_STAGING',
            'ONLYFY',
            'ONLYFY_STAGING',
            'XING',
            'XING_STAGING',
            'XMS',
            'XMS_STAGING',

            'FINANCE',

            'HUMAN_RESOURCES',

            'REP_SALES',
            'MART_SALES',
            'STG_SALES',

            'REP_FINANCE',
            'MART_FINANCE',
            'STG_FINANCE',

            'DBT'
        ]
    %}

    {# Get all databases #}
    {% set models = [] %}
    {% set databases = {} %}
    {%- for node in (graph.nodes.values() | selectattr("resource_type", "equalto", "model") | list
                    + graph.nodes.values() | selectattr("resource_type", "equalto", "seed")  | list
                    + graph.sources.values() | selectattr("resource_type", "equalto", "source") | list) %}

            {% set model_folder = node.path.split('/') %}

            {#
                Exclude ephemeral relations from 'models' dataset.
                With this, when a model is converted from table/view to ephemeral, old physical entity (table or view) will be deleted.
            #}
            {%- if node.config.materialized != 'ephemeral' and model_folder[0] in model_folders_to_check -%}

                {%- set database = node.database -%}
                {%- set schema = node.schema -%}
                {%- if node.resource_type == 'source' -%}
                    {%- set alias = node.name -%}
                {%- else -%}
                    {%- set alias = node.alias -%}
                {%- endif -%}

                {%- set relation = {'database': database, 'schema': schema, 'alias': alias} -%}

                {%- do models.append(relation) -%}

                {%- if database in databases %}
                    {% if schema not in databases[database] %}
                        {% do databases.update({database: databases[database] + [schema]}) %}
                    {% endif %}
                {%- else %}
                    {# Add database and schema because neither exist #}
                    {{ dbt_utils.log_info('Adding database ' ~ database ~ ' to dict') }}
                    {% do databases.update({database: [schema]}) %}
                {%- endif -%}
            {%- endif -%}

    {%- endfor %}

    {% do log("", True) %}


    {% set existing_relations_table = 'internal.governance.snowflake_existing_relations' %}
    {% set desired_relations_table = 'internal.governance.dbt_desired_relations' %}
    {% set deleted_orphan_relations_table = 'internal.governance.deleted_orphan_relations' %}

    {# temp tables are created for performance reasons #}
    {%- do log('Creating temp tables..', True) -%}

    {% set query %}
        create or replace transient table {{ existing_relations_table }} as
            {% for key, value in databases.items() %}
                select
                    upper(table_catalog)   as database_name,
                    upper(table_schema)    as schema_name,
                    upper(table_name)      as relation_name,
                    case
                        when table_type = 'VIEW' then 'view'
                        when table_type = 'BASE TABLE' then 'table'
                        else null
                    end as relation_type
                from
                    {{ key }}.information_schema.tables
                where
                    true
                    and upper(table_schema) <> 'INFORMATION_SCHEMA'
                    and upper(table_name) <> 'DIM_TECH_DBT_DELETED_ORPHAN_RELATIONS__DBT_TMP'
                    and
                    case
                        when upper('{{ key }}') <> 'ANALYTICS' then true
                        else upper(table_schema) in ( {%- for sch in analytics_schemas_to_check -%} '{{ sch }}' {%- if not loop.last %}, {% endif %} {%- endfor -%} )
                    end
                {% if not loop.last -%} union all {% endif -%}
            {%- endfor %}
        ;

        create or replace transient table {{ desired_relations_table }} as
            {%- for node in models -%}
                {%- set db = "upper('" ~ node.database ~ "')" -%}
                {%- set sch = "upper('" ~ node.schema ~ "')" -%}
                {%- set rel = "upper('" ~ node.alias ~ "')" %}
                select
                    {{ db.ljust(49) }}  as database_name,
                    {{ sch.ljust(50) }} as schema_name,
                    {{ rel.ljust(50) }} as relation_name
                {% if not loop.last -%} union all {%- endif %}
            {%- endfor %}
        ;

        select
            c.database_name as database_name,
            c.schema_name   as schema_name,
            c.relation_name as relation_name,
            c.relation_type as relation_type,
            {{ dry_run }}   as is_dry_run
        from
            {{ existing_relations_table }} as c
            left join {{ desired_relations_table }} as desired on c.database_name = desired.database_name and c.schema_name = desired.schema_name and c.relation_name = desired.relation_name
        where
            desired.relation_name is null
        order by
            c.database_name,
            c.schema_name,
            c.relation_name
    {% endset %}


    {%- do log('To be dropped tables are identified..\n', True) -%}

    {%- set result = run_query(query) -%}

    {% if result %}

        {# count the rows in result set, something greater than 10 might be worth a manual check #}
        {% set ns = namespace(number_of_rows_in_result=0) %}
        {%- for to_delete in result -%}
            {%- do log('To be dropped: ' ~ to_delete[3] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1] ~ '.' ~ to_delete[2], True) -%}
            {% set ns.number_of_rows_in_result = loop.index %}
        {%- endfor -%}

        {% do log('Number of relations to be dropped: ' ~ ns.number_of_rows_in_result ~ '\n', True) %}

        {% if ns.number_of_rows_in_result > 50 and block_high_number_deletion %}

            {% do log('Number of relations to be dropped is too high, please make a manual check!' ~ '\n', True) %}
            {%- for to_delete in result -%}
                {% set insert_log_command = 'insert into ' ~ deleted_orphan_relations_table
                                                ~ ' select '
                                                ~ '\'' ~ to_delete[0] ~ '\' as database_name, '
                                                ~ '\'' ~ to_delete[1] ~ '\' as schema_name, '
                                                ~ '\'' ~ to_delete[2] ~ '\' as relation_name, '
                                                ~ '\'' ~ to_delete[3] ~ '\' as relation_type, '
                                                ~ '\'' ~ to_delete[4] ~ '\' as is_dry_run, '
                                                ~ 'sysdate() as executed_at_utc, '
                                                ~ '\'warning: number of relations to be dropped is too high, please make a manual check! relation is not dropped.\' as detail'
                %}
                {% do run_query(insert_log_command) %}
            {%- endfor -%}

        {% else %}
            {%- for to_delete in result -%}
                {% set drop_command = 'drop ' ~ to_delete[3] ~ ' if exists ' ~ to_delete[0] ~ '.' ~ to_delete[1] ~ '.' ~ to_delete[2] ~ ' cascade;' %}
                {% set insert_log_command = 'insert into ' ~ deleted_orphan_relations_table
                                                ~ ' select '
                                                ~ '\'' ~ to_delete[0] ~ '\' as database_name, '
                                                ~ '\'' ~ to_delete[1] ~ '\' as schema_name, '
                                                ~ '\'' ~ to_delete[2] ~ '\' as relation_name, '
                                                ~ '\'' ~ to_delete[3] ~ '\' as relation_type, '
                                                ~ '\'' ~ to_delete[4] ~ '\' as is_dry_run, '
                                                ~ 'sysdate() as executed_at_utc, '
                                                ~ 'iff(' ~ to_delete[4] ~ ', \'relation is not dropped since it is a dry run\', \'relation is dropped\') as detail'
                %}

                {%- if not dry_run -%}
                    {% set drop_result = run_query(drop_command) %}
                    {% if drop_result %}
                        {%- do log('Dropped ' ~ to_delete[3] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1] ~ '.' ~ to_delete[2], True) -%}
                        {% do run_query(insert_log_command) %}
                    {% else %}
                        {% do log('drop command failed..', True) %}
                    {%- endif -%}
                {% else %}
                    {%- do log(to_delete[3] ~ ' ' ~ to_delete[0] ~ '.' ~ to_delete[1] ~ '.' ~ to_delete[2] ~ ' is not dropped since it is a dry run.', True) -%}
                {% do run_query(insert_log_command) %}
                {%- endif -%}
            {%- endfor -%}

        {%- endif -%}

    {% else %}
        {% do log('No orphan tables to clean.\n', True) %}

    {% endif %}

    {# drop temp tables #}
    {% set drop_temp_tables_query %}
    drop table {{ existing_relations_table }};
    drop table {{ desired_relations_table }};
    {% endset %}
    {%- set drop_temp_tables_result = run_query(drop_temp_tables_query) -%}
    {% if drop_temp_tables_result %}
        {%- do log('Dropped temp tables', True) -%}
    {% else %}
        {% do log('Drop command failed for temp tables..', True) %}
    {%- endif -%}

{% endif %}

{% endmacro %}
