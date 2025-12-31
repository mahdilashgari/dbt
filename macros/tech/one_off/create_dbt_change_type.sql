{% macro create_dbt_change_type() %}



{% if execute %}
    {{ log("retrieving list of snapshots", info=True) }}

    {% set query%}
        select UPPER(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME) as full_name 
        from snapshots.information_schema.tables 
        where upper(table_schema) != 'INFORMATION_SCHEMA'
        and full_name not in (
            'SNAPSHOTS.ADOBE_DATAFEEDS.ADOBE_DATAFEEDS__DATAFEEDS_SNAPSHOT',
            'SNAPSHOTS.ADOBE_DATAFEEDS.RESTORED_ADOBE_DATAFEEDS__DATAFEEDS_SNAPSHOT',
            'SNAPSHOTS.MYSQL_PROFILEBACKEND.MYSQL_PROFILEBACKEND__LANGUAGE_SKILLS_SNAPSHOT',
            'SNAPSHOTS.MYSQL_MISC.MYSQL_MISC__ALERTS_SNAPSHOT',
            'SNAPSHOTS.MYSQL_COMPANIES.MYSQL_COMPANIES__KUNUNU_REVIEWS_SNAPSHOT',
            'SNAPSHOTS.MYSQL_CONTENT.MYSQL_CONTENT__SUBSCRIPTIONS_SNAPSHOT',
            'SNAPSHOTS.MYSQL_XAS.MYSQL_XAS__ROLES_DEV_SNAPSHOT')
        and full_name in (

'SNAPSHOTS.MYSQL_XAS.MYSQL_XAS__AD_AUCTIONS_SNAPSHOT',
'SNAPSHOTS.MYSQL_MESSAGES.MYSQL_MESSAGES__THREAD_SNAPSHOT',
'SNAPSHOTS.MYSQL_XAS.MYSQL_XAS__AD_DELIVERIES_SNAPSHOT')
            order by row_count asc
    {% endset%}
    {% set tables = run_query(query) %}
    {{ log(tables|length ~" tables found", info=True) }}
    
    {% do run_query("use warehouse BI_DBT_WH_XXXLARGE")  %}
    
    {% for table in tables.rows -%}

            {{ log("adding dbt_change_type to "+ table['FULL_NAME'] , info=True) }}
            {% set update_statement%}
                alter table {{table['FULL_NAME']}} add column "DBT_CHANGE_TYPE" character varying(6)
            {% endset%}
            {{ log(update_statement, info=True) }}
            {% set results = run_query(update_statement) %}
            

            {{ log("populating dbt_change_type for "+ table['FULL_NAME'] , info=True) }}
            {% set update_statement%}
                update {{table['FULL_NAME']}} tgt
                        set dbt_change_type = case
                            when dbt_valid_to is null then 'insert'
                            when dbt_valid_to is not null and not exists
                            (
                                select 1 from {{table['FULL_NAME']}} snap
                                where snap.dbt_unique_sk = tgt.dbt_unique_sk and snap.dbt_valid_from >= tgt.dbt_valid_to
                            ) then 'delete'
                            else 'update'
                            end
            {% endset%}
            {{ log(update_statement, info=True) }}
            {% set results = run_query(update_statement) %}
            


    {% endfor %}

{% endif %}

{% endmacro %}