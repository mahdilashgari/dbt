{% macro create_dbt_is_current(
    schema_name='ftp_anzeigendaten') %}



{% if execute %}
    {{ log("retrieving list of snapshots", info=True) }}

    {% set query%}
        select 
            UPPER(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME) as full_name,
            table_catalog,
            table_schema,
            table_name
        from snapshots.information_schema.tables
        where true
        and table_schema <> 'INFORMATION_SCHEMA'
        and full_name <> 'SNAPSHOTS.ADOBE_DATAFEEDS.ADOBE_DATAFEEDS__DATAFEEDS_SNAPSHOT'
        and full_name <> 'SNAPSHOTS.ADOBE_DATAFEEDS.RESTORED_ADOBE_DATAFEEDS__DATAFEEDS_SNAPSHOT'
        and full_name <> 'SNAPSHOTS.FTP_NOVOMIND.FTP_NOVOMIND__EMAIL_PROPERTIES_SNAPSHOT_BCKP'
        --and full_name <> 'SNAPSHOTS.MYSQL_CONTENT.MYSQL_CONTENT__PAGE_FOLLOWS_SNAPSHOT'
        and full_name <> 'SNAPSHOTS.GEOGRAPHICAL_DIMENSIONS.GEOGRAPHICAL_DIMENSIONS__COUNTRY_DATA_SNAPSHOT'
        and full_name <> 'SNAPSHOTS.HDFS_ADTECH.HDFS_ADTECH__XAS_DETAILED_BIDS_SNAPSHOT_2'
        --and full_name <> 'SNAPSHOTS.ARCHIVE_EXASOL_OPENBI_CI.ARCHIVE_EXASOL_OPENBI_CI__GMA_SUPERMART_SNAPSHOT'
        and full_name not like '%MYSQL_%'
        and full_name not like '%_BCKP%'
        and full_name not like '%_BCK%'
        and full_name not like 'SNAPSHOTS.SALESFORCE_ONLYFY.SALESFORCE_ONLYFY__ACCOUNTS_SNAPSHOT'
        --and full_name not like 'SNAPSHOTS.BRAZE_XING.BRAZE_XING__BRAZE_CURRENTS_SNAPSHOT'
        order by bytes asc
        --where lower(table_schema) = {{ "'" + schema_name | lower + "'"}}
    {% endset%}
    {% set tables = run_query(query) %}
    {{ log(tables|length ~" tables found", info=True) }}
        
    {% for table in tables.rows -%}

            {% do run_query("use warehouse BI_DBT_WH_XXLARGE")  %}


            {% if "DBT_IS_CURRENT" not in adapter.get_columns_in_relation(ref(table['TABLE_NAME'] | lower)) | upper %}
                {{ log("adding dbt_is_current to "+ table['FULL_NAME'] , info=True) }}
                {% set update_statement%}
                    alter table {{table['FULL_NAME']}} add column "DBT_IS_CURRENT" boolean
                {% endset%}
                {{ log(update_statement, info=True) }}
                {% set results = run_query(update_statement) %}

                {% set update_needed = true %}
            {% else %}
                {{ log("dbt_is_current already exists on "+ table['FULL_NAME'] , info=True) }}
                {{ log("check if dbt_is_current contains null values for "+ table['FULL_NAME'] , info=True) }}

                {% set count = run_query("select count(*) as CNT from " + table['FULL_NAME'] + " where dbt_is_current is null") %}

                {% if count.rows[0]['CNT'] > 0 %}
                    {{ log("dbt_is_current contains null values for "+ table['FULL_NAME'] , info=True) }}
                    {% set update_needed = true %}
                {% else %}
                    {{ log("dbt_is_current does not contain null values for "+ table['FULL_NAME'] , info=True) }}
                    {% set update_needed = false %}
                {% endif %}

            {% endif %}

            {% if update_needed %}
                {{ log("populating dbt_is_current for "+ table['FULL_NAME'] , info=True) }}

                {{ log("check type of" + table['FULL_NAME'] , info=True) }}

                {% set change_types = run_query("select distinct dbt_change_type from " + table['FULL_NAME']) %}

                {% if change_types|length == 1 and change_types.rows[0]['DBT_CHANGE_TYPE'] == 'insert' %}
                    {{ log(table['FULL_NAME'] + " is insert only" , info=True) }}
                    {% set update_statement%}
                        update 
                        {{table['FULL_NAME']}} tgt
                        set dbt_is_current = true
                    {% endset%}
                    {{ log(update_statement, info=True) }}
                    {% set results = run_query(update_statement) %}
                {% else %}
                    {{ log(table['FULL_NAME'] + " has multiple change types" , info=True) }}
                    {% set update_statement%}
                        update 
                        {{table['FULL_NAME']}} tgt
                            set dbt_is_current = case
                                                when dbt_valid_to is null then true
                                                when dbt_valid_to is not null and not exists
                                                (
                                                    select 1 from {{table['FULL_NAME']}} snap
                                                    where snap.dbt_unique_sk = tgt.dbt_unique_sk and snap.dbt_valid_from >= tgt.dbt_valid_to
                                                ) then true
                                                else false
                                                end
                    {% endset%}
                    {{ log(update_statement, info=True) }}
                    {% set results = run_query(update_statement) %}
                {% endif %}       

            {% endif %}

    
    {% endfor %}

{% endif %}

{% endmacro %}