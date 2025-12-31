{% macro drop_raw_tables() %}



{% if execute %}
    {{ log("retrieving list of snapshots", info=True) }}

    {% set query%}
        select UPPER(TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME) as full_name
        from raw.information_schema.tables 
        where upper(table_schema) != 'INFORMATION_SCHEMA'
            and full_name not in (
            'RAW.ADOBE_DATAFEEDS.DATAFEEDS',
            'RAW.MYSQL_PROFILEBACKEND.LANGUAGE_SKILLS_HST',
            'RAW.MYSQL_MISC.ALERTS_HST',
            'RAW.MYSQL_COMPANIES.KUNUNU_REVIEWS_HST',
            'RAW.MYSQL_CONTENT.SUBSCRIPTIONS_HST',
            'RAW.MYSQL_MESSAGES.THREAD_HST',
            'RAW.MYSQL_XAS.AD_AUCTIONS_HST',
            'RAW.MYSQL_XAS.AD_DELIVERIES_HST',
            'RAW.SALESFORCE_ONLYFY.CAMPAIGNMEMBER_HST',
            'RAW.SALESFORCE_ONLYFY.QUOTE_HST',
            'RAW.SALESFORCE_ONLYFY.TASK_HST',
            'RAW.SALESFORCE_ONLYFY.OPPORTUNITY_HST',
            'RAW.SALESFORCE_ONLYFY.CONTACT_HST',
            'RAW.SALESFORCE_ONLYFY.CASE_HST',
            'RAW.SALESFORCE_ONLYFY.OPPORTUNITYLINEITEM_HST',
            'RAW.SALESFORCE_ONLYFY.CONTRACT_HST',
            'RAW.SALESFORCE_ONLYFY.ACCOUNT_HST',
            'RAW.SALESFORCE_ONLYFY.LEAD_HST'
            )
            and full_name like '%_HST'
            order by full_name asc
    {% endset%}
    {% set tables = run_query(query) %}
    {{ log(tables|length ~" tables found", info=True) }}

    
    {% for table in tables.rows -%}

            

            {{ log("dropping table "+ table['FULL_NAME'] , info=True) }}
            {% set update_statement%}
                DROP TABLE IF EXISTS {{table['FULL_NAME']}}
            {% endset%}
            {{ log(update_statement, info=True) }}
            {% set results = run_query(update_statement) %}
            


    {% endfor %}

{% endif %}

{% endmacro %}