{% macro table_exists(database, schema, table) %}
    {% set query %}
        select count(*) from {{ database }}.information_schema.tables 
        where table_schema = '{{ schema | upper }}'
        and table_name = '{{ table | upper }}'
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set count = results.columns[0].values()[0] %}
        {{ return(count > 0) }}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}


{% macro apply_source_meta_as_tags(results) %}

  {% if execute %}
    {{ log("apply_source_meta_as_tags run from on_run_end", info=True) }}

    {% for res in results -%}

        
        
        --check snapshots tags
        {% if res.node.resource_type == 'snapshot' %}
            
            {%- set source_id = res.node.depends_on.nodes[0] -%}
            {%- set database = res.node.database -%}
            {%- set schema = res.node.schema -%}
            {%- set alias = res.node.alias -%}
            {%- set full_name = database + '.' + schema + '.' + alias -%}


            {% if not ('kafka_models' in res.node.tags or 'snowflake_data_sharing' in res.node.tags) %}
                {% set source_metadata = graph.sources[source_id] %}
            {% else %}
            -- these snapshots use ref instead of source, so we need to get source from ref model
                {% set first_dep_node = graph.nodes[source_id] %}
                {% set second_level_dep_id = first_dep_node.depends_on.nodes[0] %}
                {% set source_metadata = graph.sources[second_level_dep_id] %}
            {% endif %}
            {{ log("========== Processing tags for " + full_name + " ==========", info=True) }}
            {{ log("========== checking snapshot table ==========", info=True) }}


            {% set query%}
                select 
                    LEVEL,
                    UPPER(OBJECT_DATABASE || '.' || OBJECT_SCHEMA || '.' || OBJECT_NAME) as TABLE_NAME,
                    COLUMN_NAME,
                    UPPER(TAG_NAME) as TAG_NAME,
                    TAG_VALUE 
                from table({{database}}.information_schema.tag_references_all_columns('{{full_name}}', 'table'))
            {% endset%}
            {% set existing_tags = run_query(query) %}

        --check raw tags
        {% elif res.node.database in ('raw','raw_test', 'landing_zone') and res.node.resource_type == 'model' %}

            {%- set source_id = res.node.depends_on.nodes[0] -%}
            {%- set database = res.node.database -%}
            {%- set schema = res.node.schema -%}
            {%- set alias = res.node.alias -%}
            {%- set full_name = database + '.' + schema + '.' + alias -%}
            {{ log("========== Processing tags for " + full_name + " ==========", info=True) }}
            
            {%- set alias_snapshot = res.node.alias[:-4] if res.node.alias.endswith('_hst') else res.node.alias -%}
            {%- set alias_snapshot_table_name = schema + '__' + alias_snapshot + '_snapshot' -%}
            {%- set full_name_snapshots = 'snapshots.' + schema + '.' + alias_snapshot_table_name -%}

            {% set table_exists_flag = table_exists('snapshots', schema,alias_snapshot_table_name ) %}


            {% if table_exists_flag %}
                {{ log("========== checking raw view ==========", info=True) }}
                {% set query%}
                    select 
                        LEVEL,
                        UPPER(OBJECT_DATABASE || '.' || OBJECT_SCHEMA || '.' || OBJECT_NAME) as TABLE_NAME,
                        COLUMN_NAME,
                        UPPER(TAG_NAME) as TAG_NAME,
                        TAG_VALUE 
                    from table({{database}}.information_schema.tag_references_all_columns('{{full_name_snapshots}}', 'table'))
                {% endset%}
                {% set existing_tags = run_query(query) %}

                {% set first_dep_node = graph.nodes[source_id] %}
                {% set second_level_dep_id = first_dep_node.depends_on.nodes[0] %}

                -- raw model may use snapshots which refer to a base model instead of a source
                {% if second_level_dep_id not in graph.sources.keys()%}
                    {% set source_metadata = graph.nodes[second_level_dep_id] %}
                {% else %}
                    {% set source_metadata = graph.sources[second_level_dep_id] %}
                {% endif %}
            {% else %}
                {{ log("========== snapshots table does not exist for the raw model ==========", info=True) }}
            {% endif %}

        {% endif %}
        --apply tags to either raw or snapshots or landing zone models
        {% if res.node.resource_type == 'snapshot' or (res.node.database in ('raw','raw_test', 'landing_zone') and res.node.resource_type == 'model' and table_exists_flag == true) %}

            {{ log("========== Tagging raw view or snapshots table ==========", info=True) }}
            {% for column in source_metadata.columns %}
                {{ log("Setting tags for " + column, info=True) }}
                {% for column_tag in source_metadata.columns[column].config.meta %}

                    {% set desired_tag_value = source_metadata.columns[column].config.meta[column_tag] %}
                    {{ log('Ensuring tag '+column_tag+' has value '+desired_tag_value+' at column level', info=True) }}
                    {% set current_tag_value = existing_tags|selectattr('0','equalto','COLUMN')|selectattr('1','equalto',full_name|upper)|selectattr('2','equalto',column|upper)|selectattr('3','equalto',column_tag|upper)|list -%}
                    {% if current_tag_value|length > 0 and current_tag_value[0][4]==desired_tag_value %}
                        {{ log('Correct tag value already exists', info=True) }}
                    {% else %}
                        {{ set_column_tag(
                            table_name=full_name | upper,
                            column_name=column | upper,
                            tag_name=column_tag | upper,
                            desired_tag_value=desired_tag_value) }}
                    {% endif %}
                {% endfor %}
                {% set current_column_tags = existing_tags|selectattr('0','equalto','COLUMN')|selectattr('1','equalto',full_name|upper)|selectattr('2','equalto',column|upper)|list -%}

                {% if current_column_tags|length > 0%}
                    {% for current_tag in current_column_tags %}
                        {% if current_tag[3] | lower not in source_metadata.columns[column].config.meta %}
                            {{ log(current_tag[3] + ' is set for column ' + column + ' but not present in its meta', info=True) }}
                            {{ unset_column_tag(
                                table_name=full_name | upper,
                                column_name=column | upper,
                                tag_name=current_tag[3] | upper
                            ) }}
                    {% endif %}
                    {% endfor %}
                {% endif %}
            {% endfor %}
            {{ log("========== Finished processing tags for " + full_name + " ==========", info=True) }}
        {% endif %}
    {% endfor %}
  {% endif %}
  -- Need to return something other than None, since DBT will try to execute it as SQL statement
  {{ return('') }}
{% endmacro %}


{% macro apply_model_meta_as_tags(results) %}
{# 
-- Applies pii tags to columns in analytics models
-- could get refactored and merged with the apply_source_meta_as_tags macro
#}

  {% if execute %}

    {% for res in results  -%}

        {% if res.node.database == 'analytics' and res.node.resource_type == 'model' %}

            {%- set model = res.node -%}
            {%- set database = model.database -%}
            {%- set schema = model.schema -%}
            {%- set alias = model.alias -%}
            {%- set full_name = database + '.' + schema + '.' + alias -%}

            {{ log("========== Processing tags for " + full_name + " ==========", info=True) }}
            {{ log("========== If you receive an error here, please visit: ==========", info=True) }}
            {{ log("========== https://emea.dbt.com/explore/74/projects/309/details/macro.new_work_se_analytics.apply_model_meta_as_tags ==========", info=True) }}
            
            {% set query%}
                select 
                    LEVEL,
                    UPPER(OBJECT_DATABASE || '.' || OBJECT_SCHEMA || '.' || OBJECT_NAME) as TABLE_NAME,
                    COLUMN_NAME,
                    UPPER(TAG_NAME) as TAG_NAME,
                    TAG_VALUE 
                from table({{database}}.information_schema.tag_references_all_columns('{{full_name}}', 'table'))
            {% endset%}

            {% set existing_tags = run_query(query) %}


            {% for column_name, column_data in model.columns.items() %}
                {{ log("Setting tags for " + column_name, info=True) }}

                {# we check if the column is case sensitive or not#}
                {% if column_name is upper or column_name is lower%}
                    {% set column_name = column_name | upper %}
                {% endif %}

                {% for target_tag,target_value in column_data.config.meta.items() %}

                    {{ log('Ensuring tag '+ target_tag +' has value '+ target_value +' at column level', info=True) }}
                    {% set current_tag_value = existing_tags|selectattr('0','equalto','COLUMN')|selectattr('1','equalto',full_name|upper)|selectattr('2','equalto',column_name)|selectattr('3','equalto',target_tag|upper)|list -%}
                    {% if current_tag_value|length > 0 and current_tag_value[0][4]==target_value %}
                        {{ log('Correct tag value already exists', info=True) }}
                    {% else %}
                        {{ set_column_tag(
                            table_name=full_name | upper,
                            column_name=column_name,
                            tag_name=target_tag | upper,
                            desired_tag_value=target_value) }}
                    {% endif %}
                {% endfor %}
                
                {% set current_column_tags = existing_tags|selectattr('0','equalto','COLUMN')|selectattr('1','equalto',full_name|upper)|selectattr('2','equalto',column_name)|list -%}

                {% if current_column_tags|length > 0%}
                    {% for current_tag in current_column_tags %}
                        {% if current_tag[3] | lower not in column_data.config.meta %}
                            {{ log(current_tag[3] + ' is set for column ' + column_name + ' but not present in its meta', info=True) }}
                            {{ unset_column_tag(
                                table_name=full_name | upper,
                                column_name=column_name,
                                tag_name=current_tag[3] | upper
                            ) }}
                    {% endif %}
                    {% endfor %}
                {% endif %}
            {% endfor %}
            {{ log("========== Finished processing tags for " + full_name + " ==========", info=True) }}
        {% endif %}
    {% endfor %}
  {% endif %}

{% endmacro %}


{% macro source_contains_tag_meta(source_id="source.new_work_se_analytics.salesforce_onlyfy.opportunitylineitem") %}

{# 
-- Returns True if the source table contains meta information for any column
-- Otherwise it returns False
#}
    {% set source_data = graph.sources[source_id]%}

    {% if source_data.columns == {} %}
    {# -- No columns description - false #}
        {{ return(False) }}
    {% endif %}

    {% for column in source_data.columns %}
        {% if source_data.columns[column].config.meta != {} %}
            {# -- At least one col contains meta - true #} 
            {{ return(True) }}
    	{% endif %}
    {% endfor %}
    {{ return(False) }}

{% endmacro %}



{% macro set_column_tag(
    table_name,
    column_name,
    tag_name,
    desired_tag_value) %}
        
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=True) }}
        {% set query%}
            alter table {{table_name}} modify column "{{column_name}}" set tag internal.governance.{{tag_name}} = '{{desired_tag_value}}'
        {% endset%}
        {% set result = run_query(query) %}
{% endmacro %}


{% macro unset_column_tag(
    table_name,
    column_name,
    tag_name) %}

        {{ log('Removing tag '+tag_name+' from column '+column_name, info=True) }}
        {% set query%}
            alter table {{table_name}} modify column "{{column_name}}" unset tag internal.governance.{{tag_name}}
        {% endset%}
        {% set result = run_query(query) %}
{% endmacro %}