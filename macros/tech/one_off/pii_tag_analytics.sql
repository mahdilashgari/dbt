{% macro one_time_tag() %}

  {% if execute %}

    {% for model in graph.nodes.values() | selectattr('tags', "ne", []) | selectattr('database', "equalto", 'analytics') | selectattr('resource_type', "equalto", 'model') -%}
        {% if 'pii' in model.tags %}

            {%- set database = model.database -%}
            {%- set schema = model.schema -%}
            {%- set alias = model.alias -%}
            {%- set full_name = database + '.' + schema + '.' + alias -%}

            {{ log("========== Processing tags for " + full_name + " ==========", info=True) }}

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

                {% for target_tag,target_value in column_data.meta.items() %}
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
                        {% if current_tag[3] | lower not in column_data.meta %}
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
