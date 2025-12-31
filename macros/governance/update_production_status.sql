{% macro update_production_status(results) %}

  {% if execute %}

    {% for res in results -%}

        {% if res.node.database.lower() == 'raw' or (res.node.database.lower() == 'analytics' and res.node.schema.lower() == 'central') or res.node.database.lower() == 'landing_zone' %}
        
            {%- set database = res.node.database -%}
            {%- set schema = res.node.schema -%}
            {%- set alias = res.node.alias -%}
            {%- set full_name = database + '.' + schema + '.' + alias -%}

            {{ log("========== Processing tags for " + full_name + " ==========", info=True) }}

            {% set query%}
                select 
                    table_name,
                    tag_name,
                    production_status
                from internal.governance.raw_production_status
            {% endset%}

            {% set existing_tags = run_query(query) %}

                {{ log("Setting tags for " + full_name, info=True) }}
                {% for column_tag in res.node.meta %}

                    {% set desired_tag_value = res.node.meta[column_tag] %}
                    {{ log('Ensuring tag '+column_tag+' has value '+desired_tag_value+' at table level', info=True) }}
                        {{ set_table_tag(
                            table_name=full_name | upper,
                            tag_name=column_tag | upper,
                            desired_tag_value=desired_tag_value) }}
                
                {% set current_column_tags = existing_tags|selectattr('0','equalto',full_name|upper)|list -%}

                {% if current_column_tags|length > 0%}
                    {% for current_tag in current_column_tags %}
                        {% if current_tag[1] | lower not in res.node.meta %}
                            {{ log(current_tag[1] + ' is set for table ' + full_name + ' but not present in its meta', info=True) }}
                            {{ unset_table_tag(
                                table_name=full_name | upper,
                                tag_name=current_tag[1] | upper
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

{% macro set_table_tag(
    table_name,
    tag_name,
    desired_tag_value) %}
        {{ log('Setting tag value for '+tag_name+' to value '+desired_tag_value, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{table_name}} set tag internal.governance.{{tag_name}} = '{{desired_tag_value}}'
        {%- endcall -%}
        {{ log(load_result('main').data, info=True) }}
{% endmacro %}


{% macro unset_table_tag(
    table_name,
    tag_name) %}
        {{ log('Removing tag '+tag_name+' from table '+table_name, info=True) }}
        {%- call statement('main', fetch_result=True) -%}
            alter table {{table_name}} unset tag internal.governance.{{tag_name}}
        {%- endcall -%}
        {{ log(load_result('main').data, info=True) }}
{% endmacro %}