{% macro generate_alias_name(custom_alias_name=none, node=none) -%}

    {# List of prefixes to remove from the physical relation name #}
    {% set prefix_list = [
            'onlyfy_',
            'central_',
            'xing_',
            'xms_',
            'raw_',
            'internal_'
        ]
    %}

    {%- if custom_alias_name -%}
        {{ custom_alias_name | trim }}
    {%- elif node.config.database in ['snapshots','snapshots_test'] -%}
        {{ node.name }}
    {%- else -%}
        {%- set item_name = namespace(value='') -%}

        {%- if custom_alias_name -%}
            {%- set item_name.value = custom_alias_name -%}
        {%- elif node.version -%}
            {%- set item_name.value = node.name ~ "_v" ~ (node.version | replace(".", "_")) -%}
        {% else %}
            {%- set item_name.value = node.name -%}
        {%- endif %}

        {#
         # Determine the folder structure of the model by splitting the node path.
         # If the model is in 'staging' or 'intermediate' folders,
         #   if target is prod, extract the last part of the item name splitted by '__'. e.g. stg_user__accounts becomes accounts
         #   if target is not prod, use full node name to avoid conflicts between domains and levels. e.g stg_customer_relationship__accounts, stg_user__accounts or int_user__accounts
         # Otherwise, remove specific prefixes from the item name if they exist.
         # This ensures that the alias name is correctly formatted based on its location and naming conventions.
         #}
        {% set model_folder = node.path.split('/') %}
        {%- if model_folder[0] in ['staging', 'intermediate'] -%}
            {%- if target.name == 'prod' -%}
                {%- set name_parts = item_name.value.split('__') -%}
                {%- set item_name.value = name_parts[name_parts|length-1] -%}
            {%- else -%}
                {%- set item_name.value = node.name -%}
            {%- endif -%}
        {% else %}
            {% for prefix in prefix_list %}
                {% if item_name.value.startswith(prefix) %}
                    {%- set item_name.value = item_name.value[prefix|length:] -%}
                {% endif %}
            {% endfor %}
        {%- endif -%}
        {{ item_name.value }}
    {%- endif -%}
{%- endmacro %}
