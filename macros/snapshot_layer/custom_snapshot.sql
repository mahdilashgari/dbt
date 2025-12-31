{% materialization snapshot, default %}
  {%- set config = model['config'] -%}

  {%- set target_table = model.get('alias', model.get('name')) -%}

  {%- set strategy_name = config.get('strategy') -%}
  {%- set unique_key = config.get('unique_key') %}
  -- grab current tables grants config for comparision later on
  {%- set grant_config = config.get('grants') -%}

  {% set target_relation_exists, target_relation = get_or_create_relation(
          database=model.database,
          schema=model.schema,
          identifier=target_table,
          type='table') -%}

  {%- if not target_relation.is_table -%}
    {% do exceptions.relation_wrong_type(target_relation, 'table') %}
  {%- endif -%}


  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set strategy_macro = strategy_dispatch(strategy_name) %}
  {% set strategy = strategy_macro(model, "snapshotted_data", "source_data", config, target_relation_exists) %}

  {% if not target_relation_exists %}

      {% set build_sql = build_snapshot_table(strategy, model['compiled_code']) %}
      {% set final_sql = create_table_as(False, target_relation, build_sql) %}

  {% else %}

      {{ adapter.valid_snapshot_target(target_relation) }}

      {% set staging_table = build_snapshot_staging_table(strategy, sql, target_relation) %}

      -- this may no-op if the database does not require column expansion
      {% do adapter.expand_target_column_types(from_relation=staging_table,
                                               to_relation=target_relation) %}

      {% set missing_columns = adapter.get_missing_columns(staging_table, target_relation)
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% do create_columns(target_relation, missing_columns) %}

      {% set source_columns = adapter.get_columns_in_relation(staging_table)
                                   | rejectattr('name', 'equalto', 'dbt_unique_key')
                                   | rejectattr('name', 'equalto', 'DBT_UNIQUE_KEY')
                                   | list %}

      {% set quoted_source_columns = [] %}
      {% for column in source_columns %}
        {% do quoted_source_columns.append(adapter.quote(column.name)) %}
      {% endfor %}

      {% set final_sql = snapshot_merge_sql(
            target = target_relation,
            source = staging_table,
            insert_cols = quoted_source_columns
         )
      %}

  {% endif %}

  {% call statement('main') %}
      {{ final_sql }}
  {% endcall %}

  {% set should_revoke = should_revoke(target_relation_exists, full_refresh_mode=False) %}
  {% do apply_grants(target_relation, grant_config, should_revoke=should_revoke) %}

  {% do persist_docs(target_relation, model) %}

  {% if not target_relation_exists %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {% if staging_table is defined %}
      {% do post_snapshot(staging_table) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro build_snapshot_table(strategy, sql) -%}
    select *,
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from,
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
        'insert' as dbt_change_type,
        true as dbt_is_current
    from (
        {{ sql }}
    ) sbq
{% endmacro %}


{% macro snapshot_merge_sql(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    merge into {{ target }} as DBT_INTERNAL_DEST
    using {{ source }} as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'update'
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to,
            dbt_change_type = DBT_INTERNAL_SOURCE.dbt_change_type,
            dbt_is_current = false
    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'delete'
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to,
            dbt_change_type = DBT_INTERNAL_SOURCE.dbt_change_type
    when matched
      and DBT_INTERNAL_SOURCE.dbt_change_type = 'reinsert'
          then update
          set dbt_is_current = false

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ({{ insert_cols_csv }})
        values ({{ insert_cols_csv }})

{% endmacro %}

{% macro snapshot_staging_table(strategy, source_sql, target_relation) -%}

    with snapshot_query as (

        {{ source_sql }}

    ),

    snapshotted_data as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key

        from {{ target_relation }}
        where dbt_valid_to is null

    ),
    
    {%- if strategy.invalidate_hard_deletes %}

    snapshotted_deletes as (

        select *,
            {{ strategy.unique_key }} as dbt_unique_key
        from {{ target_relation }}
        where dbt_change_type = 'delete'
    ),
    {% endif %}

    insertions_source_data as (

        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id

        from snapshot_query
    ),
    {%- if not strategy.is_event %}
        updates_source_data as (

            select
                *,
                {{ strategy.unique_key }} as dbt_unique_key,
                {{ strategy.updated_at }} as dbt_updated_at,
                {{ strategy.updated_at }} as dbt_valid_from,
                {{ strategy.updated_at }} as dbt_valid_to

            from snapshot_query
        ),

        {%- if strategy.invalidate_hard_deletes %}

            deletes_source_data as (

                select
                    *,
                    {{ strategy.unique_key }} as dbt_unique_key
                from snapshot_query
            ),
        {% endif %}
    {% endif %}
    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*,
            true as dbt_is_current

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
        {%- if not strategy.is_event %}
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                {{ strategy.row_changed }}
            )
        )
        {% endif %}
    )
    {%- if not strategy.is_event %}
        ,

        updates as (

            select
                'update' as dbt_change_type,
                source_data.*,
                snapshotted_data.dbt_scd_id,
                false as dbt_is_current

            from updates_source_data as source_data
            join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
            where (
                {{ strategy.row_changed }}
            )
        )

        {%- if strategy.invalidate_hard_deletes -%}
            ,

            deletes as (

                select
                    'delete' as dbt_change_type,
                    source_data.*,
                    {{ snapshot_get_time() }} as dbt_valid_from,
                    {{ snapshot_get_time() }} as dbt_updated_at,
                    {{ snapshot_get_time() }} as dbt_valid_to,
                    snapshotted_data.dbt_scd_id,
                    true as dbt_is_current

                from snapshotted_data
                left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
                where source_data.dbt_unique_key is null
            ),

            reinsertions as (

                select
                'reinsert' as dbt_change_type,
                insertions.* exclude (dbt_change_type, dbt_scd_id, dbt_is_current),
                snapshotted_deletes.dbt_scd_id,
                false as dbt_is_current          
                from snapshotted_deletes
                join insertions on snapshotted_deletes.dbt_unique_key = insertions.dbt_unique_key

            )
        {%- endif %}
    {%- endif %}

    select * from insertions
    {%- if not strategy.is_event %}
        union all
        select * from updates
        {%- if strategy.invalidate_hard_deletes %}
            union all
            select * from deletes
            union all
            select * from reinsertions
        {%- endif %}
    {%- endif %}

{%- endmacro %}

{# BI-17670: [2024-02-28] this macro acts like a wrapper to make the is_event variable available, to be used later creating the staging table  #}
{% macro snapshot_event_strategy(node, snapshotted_rel, current_rel, config, target_exists) %}
    {% set is_event = True %}
    {% set primary_key = config['unique_key'] %}
    {% set invalidate_hard_deletes = config.get('invalidate_hard_deletes', false) %}
    {% set updated_at = config.get('updated_at', snapshot_get_time()) %}
    {% set scd_id_expr = snapshot_hash_arguments([primary_key, updated_at]) %}

    {% do return({
        "unique_key": primary_key,
        "updated_at": updated_at,
        "row_changed": '',
        "scd_id": scd_id_expr,
        "invalidate_hard_deletes": invalidate_hard_deletes,
        "is_event": is_event
    }) %}
{% endmacro %}