{{
  config(
    materialized = 'incremental',
    unique_key = 'user_id',
    tags = ['static'],
    schema = 'kafka_dp_ingestion'
  )
}}

with source_data as (

    select
        *,
        record_metadata:topic::string                                           as metadata_kafka_topic,
        record_metadata:partition::string                                       as metadata_kafka_partition_number,
        record_metadata:offset::integer                                         as metadata_kafka_partition_offset,
        to_timestamp(record_metadata:CreateTime::number / 1000)                 as metadata_kafka_message_arrival_ts,
        record_metadata:CreateTime::string                                      as metadata_kafka_message_arrival_ts_string,
        record_metadata:key::string                                             as metadata_kafka_key,
        record_metadata:schema_id::string                                       as metadata_kafka_schema_id,
        record_metadata:header::string                                          as metadata_kafka_message_header,
        to_timestamp(record_metadata:SnowflakeConnectorPushTime::number / 1000) as metadata_snowflake_connector_push_time_ts,
        record_metadata:SnowflakeConnectorPushTime                              as metadata_snowflake_connector_push_time
    from {{ source('kafka_dp_ingestion', 'asds_candidate_embedder_raw_embeddings_public_v1_snowpipe') }}
    {% if is_incremental() %}
        where record_metadata:SnowflakeConnectorPushTime > (
            select max(tab.metadata_snowflake_connector_push_time) as last_push_time from {{ this }} as tab
        )
    {% endif %}

),

deduplicated as (
    select * from source_data
    qualify row_number() over (
        partition by user_id
        order by metadata_kafka_message_arrival_ts desc
    ) = 1
)

select * from deduplicated
