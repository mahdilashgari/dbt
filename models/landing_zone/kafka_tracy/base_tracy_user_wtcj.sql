{{
  config(
    materialized = 'incremental',
    unique_key = 'tracy_user_wtcj_sk',
    tags = ['kafka_tracy', 'raw', 'kafka_models'],
    schema = 'kafka_tracy',
    )
}}

select
    * exclude (
        record_metadata,
        dt,
        value
    ),
    uuid_string()                                                           as tracy_user_wtcj_sk,
    parse_json(value)                                                       as value_json,
    parse_json(value):probability::number(20, 10)                           as probability,
    parse_json(value):reasons::varchar                                      as reasons,
    parse_json(value):wtcj_class::varchar                                   as wtcj_class,
    record_metadata:topic::string                                           as metadata_kafka_topic,
    record_metadata:PARTITION::string                                       as metadata_kafka_partition_number,
    record_metadata:offset::integer                                         as metadata_kafka_partition_offset,
    to_timestamp(record_metadata:CreateTime::number / 1000)                 as metadata_kafka_message_arrival_ts,
    record_metadata:CreateTime::string                                      as metadata_kafka_message_arrival_ts_string,
    record_metadata:KEY::string                                             as metadata_kafka_key,
    record_metadata:schema_id::string                                       as metadata_kafka_schema_id,
    record_metadata:header::string                                          as metadata_kafka_message_header,
    to_timestamp(record_metadata:SnowflakeConnectorPushTime::number / 1000) as metadata_snowflake_connector_push_time_ts,
    record_metadata:SnowflakeConnectorPushTime                              as metadata_snowflake_connector_push_time,
    current_date()                                                          as dt
from {{ source('kafka_tracy', 'tracy_user_wtcj_snowpipe') }}
where
    1 = 1
    {% if is_incremental() %}
        and record_metadata:SnowflakeConnectorPushTime > (
            select max(tab.metadata_snowflake_connector_push_time) as last_push_time from {{ this }} as tab
        )
    {% endif %}
