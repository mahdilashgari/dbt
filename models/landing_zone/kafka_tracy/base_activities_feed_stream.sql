{{
  config(
    materialized = 'incremental',
    unique_key = 'activities_feed_stream_sk',
    tags= ['kafka_tracy', 'raw', 'kafka_models'],
    schema= 'kafka_tracy',
    )
}}
select
    * exclude (
        record_metadata
    )
    ,
    uuid_string()                                                           as activities_feed_stream_sk
    ,
    activity:actor_id::NUMBER                                               as actor_id
    ,
    activity:actor_urn::VARCHAR                                             as actor_urn
    ,
    activity:changes::VARCHAR                                               as activity_changes
    ,
    activity:content::VARCHAR                                               as content
    ,
    activity:created_at::NUMBER                                             as created_at
    ,
    activity:deleted_at::NUMBER                                             as deleted_at
    ,
    activity:id::NUMBER                                                     as id
    ,
    activity:message::VARCHAR                                               as message
    ,
    activity:object_id::NUMBER                                              as object_id
    ,
    activity:object_urn::VARCHAR                                            as object_urn
    ,
    activity:origin_id::NUMBER                                              as origin_id
    ,
    activity:origin_urn::VARCHAR                                            as origin_urn
    ,
    activity:parent_id::NUMBER                                              as parent_id
    ,
    activity:parent_urn::VARCHAR                                            as parent_urn
    ,
    activity:story_type::VARCHAR                                            as story_type
    ,
    activity:taken_down_at::NUMBER                                          as taken_down_at
    ,
    activity:target_id::NUMBER                                              as target_id
    ,
    activity:target_urn::VARCHAR                                            as target_urn
    ,
    activity:updates::VARCHAR                                               as updates
    ,
    activity:verb::VARCHAR                                                  as verb
    ,
    activity:visibility::VARCHAR                                            as visibility,
    record_metadata:topic::STRING                                           as metadata_kafka_topic,
    record_metadata:PARTITION::STRING                                       as metadata_kafka_partition_number,
    record_metadata:offset::INTEGER                                         as metadata_kafka_partition_offset,
    to_timestamp(record_metadata:CreateTime::NUMBER / 1000)                 as metadata_kafka_message_arrival_ts,
    record_metadata:CreateTime::STRING                                      as metadata_kafka_message_arrival_ts_string,
    record_metadata:KEY::STRING                                             as metadata_kafka_key,
    record_metadata:schema_id::STRING                                       as metadata_kafka_schema_id,
    record_metadata:header::STRING                                          as metadata_kafka_message_header,
    to_timestamp(record_metadata:SnowflakeConnectorPushTime::NUMBER / 1000) as metadata_snowflake_connector_push_time_ts,
    record_metadata:SnowflakeConnectorPushTime                              as metadata_snowflake_connector_push_time,
from {{ source('kafka_tracy', 'activities_feeds_stream_snowpipe') }}
where
    dt >= '20250601'  -- Adjust this date as needed
    {% if is_incremental() %}
        and record_metadata:SnowflakeConnectorPushTime > (
            select max(tab.metadata_snowflake_connector_push_time) as last_push_time from {{ this }} as tab
        )
    {% endif %}
