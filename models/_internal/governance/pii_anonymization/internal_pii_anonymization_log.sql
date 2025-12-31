{{ config(
    materialized = 'incremental',
    unique_key = 'pii_anonymization_log_id',
    tags = ['governance', 'pii_anonymization', 'no_pii'],
     
) }}
select
    uuid_string()                             as pii_anonymization_log_id,
    '{{ var("pii_process", "pii_tagging") }}' as pii_process,
    '{{ var("pii_status", "finished") }}'     as pii_status,
    current_date()                            as pii_anonymization_log_date,
    current_timestamp()                       as pii_log_timestamp,
