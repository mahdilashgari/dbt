{#-
    The cast_to_timestamp_ltz macro converts a given timestamp from a specified time zone to UTC and then casts it to a timestamp_ltz (local time zone) format.
    It takes two parameters:
    - original_timezone: The original time zone of the timestamp.
    - timestamp_column_name: The name of the column containing the timestamp to be converted.
    The macro uses the convert_timezone function to adjust the timestamp to UTC, appends the UTC offset (+00:00), and then casts the result to timestamp_ltz.
-#}

{%- macro cast_to_timestamp_ltz(original_timezone, timestamp_column_name) -%}
    convert_timezone('{{ original_timezone }}', 'Europe/Berlin', {{ timestamp_column_name }})::timestamp_ltz
{%- endmacro -%}