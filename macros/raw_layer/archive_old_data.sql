{#
-- This macro moves data from raw table to the old history table
-- this is used in the case of very large tables that requires dividing into 
-- multiple tables (for example: mysql_xas.ad_deliviries_hst and mysql_xas.ad_deliveries_history)
-- it works by moving data older than X number of months to history table
-- and deleting data older than Y number of months from history table
-- variables:
-- daily_table_no_of_months: no of months to keep in daily table
-- archive_table_no_of_months: no of months to keep in the history table
#}

{%- macro archive_old_data(
    daily_table = '',
    archive_table = '',
    date_column = '',
    daily_table_no_of_months = 24,
    archive_table_no_of_months = 48
) -%}
    --begin transaction (to ensure all statements gets executed or none gets executed):
    {% do run_query("BEGIN TRANSACTION;")  %}
    -- insert old data into history table:
    {% do run_query("insert into " + archive_table + " select * from " + daily_table + " where " + date_column +" < DATEADD(MONTH, -" + daily_table_no_of_months|string +", current_date()); ")  %}
    -- delete the old rows from the daily table
    {% do run_query("delete from " + daily_table + " where " + date_column + " < DATEADD(MONTH, -" + daily_table_no_of_months|string + ", current_date());")  %}
    -- delete the old rows from the history table
    {% do run_query("delete from " + archive_table + " where " + date_column + " < DATEADD(MONTH, -" + archive_table_no_of_months|string + ", current_date());")  %}
    -- commit when everything is successful:
    {% do run_query("COMMIT;")  %}

{% endmacro %}