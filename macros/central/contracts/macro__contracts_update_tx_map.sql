{#
-- This macro updates TX_MAP values of contracts (FCT_XING_CONTRACT_TRANSACTIONS in Payment & Billing datamodel)
--
-- Sytanx & Debug: dbt run-operation macro__contracts_update_tx_map
#}

{% macro macro__contracts_update_tx_map() %}

{% set events_table = ref('central_int_contracts__contract_tx_events') %}
{% set contract_tx_table = ref('central_fct_xing_contract_transactions') %}
{% set contract_details_table = ref('central_dim_xing_contract_details') %}
{% set product_table = ref('central_dim_xing_products') %}


{# START - For debug mode only, make sure this is commented on PROD #}
{#
{% set events_table = 'ANALYTICS.CENTRAL_STAGING.INT_CONTRACTS__CONTRACT_TX_EVENTS' %}
{% set contract_tx_table = 'ANALYTICS.TEST_ERDINC_ULUTURK.ALI__DIM_XING_CONTRACTS_INC' %}
{% set contract_details_table = 'ANALYTICS.CENTRAL.DIM_XING_CONTRACT_DETAILS' %}
{% set product_table = 'ANALYTICS.CENTRAL.DIM_XING_PRODUCTS' %}
#}
{# END - For debug mode only, make sure this is commented on PROD #}


{{ log("##Tables## events_table(" ~ events_table ~ ") ## contract_tx_table(" ~ contract_tx_table ~ ") ## contract_details_table(" ~ contract_details_table ~ ") ## product_table(" ~ product_table ~ ")", info=True) }}


{# --Set temp table unique name #}
{% set temp_table_name = contract_tx_table ~ '__DBT_TMP_' ~ modules.datetime.datetime.utcnow().strftime('%Y%m%d%H%M%S%f') %}


{# --Create a temp table that will handle the logic #}
{% set query %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_name }}_1 AS
SELECT DISTINCT
    ROUND((ROW_NUMBER() OVER (ORDER BY CONTRACT_ID ASC)/ 100000),0) AS SEQU,
	CONTRACT_ID
FROM  {{ contract_tx_table }} tx
join {{ events_table }} e on BITAND(tx.contract_tx_map, POW(2, e.type_bitnum))!= 0
WHERE TX_MAP_UPDATED_AT_UTC IS NULL {# In case this info becomes valuable at some point, this condition in Talend used to be tx.JOB_INSTANCE_ID >= ##JOB_INSTANCE_ID## #}
and e.contract_tx_type = 'payments.sync.contract.updated';
{% endset %}
{% set result = run_query(query) %}



{# START - For debug mode only, make sure this is commented on PROD #}
{#
{% set query %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_name }}_1 AS
SELECT *
FROM  {{ temp_table_name }}_1
WHERE CONTRACT_ID= 337549145;
{% endset %}
{% set result = run_query(query) %}
#}
{# END - For debug mode only, make sure this is commented on PROD #}



{# --BEGIN Grab MIN & MAX contracts involved in the current iteration #}
{% set query %}
SELECT SEQU, MAX(CONTRACT_ID) AS MAX_CONTRACT_ID, MIN(CONTRACT_ID) AS MIN_CONTRACT_ID
FROM {{ temp_table_name }}_1
GROUP BY SEQU;
{% endset %}

{% set result_contracts_grouped_by_sequ = run_query(query) %}
{# --END Grab MIN & MAX contracts #}


{# --START iterate on every SEQU #}
{% for current_sequ_row in result_contracts_grouped_by_sequ %}

{% set SEQU = current_sequ_row[0]|int %}
{% set MAX_CONTRACT_ID = current_sequ_row[1]|int %}
{% set MIN_CONTRACT_ID = current_sequ_row[2]|int %}
{{ log('SEQU=' ~ SEQU ~ " ### MIN_CONTRACT_ID=" ~ MIN_CONTRACT_ID ~ " ### MAX_CONTRACT_ID=" ~ MAX_CONTRACT_ID ~ " || Number of contracts in current iteration=" ~ (MAX_CONTRACT_ID-MIN_CONTRACT_ID+1), info=True) }}

{% set query %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_name }}_2 AS
SELECT
	K_CONTRACT_TX_EVENT,
	ROW_CREATE_DATE,
	ROW_UPDATE_DATE,
	TYPE_BITNUM,
	TYPE_BITVALUE,
	CONTRACT_TX_TYPE,
	CONTRACT_TX_DATE_SRC,
    IS_DERIVED,
	PAUSED,
	PAUSED_PREV,
	BLOCKED_INVOICE,
	BLOCKED_INVOICE_PREV,
	CANCELED,
	CANCELED_PREV,
	ENDED_AT,
	ENDED_AT_PREV,
	CANCEL_DATE_GREATER_THAN_START_DATE,
	CONSUMPTION_ENDED_AT,
	IS_DELETED,
    BK_CONTRACT_ID_PREV
FROM {{ events_table }}
WHERE IS_DERIVED AND IS_ACTIVE;
{% endset %}

{% set result = run_query(query) %}

{# --BEGIN SET the value for a new column: KEY #}
{% set query %}
ALTER TABLE {{ temp_table_name }}_2 ADD COLUMN KEY VARCHAR(500);
{% endset %}

{% set result = run_query(query) %}

{% set query %}
UPDATE {{ temp_table_name }}_2
SET KEY = CONCAT('IS_PAUSED;', COALESCE(PAUSED::VARCHAR, 'XXXX'),
                ',PAUSED_PREV;', COALESCE(PAUSED_PREV::VARCHAR, 'XXXX'),
                ',IS_BLOCKED_INVOICE;', COALESCE(BLOCKED_INVOICE::VARCHAR, 'XXXX'),
                ',BLOCKED_INVOICE_PREV;', COALESCE(BLOCKED_INVOICE_PREV::VARCHAR, 'XXXX'),
                ',IS_CANCELED;', COALESCE(CANCELED::VARCHAR, 'XXXX'),

                ',CANCELED_PREV;', CASE WHEN CANCELED IS NULL THEN 'XXXX' ELSE CANCELED_PREV END,

                ',ENDED_AT;', COALESCE(ENDED_AT::VARCHAR, 'XXXX'),
                ',ENDED_AT_PREV;', COALESCE(ENDED_AT_PREV::VARCHAR, 'XXXX'),
                ',CANCEL_DATE_GREATER_THAN_START_DATE;', COALESCE(CANCEL_DATE_GREATER_THAN_START_DATE::VARCHAR, 'XXXX'),
                ',CONSUMPTION_ENDED_AT;', COALESCE(CONSUMPTION_ENDED_AT::VARCHAR, 'XXXX'),
                ',IS_DELETED;', COALESCE(IS_DELETED::VARCHAR, 'XXXX'),
                ',BK_CONTRACT_ID_PREV;', COALESCE(BK_CONTRACT_ID_PREV::VARCHAR, 'XXXX'));
{% endset %}

{% set result = run_query(query) %}

{# --END SET the value for a new column: KEY #}

{# --BEGIN Normalize the values in KEY into a new column new_key  #}

{% set query %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_name }}_2 AS
SELECT
  t.*,
  TRIM(normalized_key.VALUE) AS new_key
FROM
  {{ temp_table_name }}_2 t,
  LATERAL FLATTEN(SPLIT(KEY, ',')) AS normalized_key;
{% endset %}

{% set result = run_query(query) %}

{# --END Normalize the values in KEY into a new column new_key  #}

{# --BEGIN SPLIT the values in new_key into two columns (;) #}

{% set query %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_name }}_2 AS
SELECT
  * EXCLUDE(KEY, NEW_KEY),
  TRIM(SPLIT_PART(NEW_KEY, ';', 1)) AS pivot_key,
  TRIM(SPLIT_PART(NEW_KEY, ';', 2)) AS pivot_val
FROM {{ temp_table_name }}_2
WHERE pivot_val != 'XXXX';
{% endset %}

{% set result = run_query(query) %}

{# --END SPLIT the values in new_key into two columns (;) #}

{# --START initial preparation to generate an update stmt #}

{% set query %}
UPDATE {{ temp_table_name }}_2
SET pivot_val = CASE
                     WHEN pivot_val LIKE '%NULL%' THEN pivot_key || ' IS ' || pivot_val
                     WHEN pivot_val LIKE '%<>%' THEN '(' || pivot_key || ' ' || pivot_val || ' ' || REPLACE(pivot_key, '_PREV', '') || ' OR ' || pivot_key || ' IS NULL)'
                     ELSE pivot_key || ' = ' || pivot_val
                  END;
{% endset %}

{% set result = run_query(query) %}

{# --END initial preparation to generate an update stmt #}

{# --BEGIN Denormalize the rows #}

{% set query %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_name }}_2 AS
    SELECT DISTINCT
    K_CONTRACT_TX_EVENT,
    ROW_CREATE_DATE,
    ROW_UPDATE_DATE,
    TYPE_BITNUM,
    TYPE_BITVALUE,
    CONTRACT_TX_TYPE,
    CONTRACT_TX_DATE_SRC,
    LISTAGG(PIVOT_VAL, ' AND ') WITHIN GROUP (ORDER BY K_CONTRACT_TX_EVENT,ROW_CREATE_DATE,ROW_UPDATE_DATE,TYPE_BITNUM,TYPE_BITVALUE,CONTRACT_TX_TYPE,CONTRACT_TX_DATE_SRC) OVER (PARTITION BY K_CONTRACT_TX_EVENT) AS CONDITIONS
FROM
    {{ temp_table_name }}_2;
{% endset %}

{% set result = run_query(query) %}

{# --END Denormalize the rows #}

{% set query %}
SELECT COUNT(*) FROM {{ temp_table_name }}_2;
{% endset %}
{% set result = run_query(query) %}

{{ log("!!!!COUNT IS = " ~ result[0][0], info=True) }}

{% set query = create_temp_table(temp_table_name ~ "_3") %}
{% set result = run_query(query) %}

{% set query %}
SELECT * FROM {{ temp_table_name }}_2;
{% endset %}
{% set result = run_query(query) %}

{# ---------------------------------------------- #}
{# Transform the result rows into a list of dictionaries bcz this makes it easier to pickup the right column later (rows from {{ temp_table_name }}_2) #}
{% set result = result_tuples_to_dict(result) %}

{# ---------------------------------------------- #}

{# Log column names #}
{% for column in result.column_names %}
    {{ log("##$$$$$$$$$$$$$$$$$$$$" ~ column.name ~ "##$$$$$$$$$$$$$$$$$$$$" , info=True) }}
{% endfor %}

{% set ns = namespace(current_row=-1) %}
{% for row in result %}
    {% set ns.current_row =  ns.current_row + 1 %}
    {{ log("##//Iteration: " ~ ns.current_row ~ " result[" ~ ns.current_row ~ "][CONDITIONS]= " ~ result[ns.current_row]['CONDITIONS'] ~ " | result[ns.current_row]['TYPE_BITVALUE']= " ~ result[ns.current_row]['TYPE_BITVALUE'] ~ "//##" , info=True) }}

    {% set query = getSqlTemplate(temp_table_name ~ "_3", result[ns.current_row]['CONDITIONS'], result[ns.current_row]['TYPE_BITVALUE'], "'" ~ result[ns.current_row]['CONTRACT_TX_TYPE'] ~ "'", MIN_CONTRACT_ID, MAX_CONTRACT_ID, contract_tx_table, contract_details_table, product_table, events_table) %}
    {% set result = run_query(query) %}
{% endfor %}

{# Now from each row, we'll calculate the new TX_MAP and update it in the contracts table #}
{{ log("##$$$$$$$$$$$$$$$$$$$$ 11111111111111" , info=True) }}


{# START - For debug mode only, make sure this is commented on PROD - create {{ temp_table_name }}_3_init #}
{#
{% set query %}
CREATE OR REPLACE TABLE {{ temp_table_name }}_3_init AS SELECT * FROM {{ temp_table_name }}_3;
{% endset %}
{% set result = run_query(query) %}
#}
{# END - For debug mode only, make sure this is commented on PROD - create {{ temp_table_name }}_3_init #}


{# Now some aggregations to be applied to the table #}
{% set query %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_name }}_3 AS
SELECT SUM(BIT_VALUE) AS BIT_VALUE, COUNT(EVENT) AS EVENT, MAX(ROW_UPDATE_DATE) AS ROW_UPDATE_DATE,
CONTRACT_ID,
TX_MAP_UPDATED_AT_UTC,
ROW_CREATE_DATE,
XING_USER_ID,
CURRENCY,
CONTRACT_TX_DATE,
CONTRACT_TX_DATE_NEXT,
CONTRACT_TX_MAP,
IS_PAUSED,
UNPAUSE_ON,
IS_BLOCKED_INVOICE,
IS_CANCELED,
CANCELED_AT,
ENDED_AT,
RESUMED_AT,
DURATION,
DURATION_TYPE,
IS_RENEWED,
IS_RENEWAL,
IS_SUBSCRIPTION,
WAS_SUBSCRIPTION,
IS_INVOICED,
AGREEMENT_AT,
CONSUMPTION_ENDED_AT,
CONTRACT_TYPE,
MERCHANT,
UPDATED_AT,
CREATED_AT,
IS_DELETED
FROM {{ temp_table_name }}_3
GROUP BY CONTRACT_ID, TX_MAP_UPDATED_AT_UTC, ROW_CREATE_DATE,XING_USER_ID,CURRENCY,CONTRACT_TX_DATE,CONTRACT_TX_DATE_NEXT,CONTRACT_TX_MAP,IS_PAUSED,UNPAUSE_ON,IS_BLOCKED_INVOICE,IS_CANCELED,CANCELED_AT,ENDED_AT,RESUMED_AT,DURATION,DURATION_TYPE,IS_RENEWED,IS_RENEWAL,IS_SUBSCRIPTION,WAS_SUBSCRIPTION,IS_INVOICED,AGREEMENT_AT,CONSUMPTION_ENDED_AT,CONTRACT_TYPE,MERCHANT,UPDATED_AT,CREATED_AT,IS_DELETED;
{% endset %}
{% set result = run_query(query) %}
{# ################ #}


{# --START Calculate & update TX_MAP #}
    {% set query %}
        MERGE INTO {{ contract_tx_table }} trg
        USING {{ temp_table_name }}_3 src
        ON trg.CONTRACT_ID = src.CONTRACT_ID
        AND trg.ROW_CREATE_DATE = src.ROW_CREATE_DATE
        AND trg.CONTRACT_TX_DATE = src.CONTRACT_TX_DATE
        AND trg.TX_MAP_UPDATED_AT_UTC IS NULL
        WHEN MATCHED THEN UPDATE SET
        trg.ROW_UPDATE_DATE = src.ROW_UPDATE_DATE,
        trg.CONTRACT_TX_MAP = (src.CONTRACT_TX_MAP + src.BIT_VALUE),
        trg.TX_MAP_UPDATED_AT_UTC = sysdate(),
        trg.contract_operational_sk = {{ dbt_utils.generate_surrogate_key([
            'trg.CONTRACT_ID',
            'trg.CURRENCY',
            '(src.CONTRACT_TX_MAP + src.BIT_VALUE)',
            'coalesce(trg.IS_PAUSED, true)',
            'coalesce(trg.IS_BLOCKED_INVOICE, true)',
            'coalesce(trg.IS_CANCELED, true)',
            'trg.CANCELED_AT',
            'trg.ENDED_AT',
            'trg.RESUMED_AT',
            'trg.DURATION',
            'trg.DURATION_TYPE',
            'coalesce(trg.IS_RENEWED, true)',
            'coalesce(trg.IS_RENEWAL, true)',
            'coalesce(trg.IS_SUBSCRIPTION, true)',
            'coalesce(trg.WAS_SUBSCRIPTION, true)',
            'coalesce(trg.IS_INVOICED, true)',
            'trg.AGREEMENT_AT',
            'trg.CONTRACT_TYPE',
            'trg.MERCHANT',
            'trg.UPDATED_AT',
            'trg.CREATED_AT',
            'coalesce(trg.IS_DELETED, true)',
            'trg.CONSUMPTION_ENDED_AT',
            'trg.UNPAUSE_ON',
            'trg.K_DATA_SOURCE'
        ]) }}
        ;
    {% endset %}
    {% set result = run_query(query) %}
{# --END Calculate & update TX_MAP #}


{# --START Setting current_timestamp for TX_MAP_UPDATED_AT_UTC that are NULLs At the end of finishing the work on the current SEQU, those are the rows that will remain with their default TX_MAP but we still need to mention that they were treated #}
{% set query %}
UPDATE {{ contract_tx_table }} SET TX_MAP_UPDATED_AT_UTC = sysdate()
WHERE CONTRACT_ID >= {{ MIN_CONTRACT_ID }} AND CONTRACT_ID <= {{ MAX_CONTRACT_ID }} AND TX_MAP_UPDATED_AT_UTC IS NULL;
{% endset %}
{% set result = run_query(query) %}
{# --END Setting current_timestamp for TX_MAP_UPDATED_AT_UTC that are NULLs At the end of finishing the work on the current SEQU, those are the rows that will remain with their default TX_MAP but we still need to mention that they were treated #}


{% endfor %} {# --END iterate on every SEQU #}

{% endmacro %}


{% macro getSqlTemplate(temp_table_to_create, condition, bit_value, event, min_bk_id, max_bk_id, contract_tx_table, contract_details_table, product_table, events_table) %}
INSERT INTO {{ temp_table_to_create }}
SELECT
	s.CONTRACT_ID,
    s.TX_MAP_UPDATED_AT_UTC,
	s.ROW_CREATE_DATE,
	sysdate() AS ROW_UPDATE_DATE,
	s.XING_USER_ID,
	s.CURRENCY,
	s.CONTRACT_TX_DATE,
	s.CONTRACT_TX_DATE_NEXT,
	s.CONTRACT_TX_MAP,
	s.IS_PAUSED,
	s.UNPAUSE_ON,
	s.IS_BLOCKED_INVOICE,
	s.IS_CANCELED,
	s.CANCELED_AT,
	s.ENDED_AT,
	s.RESUMED_AT,
	s.DURATION,
	s.DURATION_TYPE,
	s.IS_RENEWED,
	s.IS_RENEWAL,
	s.IS_SUBSCRIPTION,
	s.WAS_SUBSCRIPTION,
	s.IS_INVOICED,
	s.AGREEMENT_AT,
	s.CONSUMPTION_ENDED_AT,
	s.CONTRACT_TYPE,
	s.MERCHANT,
	s.UPDATED_AT,
	s.CREATED_AT,
	s.IS_DELETED,
	{{ bit_value }} AS BIT_VALUE,
	{{ event }} AS EVENT
FROM (
	--all contract versions of chunk
	SELECT
		A.CONTRACT_ID,
        A.TX_MAP_UPDATED_AT_UTC,
		A.ROW_CREATE_DATE,
		A.ROW_UPDATE_DATE,
		A.XING_USER_ID,
		A.CURRENCY,
		A.CONTRACT_TX_DATE,
		A.CONTRACT_TX_DATE_NEXT,
		A.CONTRACT_TX_MAP,
		A.IS_PAUSED,
		A.UNPAUSE_ON,
		A.IS_BLOCKED_INVOICE,
		A.IS_CANCELED,
		A.CANCELED_AT,
		A.ENDED_AT,
		A.RESUMED_AT,
		A.DURATION,
		A.DURATION_TYPE,
		A.IS_RENEWED,
		A.IS_RENEWAL,
		A.IS_SUBSCRIPTION,
		A.WAS_SUBSCRIPTION,
		A.IS_INVOICED,
		A.AGREEMENT_AT,
		A.CONSUMPTION_ENDED_AT,
		A.CONTRACT_TYPE,
		A.MERCHANT,
		A.UPDATED_AT,
		A.CREATED_AT,
		A.IS_DELETED,
		LAG (A.IS_PAUSED, 1) OVER (PARTITION BY A.CONTRACT_ID ORDER BY A.CONTRACT_TX_DATE ASC) AS PAUSED_PREV,
		LAG (A.IS_BLOCKED_INVOICE, 1) OVER (PARTITION BY A.CONTRACT_ID ORDER BY A.CONTRACT_TX_DATE ASC) AS BLOCKED_INVOICE_PREV,
		LAG (A.IS_CANCELED, 1) OVER (PARTITION BY A.CONTRACT_ID ORDER BY A.CONTRACT_TX_DATE ASC) AS CANCELED_PREV,
		LAG (A.ENDED_AT, 1) OVER (PARTITION BY A.CONTRACT_ID ORDER BY A.CONTRACT_TX_DATE ASC) AS ENDED_AT_PREV,
		LAG (A.CONTRACT_ID, 1) OVER (PARTITION BY A.CONTRACT_ID ORDER BY A.CONTRACT_TX_DATE ASC) AS BK_CONTRACT_ID_PREV,
		CASE WHEN A.CONTRACT_TX_DATE > cd.START_DATE THEN TRUE ELSE FALSE END CANCEL_DATE_GREATER_THAN_START_DATE --due to the fact that the CANCEL_DATE is not set constantly by P&B, we have to take our event date
	FROM
		{{ contract_tx_table }} AS A
	INNER JOIN (
				--chunk of mapr delta
				SELECT DISTINCT tx.CONTRACT_ID FROM {{ contract_tx_table }} tx
				WHERE
                TX_MAP_UPDATED_AT_UTC IS NULL {# In case this info becomes valuable at some point, this condition in Talend used to be tx.JOB_INSTANCE_ID >= ##JOB_INSTANCE_ID## #}
				AND tx.CONTRACT_ID >= {{ min_bk_id }} AND tx.CONTRACT_ID <= {{ max_bk_id }}  	-- chunk of delta
				) base ON base.CONTRACT_ID = A.CONTRACT_ID
	LEFT JOIN ( --this JOIN is to get the Start_Date of the contract which is needed for the derived event definition (cancel events)
		SELECT cd.CONTRACT_ID, MAX(cd.START_DATE) START_DATE
		FROM {{ contract_details_table }} cd
		JOIN {{ product_table }} p on p.XING_PRODUCT_SK = cd.XING_PRODUCT_SK AND p.PRODUCT_GROUP IN ('JSR','PBS','PRM','SAL') --derived events should only be created for all subscription b2c products. since we don't have a product catalogue, the products which are in focus are hard coded here. Best Practice should be a proper product entity with an attribute "IS_SUBSCRIPTION_PRODUCT" which could be used here instead of hard coded product groups.
		WHERE cd.START_DATE IS NOT NULL
		GROUP BY cd.CONTRACT_ID --due to the fact that there are some (obsolete) contracts with the same premium product and 2 start dates, these technical workaround avoids multiplied rows (e.g. CONTRACT_ID 00302c501da1bc95266352b8921c4a175970fd9e)
		) cd on cd.CONTRACT_ID = A.CONTRACT_ID
) s
INNER JOIN (SELECT DISTINCT e.CONTRACT_TX_TYPE, e.TYPE_BITNUM FROM {{ events_table }} e) e on BITAND(s.CONTRACT_TX_MAP, POW(2, e.TYPE_BITNUM))!= 0 -- needed to filter only on updates
LEFT JOIN (SELECT e.CONTRACT_TX_TYPE, e.TYPE_BITNUM FROM {{ events_table }} e WHERE e.CONTRACT_TX_TYPE = {{ event }}) e_lifecycle on BITAND(s.CONTRACT_TX_MAP, POW(2, e_lifecycle.TYPE_BITNUM))!= 0 -- checks if the lifecycle event already exists
WHERE e.CONTRACT_TX_TYPE in ('payments.sync.contract.updated') -- only updates should be considered, to make sure that interpreted events were only cretaed based on  an already existing contract version.
AND e_lifecycle.CONTRACT_TX_TYPE IS NULL --if event is already interpreted than do nothing
AND (
	{{ condition }}
);
{% endmacro %}

{% macro create_temp_table(temp_table_to_create) %}
CREATE OR REPLACE TEMPORARY TABLE {{ temp_table_to_create }}
    (
        CONTRACT_ID NUMBER(38,0),
        TX_MAP_UPDATED_AT_UTC TIMESTAMP_NTZ(9),
        ROW_CREATE_DATE TIMESTAMP_NTZ(9),
        ROW_UPDATE_DATE TIMESTAMP_NTZ(9),
        XING_USER_ID NUMBER(38,0),
        CURRENCY     VARCHAR(16777216),
        CONTRACT_TX_DATE TIMESTAMP_NTZ(9),
        CONTRACT_TX_DATE_NEXT TIMESTAMP_NTZ(9),
        CONTRACT_TX_MAP NUMBER(38,0),
        IS_PAUSED       BOOLEAN,
        UNPAUSE_ON TIMESTAMP_NTZ(9),
        IS_BLOCKED_INVOICE BOOLEAN,
        IS_CANCELED        BOOLEAN,
        CANCELED_AT TIMESTAMP_NTZ(9),
        ENDED_AT TIMESTAMP_NTZ(9),
        RESUMED_AT TIMESTAMP_NTZ(9),
        DURATION         NUMBER(38,0),
        DURATION_TYPE    VARCHAR(16777216),
        IS_RENEWED       BOOLEAN,
        IS_RENEWAL       BOOLEAN,
        IS_SUBSCRIPTION  BOOLEAN,
        WAS_SUBSCRIPTION BOOLEAN,
        IS_INVOICED      BOOLEAN,
        AGREEMENT_AT TIMESTAMP_NTZ(9),
        CONSUMPTION_ENDED_AT TIMESTAMP_NTZ(9),
        CONTRACT_TYPE VARCHAR(16777216),
        MERCHANT      VARCHAR(16777216),
        UPDATED_AT TIMESTAMP_NTZ(9),
        CREATED_AT TIMESTAMP_NTZ(9),
        IS_DELETED BOOLEAN,
        BIT_VALUE  NUMBER(38,0),
        EVENT      VARCHAR(44)
    );
{% endmacro %}

{# Transforms the result rows into a dictionary bcz this makes it easier to pickup the right column later #}
{% macro result_tuples_to_dict(results) %}
  {% set column_names = results.columns %}
  {{ log("##$$$$$$$$$$$$$$$$$$$$ 11111111111111----" , info=True) }}

  {% set rows = [] %}
  {% for row in results %}
      {% set row_dict = {} %}
      {% for column, value in zip(column_names, row) %}
          {% do row_dict.update({ column.name: value }) %}
      {% endfor %}
      {% do rows.append(row_dict) %}
  {% endfor %}
  {{ return(rows) }} {# Use the return function provided by dbt to send back the rows #}
{% endmacro %}