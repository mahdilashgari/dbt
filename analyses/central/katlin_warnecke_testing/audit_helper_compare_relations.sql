{#
-- use this set if you are comparing to a legacy model, rather than another dbt model
-- {% set old_etl_relation=adapter.get_relation(
--       database=target.database,
--       schema="old_etl_schema",
--       identifier="fct_orders"
-- ) -%}
#}

-- use this set if you are comparing to another dbt model
{% set old_etl_relation=ref('ftp_abilify__abilify_invoice_daily') %}

-- this is your newly built dbt model 
{% set dbt_relation=ref('central_fct_abilify_invoices_daily') %}

{{ audit_helper.compare_relations(
    a_relation=old_etl_relation,
    b_relation=dbt_relation,
    primary_key="invoice_sk"
) }}