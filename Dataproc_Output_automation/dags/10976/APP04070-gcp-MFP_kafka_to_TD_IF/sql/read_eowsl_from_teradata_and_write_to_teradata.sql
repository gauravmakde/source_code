BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
-- SET QUERY_BAND = '
-- App_ID=APP04070;
-- DAG_ID=project_mfp_eowsl_kafka_to_td_tpt;
-- Task_Name=read_eowsl_from_kafka_and_write_to_td_job_6;'
-- FOR SESSION VOLATILE;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_STOCKLEDGER_WEEK_FACT',  '{{params.database_name_fact}}',  'project_mfp_eowsl_kafka_to_td_tpt',  'job_6',  0,  'LOAD_START',  'Delete from fact and stage tables',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

-- This table to Test Teradata engine.
BEGIN
SET _ERROR_CODE  =  0;

DELETE FROM {{params.gcp_project_id}}.{{params.database_name_fact}}.merch_stockledger_week_fact AS tgt
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.database_name_staging}}.merch_stockledger_week_ldg AS stg
    WHERE LOWER(end_of_week_date) <> LOWER('end_of_week_date') AND LOWER(tgt.location_num) = LOWER(location_num) AND tgt.product_hierarchy_dept_num = CAST(product_hierarchy_dept_num AS FLOAT64) AND tgt.product_hierarchy_class_num = CAST(product_hierarchy_class_num AS FLOAT64) AND tgt.product_hierarchy_subclass_num = CAST(product_hierarchy_subclass_num AS FLOAT64) AND tgt.end_of_week_date = CAST(end_of_week_date AS DATE) AND tgt.last_updated_time_in_millis < CAST(CASE WHEN last_updated_time_in_millis = '' THEN '0' ELSE last_updated_time_in_millis END AS BIGINT));

EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

BEGIN
SET _ERROR_CODE  =  0;
DELETE FROM {{params.gcp_project_id}}.{{params.database_name_staging}}.merch_stockledger_week_ldg AS stg
WHERE EXISTS (SELECT *
    FROM {{params.gcp_project_id}}.{{params.database_name_fact}}.merch_stockledger_week_fact AS tgt
    WHERE LOWER(stg.end_of_week_date) <> LOWER('end_of_week_date') AND LOWER(location_num) = LOWER(stg.location_num) AND product_hierarchy_dept_num = CAST(stg.product_hierarchy_dept_num AS FLOAT64) AND product_hierarchy_class_num = CAST(stg.product_hierarchy_class_num AS FLOAT64) AND product_hierarchy_subclass_num = CAST(stg.product_hierarchy_subclass_num AS FLOAT64) AND end_of_week_date = CAST(stg.end_of_week_date AS DATE) AND CAST(CASE WHEN stg.last_updated_time_in_millis = '' THEN '0' ELSE stg.last_updated_time_in_millis END AS BIGINT) <= last_updated_time_in_millis);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_STOCKLEDGER_WEEK_FACT',  '{{params.database_name_fact}}',  'project_mfp_eowsl_kafka_to_td_tpt',  'job_6',  1,  'INTERMEDIATE',  'Filling fact table',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

BEGIN
SET _ERROR_CODE  =  0;

INSERT INTO {{params.gcp_project_id}}.{{params.database_name_fact}}.merch_stockledger_week_fact 
(eowsl_num, correlation_num, event_time,event_time_tz, location_num,
 last_updated_time_in_millis, last_updated_time,last_updated_time_tz, ledger_book_name, end_of_week_date, product_hierarchy_dept_num,
 product_hierarchy_class_num, product_hierarchy_subclass_num, fiscal_year, calendar_half, month_name, week,
 closing_stock_retail_currency_code, total_closing_stock_retail, closing_stock_cost_currency_code,
 total_closing_stock_cost, open_stock_retail_currency_code, total_open_stock_retail, open_stock_cost_currency_code,
 total_open_stock_cost, stock_adjustment_retail_currency_code, stock_adjustment_cost_currency_code,
 stock_take_adjustment_retail_currency_code, stock_take_adjustment_cost_currency_code,
 stock_adjustment_cogs_retail_currency_code, stock_adjustment_cogs_cost_currency_code, purchase_retail_currency_code,
 purchase_cost_currency_code, deal_income_purchases_currency_code, freight_claim_retail_currency_code,
 freight_claim_cost_currency_code, freight_cost_currency_code, profit_up_charges_currency_code,
 expense_up_charges_currency_code, rtv_retail_currency_code, rtv_cost_currency_code, transfer_in_retail_currency_code,
 transfer_in_cost_currency_code, transfer_in_book_retail_currency_code, transfer_in_book_cost_currency_code,
 transfer_out_retail_currency_code, transfer_out_cost_currency_code, transfer_out_book_retail_currency_code,
 transfer_out_book_cost_currency_code, intercompany_in_retail_currency_code, intercompany_in_cost_currency_code,
 intercompany_out_retail_currency_code, intercompany_out_cost_currency_code, intercompany_mark_up_currency_code,
 intercompany_mark_down_currency_code, intercompany_margin_currency_code, sales_quantities,
 net_sale_retail_currency_code, net_sales_cost_currency_code, net_sales_retail_excluding_vat_currency_code,
 returns_retail_currency_code, returns_cost_currency_code, net_sales_non_inventory_retail_currency_code,
 net_sales_non_inventory_cost_currency_code, net_sales_non_inventory_retail_excluding_vat_currency_code,
 markup_retail_currency_code, markup_cancelation_retail_currency_code, clear_markdown_retail_currency_code,
 permanent_markdown_retail_currency_code, promotion_markdown_retail_currency_code,
 markdown_cancelation_retail_currency_code, franchise_returns_retail_currency_code, franchise_returns_cost_currency_code
 , franchise_sales_retail_currency_code, franchise_sales_cost_currency_code, franchise_markup_retail_currency_code,
 franchise_markdown_retail_currency_code, employee_discount_retail_currency_code, deal_income_sales_currency_code,
 workroom_amt_currency_code, cash_discount_amt_currency_code, cumulative_markon_percent, shrinkage_retail_currency_code
 , shrinkage_cost_currency_code, reclass_in_retail_currency_code, reclass_in_cost_currency_code,
 reclass_out_retail_currency_code, reclass_out_cost_currency_code, gross_margin_amt_currency_code,
 margin_cost_variance_currency_code, retail_cost_variance_currency_code, cost_variance_amt_currency_code,
 half_to_date_doods_available_for_sale_retail_currency_code, half_to_date_goods_available_for_sale_cost_currency_code,
 receiver_cost_adjustment_variance_currency_code, workorder_actvity_update_inventory_amt_currency_code,
 workorder_actvity_post_to_finance_amt_currency_code, restocking_fee_currency_code,
 franchise_restocking_fee_currency_code, input_vat_amt_currency_code, output_vat_amt_currency_code,
 weight_variance_retail_currency_code, recoverable_tax_amt_currency_code, total_stock_adjustment_retail,
 total_stock_adjustment_cost, total_stock_take_adjustment_retail, total_stock_take_adjustment_cost,
 total_stock_adjustment_cogs_retail, total_stock_adjustment_cogs_cost, total_purchase_retail, total_purchase_cost,
 total_deal_income_purchases_cost, total_freight_claim_retail, total_freight_claim_cost, total_freight_cost,
 total_profit_up_charges_cost, total_expense_up_charges_cost, total_rtv_retail, total_rtv_cost, total_transfer_in_retail
 , total_transfer_in_cost, total_transfer_in_book_retail, total_transfer_in_book_cost_cost, total_transfer_out_retail,
 total_transfer_out_cost, total_transfer_out_book_retail, total_transfer_out_book_cost, total_intercompany_in_retail,
 total_intercompany_in_cost, total_intercompany_out_retail, total_intercompany_out_cost, total_intercompany_mark_up_cost
 , total_intercompany_mark_down_cost, total_intercompany_margin_cost, total_net_sale_retail, total_net_sales_cost_cost,
 total_net_sales_retail_excluding_vat_cost, total_returns_retail, total_returns_cost,
 total_net_sales_non_inventory_retail, total_net_sales_non_inventory_cost,
 total_net_sales_non_inventory_retail_excluding_vat_cost, total_markup_retail, total_markup_cancelation_retail,
 total_clear_markdown_retail, total_permanent_markdown_retail, total_promotion_markdown_retail,
 total_markdown_cancelation_retail, total_franchise_returns_retail, total_franchise_returns_cost,
 total_franchise_sales_retail, total_franchise_sales_cost, total_franchise_markup_retail,
 total_franchise_markdown_retail, total_employee_discount_retail, total_deal_income_sales_cost, total_workroom_amt_cost
 , total_cash_discount_amt_cost, total_shrinkage_retail, total_shrinkage_cost, total_reclass_in_retail,
 total_reclass_in_cost, total_reclass_out_retail, total_reclass_out_cost, total_gross_margin_amt_cost,
 total_margin_cost_variance_cost, total_retail_cost_variance_cost, total_cost_variance_amt_cost,
 total_half_to_date_doods_available_for_sale_retail, total_half_to_date_goods_available_for_sale_cost,
 total_receiver_cost_adjustment_variance_cost, total_workorder_actvity_update_inventory_amt_cost,
 total_workorder_actvity_post_to_finance_amt_cost, total_restocking_fee_cost, total_franchise_restocking_fee_cost,
 total_input_vat_amt_cost, total_output_vat_amt_cost, total_weight_variance_retail, total_recoverable_tax_amt_cost,
 dw_sys_load_tmstp, dw_sys_updt_tmstp)
(SELECT eowsl_num,
  correlation_num,
  CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(event_time) AS TIMESTAMP) AS event_time, 
  `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(`{{params.gcp_project_id}}.NORD_UDF.ISO8601_TMSTP`(event_time) AS string)) AS event_time_tz,
  SUBSTR(location_num, 1, 10) AS location_num,
  CAST(last_updated_time_in_millis AS BIGINT) AS last_updated_time_in_millis,
  CAST(`{{params.gcp_project_id}}.NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS FLOAT64)) AS INTEGER)) AS TIMESTAMP) AS last_updated_time, 
      `{{params.gcp_project_id}}.JWN_UDF.UDF_TIME_ZONE`(CAST(`NORD_UDF.EPOCH_TMSTP`(CAST(TRUNC(CAST(CASE
      WHEN last_updated_time_in_millis = ''
      THEN '0'
      ELSE last_updated_time_in_millis
      END AS FLOAT64)) AS INTEGER)) AS STRING) ) as last_updated_time_tz,
  SUBSTR(ledger_book_name, 1, 10) AS ledger_book_name,
  CAST(end_of_week_date AS DATE) AS end_of_week_date,
  CAST(TRUNC(CAST(product_hierarchy_dept_num AS FLOAT64)) AS INTEGER) AS product_hierarchy_dept_num,
  CAST(TRUNC(CAST(product_hierarchy_class_num AS FLOAT64)) AS INTEGER) AS product_hierarchy_class_num,
  CAST(TRUNC(CAST(product_hierarchy_subclass_num AS FLOAT64)) AS INTEGER) AS product_hierarchy_subclass_num,
  CAST(TRUNC(CAST(fiscal_year AS FLOAT64)) AS INTEGER) AS fiscal_year,
  calendar_half,
  month_name,
  CAST(TRUNC(CAST(week AS FLOAT64)) AS INTEGER) AS week,
  SUBSTR(closing_stock_retail_currency_code, 1, 10) AS closing_stock_retail_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN closing_stock_retail_units = ''
       THEN '0'
       ELSE closing_stock_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN closing_stock_retail_nanos = ''
        THEN '0'
        ELSE closing_stock_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_closing_stock_retail,
  SUBSTR(closing_stock_cost_currency_code, 1, 5) AS closing_stock_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN closing_stock_cost_units = ''
       THEN '0'
       ELSE closing_stock_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN closing_stock_cost_nanos = ''
        THEN '0'
        ELSE closing_stock_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_closing_stock_cost,
  SUBSTR(open_stock_retail_currency_code, 1, 5) AS open_stock_retail_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN open_stock_retail_units = ''
       THEN '0'
       ELSE open_stock_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN open_stock_retail_nanos = ''
        THEN '0'
        ELSE open_stock_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_open_stock_retail,
  SUBSTR(open_stock_cost_currency_code, 1, 5) AS open_stock_cost_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN open_stock_cost_units = ''
       THEN '0'
       ELSE open_stock_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN open_stock_cost_nanos = ''
        THEN '0'
        ELSE open_stock_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_open_stock_cost,
  SUBSTR(stock_adjustment_retail_currency_code, 1, 5) AS stock_adjustment_retail_currency_code,
  SUBSTR(stock_adjustment_cost_currency_code, 1, 5) AS stock_adjustment_cost_currency_code,
  SUBSTR(stock_take_adjustment_retail_currency_code, 1, 5) AS stock_take_adjustment_retail_currency_code,
  SUBSTR(stock_take_adjustment_cost_currency_code, 1, 5) AS stock_take_adjustment_cost_currency_code,
  SUBSTR(stock_adjustment_cogs_retail_currency_code, 1, 5) AS stock_adjustment_cogs_retail_currency_code,
  SUBSTR(stock_adjustment_cogs_cost_currency_code, 1, 5) AS stock_adjustment_cogs_cost_currency_code,
  SUBSTR(purchase_retail_currency_code, 1, 5) AS purchase_retail_currency_code,
  SUBSTR(purchase_cost_currency_code, 1, 5) AS purchase_cost_currency_code,
  SUBSTR(deal_income_purchases_currency_code, 1, 5) AS deal_income_purchases_currency_code,
  SUBSTR(freight_claim_retail_currency_code, 1, 5) AS freight_claim_retail_currency_code,
  SUBSTR(freight_claim_cost_currency_code, 1, 5) AS freight_claim_cost_currency_code,
  SUBSTR(freight_cost_currency_code, 1, 5) AS freight_cost_currency_code,
  SUBSTR(profit_up_charges_currency_code, 1, 5) AS profit_up_charges_currency_code,
  SUBSTR(expense_up_charges_currency_code, 1, 5) AS expense_up_charges_currency_code,
  SUBSTR(rtv_retail_currency_code, 1, 5) AS rtv_retail_currency_code,
  SUBSTR(rtv_cost_currency_code, 1, 5) AS rtv_cost_currency_code,
  SUBSTR(transfer_in_retail_currency_code, 1, 5) AS transfer_in_retail_currency_code,
  SUBSTR(transfer_in_cost_currency_code, 1, 5) AS transfer_in_cost_currency_code,
  SUBSTR(transfer_in_book_retail_currency_code, 1, 5) AS transfer_in_book_retail_currency_code,
  SUBSTR(transfer_in_book_cost_currency_code, 1, 5) AS transfer_in_book_cost_currency_code,
  SUBSTR(transfer_out_retail_currency_code, 1, 5) AS transfer_out_retail_currency_code,
  SUBSTR(transfer_out_cost_currency_code, 1, 5) AS transfer_out_cost_currency_code,
  SUBSTR(transfer_out_book_retail_currency_code, 1, 5) AS transfer_out_book_retail_currency_code,
  SUBSTR(transfer_out_book_cost_currency_code, 1, 5) AS transfer_out_book_cost_currency_code,
  SUBSTR(intercompany_in_retail_currency_code, 1, 5) AS intercompany_in_retail_currency_code,
  SUBSTR(intercompany_in_cost_currency_code, 1, 5) AS intercompany_in_cost_currency_code,
  SUBSTR(intercompany_out_retail_currency_code, 1, 5) AS intercompany_out_retail_currency_code,
  SUBSTR(intercompany_out_cost_currency_code, 1, 5) AS intercompany_out_cost_currency_code,
  SUBSTR(TRIM(intercompany_mark_up_currency_code), 1, 5) AS intercompany_mark_up_currency_code,
  SUBSTR(intercompany_mark_down_currency_code, 1, 5) AS intercompany_mark_down_currency_code,
  SUBSTR(intercompany_margin_currency_code, 1, 5) AS intercompany_margin_currency_code,
  CAST(TRUNC(CAST(sales_quantities AS FLOAT64)) AS INTEGER) AS sales_quantities,
  SUBSTR(net_sale_retail_currency_code, 1, 5) AS net_sale_retail_currency_code,
  SUBSTR(net_sales_cost_currency_code, 1, 5) AS net_sales_cost_currency_code,
  SUBSTR(net_sales_retail_excluding_vat_currency_code, 1, 5) AS net_sales_retail_excluding_vat_currency_code,
  SUBSTR(returns_retail_currency_code, 1, 5) AS returns_retail_currency_code,
  SUBSTR(returns_cost_currency_code, 1, 5) AS returns_cost_currency_code,
  SUBSTR(net_sales_non_inventory_retail_currency_code, 1, 5) AS net_sales_non_inventory_retail_currency_code,
  SUBSTR(net_sales_non_inventory_cost_currency_code, 1, 5) AS net_sales_non_inventory_cost_currency_code,
  SUBSTR(net_sales_non_inventory_retail_excluding_vat_currency_code, 1, 5) AS
  net_sales_non_inventory_retail_excluding_vat_currency_code,
  SUBSTR(markup_retail_currency_code, 1, 5) AS markup_retail_currency_code,
  SUBSTR(markup_cancelation_retail_currency_code, 1, 5) AS markup_cancelation_retail_currency_code,
  SUBSTR(clear_markdown_retail_currency_code, 1, 5) AS clear_markdown_retail_currency_code,
  SUBSTR(permanent_markdown_retail_currency_code, 1, 5) AS permanent_markdown_retail_currency_code,
  SUBSTR(promotion_markdown_retail_currency_code, 1, 5) AS promotion_markdown_retail_currency_code,
  SUBSTR(markdown_cancelation_retail_currency_code, 1, 5) AS markdown_cancelation_retail_currency_code,
  SUBSTR(franchise_returns_retail_currency_code, 1, 5) AS franchise_returns_retail_currency_code,
  SUBSTR(franchise_returns_cost_currency_code, 1, 5) AS franchise_returns_cost_currency_code,
  SUBSTR(franchise_sales_retail_currency_code, 1, 5) AS franchise_sales_retail_currency_code,
  SUBSTR(franchise_sales_cost_currency_code, 1, 5) AS franchise_sales_cost_currency_code,
  SUBSTR(franchise_markup_retail_currency_code, 1, 5) AS franchise_markup_retail_currency_code,
  SUBSTR(franchise_markdown_retail_currency_code, 1, 5) AS franchise_markdown_retail_currency_code,
  SUBSTR(employee_discount_retail_currency_code, 1, 5) AS employee_discount_retail_currency_code,
  SUBSTR(deal_income_sales_currency_code, 1, 5) AS deal_income_sales_currency_code,
  SUBSTR(workroom_amt_currency_code, 1, 5) AS workroom_amt_currency_code,
  SUBSTR(cash_discount_amt_currency_code, 1, 5) AS cash_discount_amt_currency_code,
  CAST(cumulative_markon_percent AS FLOAT64) AS cumulative_markon_percent,
  SUBSTR(shrinkage_retail_currency_code, 1, 5) AS shrinkage_retail_currency_code,
  SUBSTR(shrinkage_cost_currency_code, 1, 5) AS shrinkage_cost_currency_code,
  SUBSTR(reclass_in_retail_currency_code, 1, 5) AS reclass_in_retail_currency_code,
  SUBSTR(reclass_in_cost_currency_code, 1, 5) AS reclass_in_cost_currency_code,
  SUBSTR(reclass_out_retail_currency_code, 1, 5) AS reclass_out_retail_currency_code,
  SUBSTR(reclass_out_cost_currency_code, 1, 5) AS reclass_out_cost_currency_code,
  SUBSTR(gross_margin_amt_currency_code, 1, 5) AS gross_margin_amt_currency_code,
  SUBSTR(margin_cost_variance_currency_code, 1, 5) AS margin_cost_variance_currency_code,
  SUBSTR(retail_cost_variance_currency_code, 1, 5) AS retail_cost_variance_currency_code,
  SUBSTR(cost_variance_amt_currency_code, 1, 5) AS cost_variance_amt_currency_code,
  SUBSTR(half_to_date_doods_available_for_sale_retail_currency_code, 1, 5) AS
  half_to_date_doods_available_for_sale_retail_currency_code,
  SUBSTR(half_to_date_goods_available_for_sale_cost_currency_code, 1, 5) AS
  half_to_date_goods_available_for_sale_cost_currency_code,
  SUBSTR(receiver_cost_adjustment_variance_currency_code, 1, 5) AS receiver_cost_adjustment_variance_currency_code,
  SUBSTR(workorder_actvity_update_inventory_amt_currency_code, 1, 5) AS
  workorder_actvity_update_inventory_amt_currency_code,
  SUBSTR(workorder_actvity_post_to_finance_amt_currency_code, 1, 5) AS
  workorder_actvity_post_to_finance_amt_currency_code,
  SUBSTR(restocking_fee_currency_code, 1, 5) AS restocking_fee_currency_code,
  SUBSTR(franchise_restocking_fee_currency_code, 1, 5) AS franchise_restocking_fee_currency_code,
  SUBSTR(input_vat_amt_currency_code, 1, 5) AS input_vat_amt_currency_code,
  SUBSTR(output_vat_amt_currency_code, 1, 5) AS output_vat_amt_currency_code,
  SUBSTR(weight_variance_retail_currency_code, 1, 5) AS weight_variance_retail_currency_code,
  SUBSTR(recoverable_tax_amt_currency_code, 1, 5) AS recoverable_tax_amt_currency_code,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN stock_adjustment_retail_units = ''
       THEN '0'
       ELSE stock_adjustment_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN stock_adjustment_retail_nanos = ''
        THEN '0'
        ELSE stock_adjustment_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_stock_adjustment_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN stock_adjustment_cost_units = ''
       THEN '0'
       ELSE stock_adjustment_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN stock_adjustment_cost_nanos = ''
        THEN '0'
        ELSE stock_adjustment_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_stock_adjustment_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN stock_take_adjustment_retail_units = ''
       THEN '0'
       ELSE stock_take_adjustment_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN stock_take_adjustment_retail_nanos = ''
        THEN '0'
        ELSE stock_take_adjustment_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_stock_take_adjustment_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN stock_take_adjustment_cost_units = ''
       THEN '0'
       ELSE stock_take_adjustment_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN stock_take_adjustment_cost_nanos = ''
        THEN '0'
        ELSE stock_take_adjustment_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_stock_take_adjustment_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN stock_adjustment_cogs_retail_units = ''
       THEN '0'
       ELSE stock_adjustment_cogs_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN stock_adjustment_cogs_retail_nanos = ''
        THEN '0'
        ELSE stock_adjustment_cogs_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_stock_adjustment_cogs_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN stock_adjustment_cogs_cost_units = ''
       THEN '0'
       ELSE stock_adjustment_cogs_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN stock_adjustment_cogs_cost_nanos = ''
        THEN '0'
        ELSE stock_adjustment_cogs_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_stock_adjustment_cogs_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN purchase_retail_units = ''
       THEN '0'
       ELSE purchase_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN purchase_retail_nanos = ''
        THEN '0'
        ELSE purchase_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_purchase_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN purchase_cost_units = ''
       THEN '0'
       ELSE purchase_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN purchase_cost_nanos = ''
        THEN '0'
        ELSE purchase_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_purchase_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN deal_income_purchases_units = ''
       THEN '0'
       ELSE deal_income_purchases_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN deal_income_purchases_nanos = ''
        THEN '0'
        ELSE deal_income_purchases_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_deal_income_purchases_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN freight_claim_retail_units = ''
       THEN '0'
       ELSE freight_claim_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN freight_claim_retail_nanos = ''
        THEN '0'
        ELSE freight_claim_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_freight_claim_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN freight_claim_cost_units = ''
       THEN '0'
       ELSE freight_claim_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN freight_claim_cost_nanos = ''
        THEN '0'
        ELSE freight_claim_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_freight_claim_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN freight_cost_units = ''
       THEN '0'
       ELSE freight_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN freight_cost_nanos = ''
        THEN '0'
        ELSE freight_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_freight_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN profit_up_charges_units = ''
       THEN '0'
       ELSE profit_up_charges_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN profit_up_charges_nanos = ''
        THEN '0'
        ELSE profit_up_charges_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_profit_up_charges_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN expense_up_charges_units = ''
       THEN '0'
       ELSE expense_up_charges_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN expense_up_charges_nanos = ''
        THEN '0'
        ELSE expense_up_charges_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_expense_up_charges_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN rtv_retail_units = ''
       THEN '0'
       ELSE rtv_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN rtv_retail_nanos = ''
        THEN '0'
        ELSE rtv_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_rtv_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN rtv_cost_units = ''
       THEN '0'
       ELSE rtv_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN rtv_cost_nanos = ''
        THEN '0'
        ELSE rtv_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_rtv_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_in_retail_units = ''
       THEN '0'
       ELSE transfer_in_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_in_retail_nanos = ''
        THEN '0'
        ELSE transfer_in_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_in_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_in_cost_units = ''
       THEN '0'
       ELSE transfer_in_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_in_cost_nanos = ''
        THEN '0'
        ELSE transfer_in_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_in_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_in_book_retail_units = ''
       THEN '0'
       ELSE transfer_in_book_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_in_book_retail_nanos = ''
        THEN '0'
        ELSE transfer_in_book_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_in_book_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_in_book_cost_units = ''
       THEN '0'
       ELSE transfer_in_book_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_in_book_cost_nanos = ''
        THEN '0'
        ELSE transfer_in_book_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_in_book_cost_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_out_retail_units = ''
       THEN '0'
       ELSE transfer_out_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_out_retail_nanos = ''
        THEN '0'
        ELSE transfer_out_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_out_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_out_cost_units = ''
       THEN '0'
       ELSE transfer_out_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_out_cost_nanos = ''
        THEN '0'
        ELSE transfer_out_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_out_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_out_book_retail_units = ''
       THEN '0'
       ELSE transfer_out_book_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_out_book_retail_nanos = ''
        THEN '0'
        ELSE transfer_out_book_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_out_book_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN transfer_out_book_cost_units = ''
       THEN '0'
       ELSE transfer_out_book_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN transfer_out_book_cost_nanos = ''
        THEN '0'
        ELSE transfer_out_book_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_transfer_out_book_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN intercompany_in_retail_units = ''
       THEN '0'
       ELSE intercompany_in_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN intercompany_in_retail_nanos = ''
        THEN '0'
        ELSE intercompany_in_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_intercompany_in_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN intercompany_in_cost_units = ''
       THEN '0'
       ELSE intercompany_in_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN intercompany_in_cost_nanos = ''
        THEN '0'
        ELSE intercompany_in_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_intercompany_in_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN intercompany_out_retail_units = ''
       THEN '0'
       ELSE intercompany_out_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN intercompany_out_retail_nanos = ''
        THEN '0'
        ELSE intercompany_out_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_intercompany_out_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN intercompany_out_cost_units = ''
       THEN '0'
       ELSE intercompany_out_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN intercompany_out_cost_nanos = ''
        THEN '0'
        ELSE intercompany_out_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_intercompany_out_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN intercompany_mark_up_units = ''
       THEN '0'
       ELSE intercompany_mark_up_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN intercompany_mark_up_nanos = ''
        THEN '0'
        ELSE intercompany_mark_up_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_intercompany_mark_up_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN intercompany_mark_down_units = ''
       THEN '0'
       ELSE intercompany_mark_down_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN intercompany_mark_down_nanos = ''
        THEN '0'
        ELSE intercompany_mark_down_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_intercompany_mark_down_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN intercompany_margin_units = ''
       THEN '0'
       ELSE intercompany_margin_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN intercompany_margin_nanos = ''
        THEN '0'
        ELSE intercompany_margin_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_intercompany_margin_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN net_sale_retail_units = ''
       THEN '0'
       ELSE net_sale_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN net_sale_retail_nanos = ''
        THEN '0'
        ELSE net_sale_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_net_sale_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN net_sales_cost_units = ''
       THEN '0'
       ELSE net_sales_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN net_sales_cost_nanos = ''
        THEN '0'
        ELSE net_sales_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_net_sales_cost_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN net_sales_retail_excluding_vat_units = ''
       THEN '0'
       ELSE net_sales_retail_excluding_vat_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN net_sales_retail_excluding_vat_nanos = ''
        THEN '0'
        ELSE net_sales_retail_excluding_vat_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_net_sales_retail_excluding_vat_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN returns_retail_units = ''
       THEN '0'
       ELSE returns_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN returns_retail_nanos = ''
        THEN '0'
        ELSE returns_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_returns_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN returns_cost_units = ''
       THEN '0'
       ELSE returns_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN returns_cost_nanos = ''
        THEN '0'
        ELSE returns_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_returns_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN net_sales_non_inventory_retail_units = ''
       THEN '0'
       ELSE net_sales_non_inventory_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN net_sales_non_inventory_retail_nanos = ''
        THEN '0'
        ELSE net_sales_non_inventory_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_net_sales_non_inventory_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN net_sales_non_inventory_cost_units = ''
       THEN '0'
       ELSE net_sales_non_inventory_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN net_sales_non_inventory_cost_nanos = ''
        THEN '0'
        ELSE net_sales_non_inventory_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_net_sales_non_inventory_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN net_sales_non_inventory_retail_excluding_vat_units = ''
       THEN '0'
       ELSE net_sales_non_inventory_retail_excluding_vat_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN net_sales_non_inventory_retail_excluding_vat_nanos = ''
        THEN '0'
        ELSE net_sales_non_inventory_retail_excluding_vat_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_net_sales_non_inventory_retail_excluding_vat_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN markup_retail_units = ''
       THEN '0'
       ELSE markup_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN markup_retail_nanos = ''
        THEN '0'
        ELSE markup_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_markup_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN markup_cancelation_retail_units = ''
       THEN '0'
       ELSE markup_cancelation_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN markup_cancelation_retail_nanos = ''
        THEN '0'
        ELSE markup_cancelation_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_markup_cancelation_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN clear_markdown_retail_units = ''
       THEN '0'
       ELSE clear_markdown_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN clear_markdown_retail_nanos = ''
        THEN '0'
        ELSE clear_markdown_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_clear_markdown_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN permanent_markdown_retail_units = ''
       THEN '0'
       ELSE permanent_markdown_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN permanent_markdown_retail_nanos = ''
        THEN '0'
        ELSE permanent_markdown_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_permanent_markdown_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN promotion_markdown_retail_units = ''
       THEN '0'
       ELSE promotion_markdown_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN promotion_markdown_retail_nanos = ''
        THEN '0'
        ELSE promotion_markdown_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_promotion_markdown_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN markdown_cancelation_retail_units = ''
       THEN '0'
       ELSE markdown_cancelation_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN markdown_cancelation_retail_nanos = ''
        THEN '0'
        ELSE markdown_cancelation_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_markdown_cancelation_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN franchise_returns_retail_units = ''
       THEN '0'
       ELSE franchise_returns_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN franchise_returns_retail_nanos = ''
        THEN '0'
        ELSE franchise_returns_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_franchise_returns_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN franchise_returns_cost_units = ''
       THEN '0'
       ELSE franchise_returns_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN franchise_returns_cost_nanos = ''
        THEN '0'
        ELSE franchise_returns_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_franchise_returns_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN franchise_sales_retail_units = ''
       THEN '0'
       ELSE franchise_sales_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN franchise_sales_retail_nanos = ''
        THEN '0'
        ELSE franchise_sales_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_franchise_sales_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN franchise_sales_cost_units = ''
       THEN '0'
       ELSE franchise_sales_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN franchise_sales_cost_nanos = ''
        THEN '0'
        ELSE franchise_sales_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_franchise_sales_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN franchise_markup_retail_units = ''
       THEN '0'
       ELSE franchise_markup_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN franchise_markup_retail_nanos = ''
        THEN '0'
        ELSE franchise_markup_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_franchise_markup_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN franchise_markdown_retail_units = ''
       THEN '0'
       ELSE franchise_markdown_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN franchise_markdown_retail_nanos = ''
        THEN '0'
        ELSE franchise_markdown_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_franchise_markdown_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN employee_discount_retail_units = ''
       THEN '0'
       ELSE employee_discount_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN employee_discount_retail_nanos = ''
        THEN '0'
        ELSE employee_discount_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_employee_discount_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN deal_income_sales_units = ''
       THEN '0'
       ELSE deal_income_sales_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN deal_income_sales_nanos = ''
        THEN '0'
        ELSE deal_income_sales_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_deal_income_sales_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN workroom_amt_units = ''
       THEN '0'
       ELSE workroom_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN workroom_amt_nanos = ''
        THEN '0'
        ELSE workroom_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_workroom_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN cash_discount_amt_units = ''
       THEN '0'
       ELSE cash_discount_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN cash_discount_amt_nanos = ''
        THEN '0'
        ELSE cash_discount_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cash_discount_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN shrinkage_retail_units = ''
       THEN '0'
       ELSE shrinkage_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN shrinkage_retail_nanos = ''
        THEN '0'
        ELSE shrinkage_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_shrinkage_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN shrinkage_cost_units = ''
       THEN '0'
       ELSE shrinkage_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN shrinkage_cost_nanos = ''
        THEN '0'
        ELSE shrinkage_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_shrinkage_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN reclass_in_retail_units = ''
       THEN '0'
       ELSE reclass_in_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN reclass_in_retail_nanos = ''
        THEN '0'
        ELSE reclass_in_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_reclass_in_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN reclass_in_cost_units = ''
       THEN '0'
       ELSE reclass_in_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN reclass_in_cost_nanos = ''
        THEN '0'
        ELSE reclass_in_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_reclass_in_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN reclass_out_retail_units = ''
       THEN '0'
       ELSE reclass_out_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN reclass_out_retail_nanos = ''
        THEN '0'
        ELSE reclass_out_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_reclass_out_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN reclass_out_cost_units = ''
       THEN '0'
       ELSE reclass_out_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN reclass_out_cost_nanos = ''
        THEN '0'
        ELSE reclass_out_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_reclass_out_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN gross_margin_amt_units = ''
       THEN '0'
       ELSE gross_margin_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN gross_margin_amt_nanos = ''
        THEN '0'
        ELSE gross_margin_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_gross_margin_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN margin_cost_variance_units = ''
       THEN '0'
       ELSE margin_cost_variance_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN margin_cost_variance_nanos = ''
        THEN '0'
        ELSE margin_cost_variance_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_margin_cost_variance_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN retail_cost_variance_units = ''
       THEN '0'
       ELSE retail_cost_variance_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN retail_cost_variance_nanos = ''
        THEN '0'
        ELSE retail_cost_variance_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_retail_cost_variance_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN cost_variance_amt_units = ''
       THEN '0'
       ELSE cost_variance_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN cost_variance_amt_nanos = ''
        THEN '0'
        ELSE cost_variance_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_cost_variance_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN half_to_date_doods_available_for_sale_retail_units = ''
       THEN '0'
       ELSE half_to_date_doods_available_for_sale_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN half_to_date_doods_available_for_sale_retail_nanos = ''
        THEN '0'
        ELSE half_to_date_doods_available_for_sale_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_half_to_date_doods_available_for_sale_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN half_to_date_goods_available_for_sale_cost_units = ''
       THEN '0'
       ELSE half_to_date_goods_available_for_sale_cost_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN half_to_date_goods_available_for_sale_cost_nanos = ''
        THEN '0'
        ELSE half_to_date_goods_available_for_sale_cost_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_half_to_date_goods_available_for_sale_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN receiver_cost_adjustment_variance_units = ''
       THEN '0'
       ELSE receiver_cost_adjustment_variance_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN receiver_cost_adjustment_variance_nanos = ''
        THEN '0'
        ELSE receiver_cost_adjustment_variance_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_receiver_cost_adjustment_variance_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN workorder_actvity_update_inventory_amt_units = ''
       THEN '0'
       ELSE workorder_actvity_update_inventory_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN workorder_actvity_update_inventory_amt_nanos = ''
        THEN '0'
        ELSE workorder_actvity_update_inventory_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_workorder_actvity_update_inventory_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN workorder_actvity_post_to_finance_amt_units = ''
       THEN '0'
       ELSE workorder_actvity_post_to_finance_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN workorder_actvity_post_to_finance_amt_nanos = ''
        THEN '0'
        ELSE workorder_actvity_post_to_finance_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_workorder_actvity_post_to_finance_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN restocking_fee_units = ''
       THEN '0'
       ELSE restocking_fee_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN restocking_fee_nanos = ''
        THEN '0'
        ELSE restocking_fee_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_restocking_fee_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN franchise_restocking_fee_units = ''
       THEN '0'
       ELSE franchise_restocking_fee_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN franchise_restocking_fee_nanos = ''
        THEN '0'
        ELSE franchise_restocking_fee_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_franchise_restocking_fee_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN input_vat_amt_units = ''
       THEN '0'
       ELSE input_vat_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN input_vat_amt_nanos = ''
        THEN '0'
        ELSE input_vat_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_input_vat_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN output_vat_amt_units = ''
       THEN '0'
       ELSE output_vat_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN output_vat_amt_nanos = ''
        THEN '0'
        ELSE output_vat_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_output_vat_amt_cost,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN weight_variance_retail_units = ''
       THEN '0'
       ELSE weight_variance_retail_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN weight_variance_retail_nanos = ''
        THEN '0'
        ELSE weight_variance_retail_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_weight_variance_retail,
  ROUND(CAST(CAST(TRUNC(CAST(CASE
       WHEN recoverable_tax_amt_units = ''
       THEN '0'
       ELSE recoverable_tax_amt_units
       END AS FLOAT64)) AS INTEGER) + CAST(TRUNC(CAST(CASE
        WHEN recoverable_tax_amt_nanos = ''
        THEN '0'
        ELSE recoverable_tax_amt_nanos
        END AS FLOAT64)) AS INTEGER) / POWER(10, 9) AS NUMERIC), 9) AS total_recoverable_tax_amt_cost,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_updt_tmstp
 FROM {{params.gcp_project_id}}.{{params.database_name_staging}}.merch_stockledger_week_ldg
 WHERE LOWER(event_time) <> LOWER('event_time') 
 QUALIFY (ROW_NUMBER() OVER (PARTITION BY location_num, end_of_week_date, product_hierarchy_dept_num,
       product_hierarchy_class_num, product_hierarchy_subclass_num ORDER BY CAST(CASE
         WHEN CAST(last_updated_time_in_millis AS STRING) = ''
         THEN '0'
         ELSE CAST(last_updated_time_in_millis AS STRING)
         END AS BIGINT) DESC)) = 1);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;

CALL `{{params.gcp_project_id}}`.{{params.dbenv}}_NAP_UTL.DATA_TIMELINESS_METRIC_FACT_LD('MERCH_STOCKLEDGER_WEEK_FACT',  '{{params.database_name_fact}}',  'project_mfp_eowsl_kafka_to_td_tpt',  'job_6',  2,  'LOAD_END',  '',  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME),  '');

-- SET QUERY_BAND = NONE FOR SESSION;
END;
