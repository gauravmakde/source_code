/* SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_cost_country_banner_blend_week_fact_load;
Task_Name=country_banner_blend_data_load_country_banner_week_trueday_wrk_load;'
FOR SESSION VOLATILE;*/


-- Reload the data



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.mfp_cost_plan_actual_banner_country_wrk (dept_num, week_num, banner_country_num,
 fulfill_type_num, ly_trueday_beginning_of_period_active_qty, ly_trueday_beginning_of_period_active_cost_amt,
 ly_trueday_beginning_of_period_inactive_cost_amt, ly_trueday_beginning_of_period_inactive_qty,
 ly_trueday_ending_of_period_active_cost_amt, ly_trueday_ending_of_period_active_qty,
 ly_trueday_ending_of_period_inactive_cost_amt, ly_trueday_ending_of_period_inactive_qty,
 ly_trueday_transfer_in_active_cost_amt, ly_trueday_transfer_in_active_qty, ly_trueday_transfer_out_active_cost_amt,
 ly_trueday_transfer_out_active_qty, ly_trueday_transfer_in_inactive_cost_amt, ly_trueday_transfer_in_inactive_qty,
 ly_trueday_transfer_out_inactive_cost_amt, ly_trueday_transfer_out_inactive_qty, ly_trueday_transfer_in_other_cost_amt
 , ly_trueday_transfer_in_other_qty, ly_trueday_transfer_out_other_cost_amt, ly_trueday_transfer_out_other_qty,
 ly_trueday_transfer_in_racking_cost_amt, ly_trueday_transfer_in_racking_qty, ly_trueday_transfer_out_racking_cost_amt,
 ly_trueday_transfer_out_racking_qty, ly_trueday_mark_out_of_stock_active_cost_amt,
 ly_trueday_mark_out_of_stock_active_qty, ly_trueday_mark_out_of_stock_inactive_cost_amt,
 ly_trueday_mark_out_of_stock_inactive_qty, ly_trueday_receipts_active_cost_amt, ly_trueday_receipts_active_qty,
 ly_trueday_receipts_inactive_cost_amt, ly_trueday_receipts_inactive_qty, ly_trueday_rp_receipts_active_cost_amt,
 ly_trueday_non_rp_receipts_active_cost_amt, ly_trueday_reclass_in_cost_amt, ly_trueday_reclass_in_qty,
 ly_trueday_reclass_out_cost_amt, ly_trueday_reclass_out_qty, ly_trueday_return_to_vendor_active_cost_amt,
 ly_trueday_return_to_vendor_active_qty, ly_trueday_return_to_vendor_inactive_cost_amt,
 ly_trueday_return_to_vendor_inactive_qty, ly_trueday_vendor_funds_cost_amt, ly_trueday_discount_terms_cost_amt,
 ly_trueday_merch_margin_retail_amt, ly_trueday_shrink_banner_cost_amt, ly_trueday_shrink_banner_qty,
 ly_trueday_other_inventory_adj_cost, ly_trueday_merch_margin_charges_cost,
 lly_trueday_beginning_of_period_active_cost_amt, lly_trueday_beginning_of_period_active_qty,
 lly_trueday_beginning_of_period_inactive_cost_amt, lly_trueday_beginning_of_period_inactive_qty,
 lly_trueday_ending_of_period_active_cost_amt, lly_trueday_ending_of_period_active_qty,
 lly_trueday_ending_of_period_inactive_cost_amt, lly_trueday_ending_of_period_inactive_qty,
 lly_trueday_transfer_in_active_cost_amt, lly_trueday_transfer_in_active_qty, lly_trueday_transfer_out_active_cost_amt,
 lly_trueday_transfer_out_active_qty, lly_trueday_transfer_in_inactive_cost_amt, lly_trueday_transfer_in_inactive_qty,
 lly_trueday_transfer_out_inactive_cost_amt, lly_trueday_transfer_out_inactive_qty,
 lly_trueday_transfer_in_other_cost_amt, lly_trueday_transfer_in_other_qty, lly_trueday_transfer_out_other_cost_amt,
 lly_trueday_transfer_out_other_qty, lly_trueday_transfer_in_racking_cost_amt, lly_trueday_transfer_in_racking_qty,
 lly_trueday_transfer_out_racking_cost_amt, lly_trueday_transfer_out_racking_qty,
 lly_trueday_mark_out_of_stock_active_cost_amt, lly_trueday_mark_out_of_stock_active_qty,
 lly_trueday_mark_out_of_stock_inactive_cost_amt, lly_trueday_mark_out_of_stock_inactive_qty,
 lly_trueday_receipts_active_cost_amt, lly_trueday_receipts_active_qty, lly_trueday_receipts_inactive_cost_amt,
 lly_trueday_receipts_inactive_qty, lly_trueday_rp_receipts_active_cost_amt, lly_trueday_non_rp_receipts_active_cost_amt
 , lly_trueday_reclass_in_cost_amt, lly_trueday_reclass_in_qty, lly_trueday_reclass_out_cost_amt,
 lly_trueday_reclass_out_qty, lly_trueday_return_to_vendor_active_cost_amt, lly_trueday_return_to_vendor_active_qty,
 lly_trueday_return_to_vendor_inactive_cost_amt, lly_trueday_return_to_vendor_inactive_qty,
 lly_trueday_vendor_funds_cost_amt, lly_trueday_discount_terms_cost_amt, lly_trueday_merch_margin_retail_amt,
 lly_trueday_shrink_banner_cost_amt, lly_trueday_shrink_banner_qty, lly_trueday_other_inventory_adj_cost,
 lly_trueday_merch_margin_charges_cost, dw_batch_date, dw_sys_load_tmstp)
(SELECT a.dept_num,
  a.week_num,
  a.banner_country_num,
  a.fulfill_type_num,
  CAST(SUM(a.ly_trueday_beginning_of_period_active_qty) AS BIGINT) AS ly_trueday_beginning_of_period_active_qty,
  CAST(SUM(a.ly_trueday_beginning_of_period_active_cost_amt) AS NUMERIC) AS
  ly_trueday_beginning_of_period_active_cost_amt,
  CAST(SUM(a.ly_trueday_beginning_of_period_inactive_cost_amt) AS NUMERIC) AS
  ly_trueday_beginning_of_period_inactive_cost_amt,
  CAST(SUM(a.ly_trueday_beginning_of_period_inactive_qty) AS BIGINT) AS ly_trueday_beginning_of_period_inactive_qty,
  CAST(SUM(a.ly_trueday_ending_of_period_active_cost_amt) AS NUMERIC) AS ly_trueday_ending_of_period_active_cost_amt,
  CAST(SUM(a.ly_trueday_ending_of_period_active_qty) AS BIGINT) AS ly_trueday_ending_of_period_active_qty,
  CAST(SUM(a.ly_trueday_ending_of_period_inactive_cost_amt) AS NUMERIC) AS ly_trueday_ending_of_period_inactive_cost_amt,
  CAST(SUM(a.ly_trueday_ending_of_period_inactive_qty) AS BIGINT) AS ly_trueday_ending_of_period_inactive_qty,
  CAST(SUM(a.ly_trueday_transfer_in_active_cost_amt) AS NUMERIC) AS ly_trueday_transfer_in_active_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_in_active_qty) AS BIGINT) AS ly_trueday_transfer_in_active_qty,
  CAST(SUM(a.ly_trueday_transfer_out_active_cost_amt) AS NUMERIC) AS ly_trueday_transfer_out_active_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_out_active_qty) AS BIGINT) AS ly_trueday_transfer_out_active_qty,
  CAST(SUM(a.ly_trueday_transfer_in_inactive_cost_amt) AS NUMERIC) AS ly_trueday_transfer_in_inactive_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_in_inactive_qty) AS BIGINT) AS ly_trueday_transfer_in_inactive_qty,
  CAST(SUM(a.ly_trueday_transfer_out_inactive_cost_amt) AS NUMERIC) AS ly_trueday_transfer_out_inactive_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_out_inactive_qty) AS BIGINT) AS ly_trueday_transfer_out_inactive_qty,
  CAST(SUM(a.ly_trueday_transfer_in_other_cost_amt) AS NUMERIC) AS ly_trueday_transfer_in_other_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_in_other_qty) AS BIGINT) AS ly_trueday_transfer_in_other_qty,
  CAST(SUM(a.ly_trueday_transfer_out_other_cost_amt) AS NUMERIC) AS ly_trueday_transfer_out_other_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_out_other_qty) AS BIGINT) AS ly_trueday_transfer_out_other_qty,
  CAST(SUM(a.ly_trueday_transfer_in_racking_cost_amt) AS NUMERIC) AS ly_trueday_transfer_in_racking_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_in_racking_qty) AS BIGINT) AS ly_trueday_transfer_in_racking_qty,
  CAST(SUM(a.ly_trueday_transfer_out_racking_cost_amt) AS NUMERIC) AS ly_trueday_transfer_out_racking_cost_amt,
  CAST(SUM(a.ly_trueday_transfer_out_racking_qty) AS BIGINT) AS ly_trueday_transfer_out_racking_qty,
  CAST(SUM(a.ly_trueday_mark_out_of_stock_active_cost_amt) AS NUMERIC) AS ly_trueday_mark_out_of_stock_active_cost_amt,
  CAST(SUM(a.ly_trueday_mark_out_of_stock_active_qty) AS BIGINT) AS ly_trueday_mark_out_of_stock_active_qty,
  CAST(SUM(a.ly_trueday_mark_out_of_stock_inactive_cost_amt) AS NUMERIC) AS ly_trueday_mark_out_of_stock_inactive_cost_amt,
  CAST(SUM(a.ly_trueday_mark_out_of_stock_inactive_qty) AS BIGINT) AS ly_trueday_mark_out_of_stock_inactive_qty,
  CAST(SUM(a.ly_trueday_receipts_active_cost_amt) AS NUMERIC) AS ly_trueday_receipts_active_cost_amt,
  CAST(SUM(a.ly_trueday_receipts_active_qty) AS BIGINT) AS ly_trueday_receipts_active_qty,
  CAST(SUM(a.ly_trueday_receipts_inactive_cost_amt) AS NUMERIC) AS ly_trueday_receipts_inactive_cost_amt,
  CAST(SUM(a.ly_trueday_receipts_inactive_qty) AS BIGINT) AS ly_trueday_receipts_inactive_qty,
  SUM(a.ly_trueday_rp_receipts_active_cost_amt) AS ly_trueday_rp_receipts_active_cost_amt,
  SUM(a.ly_trueday_non_rp_receipts_active_cost_amt) AS ly_trueday_non_rp_receipts_active_cost_amt,
  CAST(SUM(a.ly_trueday_reclass_in_cost_amt) AS NUMERIC) AS ly_trueday_reclass_in_cost_amt,
  CAST(SUM(a.ly_trueday_reclass_in_qty) AS BIGINT) AS ly_trueday_reclass_in_qty,
  CAST(SUM(a.ly_trueday_reclass_out_cost_amt) AS NUMERIC) AS ly_trueday_reclass_out_cost_amt,
  CAST(SUM(a.ly_trueday_reclass_out_qty) AS BIGINT) AS ly_trueday_reclass_out_qty,
  CAST(SUM(a.ly_trueday_return_to_vendor_active_cost_amt) AS NUMERIC) AS ly_trueday_return_to_vendor_active_cost_amt,
  CAST(SUM(a.ly_trueday_return_to_vendor_active_qty) AS BIGINT) AS ly_trueday_return_to_vendor_active_qty,
  CAST(SUM(a.ly_trueday_return_to_vendor_inactive_cost_amt) AS NUMERIC) AS ly_trueday_return_to_vendor_inactive_cost_amt,
  CAST(SUM(a.ly_trueday_return_to_vendor_inactive_qty) AS BIGINT) AS ly_trueday_return_to_vendor_inactive_qty,
  CAST(SUM(a.ly_trueday_vendor_funds_cost_amt) AS NUMERIC) AS ly_trueday_vendor_funds_cost_amt,
  CAST(SUM(a.ly_trueday_discount_terms_cost_amt) AS NUMERIC) AS ly_trueday_discount_terms_cost_amt,
  CAST(SUM(a.ly_trueday_merch_margin_retail_amt) AS NUMERIC) AS ly_trueday_merch_margin_retail_amt,
  SUM(a.ly_trueday_shrink_banner_cost_amt) AS ly_trueday_shrink_banner_cost_amt,
  CAST(SUM(a.ly_trueday_shrink_banner_qty) AS BIGINT) AS ly_trueday_shrink_banner_qty,
  CAST(SUM(a.ly_trueday_other_inventory_adj_cost) AS NUMERIC) AS ly_trueday_other_inventory_adj_cost,
  SUM(a.ly_trueday_merch_margin_charges_cost) AS ly_trueday_merch_margin_charges_cost,
  SUM(a.lly_trueday_beginning_of_period_active_cost_amt) AS lly_trueday_beginning_of_period_active_cost_amt,
  SUM(a.lly_trueday_beginning_of_period_active_qty) AS lly_trueday_beginning_of_period_active_qty,
  SUM(a.lly_trueday_beginning_of_period_inactive_cost_amt) AS lly_trueday_beginning_of_period_inactive_cost_amt,
  SUM(a.lly_trueday_beginning_of_period_inactive_qty) AS lly_trueday_beginning_of_period_inactive_qty,
  SUM(a.lly_trueday_ending_of_period_active_cost_amt) AS lly_trueday_ending_of_period_active_cost_amt,
  SUM(a.lly_trueday_ending_of_period_active_qty) AS lly_trueday_ending_of_period_active_qty,
  SUM(a.lly_trueday_ending_of_period_inactive_cost_amt) AS lly_trueday_ending_of_period_inactive_cost_amt,
  SUM(a.lly_trueday_ending_of_period_inactive_qty) AS lly_trueday_ending_of_period_inactive_qty,
  SUM(a.lly_trueday_transfer_in_active_cost_amt) AS lly_trueday_transfer_in_active_cost_amt,
  SUM(a.lly_trueday_transfer_in_active_qty) AS lly_trueday_transfer_in_active_qty,
  SUM(a.lly_trueday_transfer_out_active_cost_amt) AS lly_trueday_transfer_out_active_cost_amt,
  SUM(a.lly_trueday_transfer_out_active_qty) AS lly_trueday_transfer_out_active_qty,
  SUM(a.lly_trueday_transfer_in_inactive_cost_amt) AS lly_trueday_transfer_in_inactive_cost_amt,
  SUM(a.lly_trueday_transfer_in_inactive_qty) AS lly_trueday_transfer_in_inactive_qty,
  SUM(a.lly_trueday_transfer_out_inactive_cost_amt) AS lly_trueday_transfer_out_inactive_cost_amt,
  SUM(a.lly_trueday_transfer_out_inactive_qty) AS lly_trueday_transfer_out_inactive_qty,
  SUM(a.lly_trueday_transfer_in_other_cost_amt) AS lly_trueday_transfer_in_other_cost_amt,
  SUM(a.lly_trueday_transfer_in_other_qty) AS lly_trueday_transfer_in_other_qty,
  SUM(a.lly_trueday_transfer_out_other_cost_amt) AS lly_trueday_transfer_out_other_cost_amt,
  SUM(a.lly_trueday_transfer_out_other_qty) AS lly_trueday_transfer_out_other_qty,
  SUM(a.lly_trueday_transfer_in_racking_cost_amt) AS lly_trueday_transfer_in_racking_cost_amt,
  SUM(a.lly_trueday_transfer_in_racking_qty) AS lly_trueday_transfer_in_racking_qty,
  SUM(a.lly_trueday_transfer_out_racking_cost_amt) AS lly_trueday_transfer_out_racking_cost_amt,
  SUM(a.lly_trueday_transfer_out_racking_qty) AS lly_trueday_transfer_out_racking_qty,
  SUM(a.lly_trueday_mark_out_of_stock_active_cost_amt) AS lly_trueday_mark_out_of_stock_active_cost_amt,
  SUM(a.lly_trueday_mark_out_of_stock_active_qty) AS lly_trueday_mark_out_of_stock_active_qty,
  SUM(a.lly_trueday_mark_out_of_stock_inactive_cost_amt) AS lly_trueday_mark_out_of_stock_inactive_cost_amt,
  SUM(a.lly_trueday_mark_out_of_stock_inactive_qty) AS lly_trueday_mark_out_of_stock_inactive_qty,
  SUM(a.lly_trueday_receipts_active_cost_amt) AS lly_trueday_receipts_active_cost_amt,
  SUM(a.lly_trueday_receipts_active_qty) AS lly_trueday_receipts_active_qty,
  SUM(a.lly_trueday_receipts_inactive_cost_amt) AS lly_trueday_receipts_inactive_cost_amt,
  SUM(a.lly_trueday_receipts_inactive_qty) AS lly_trueday_receipts_inactive_qty,
  SUM(a.lly_trueday_rp_receipts_active_cost_amt) AS lly_trueday_rp_receipts_active_cost_amt,
  SUM(a.lly_trueday_non_rp_receipts_active_cost_amt) AS lly_trueday_non_rp_receipts_active_cost_amt,
  SUM(a.lly_trueday_reclass_in_cost_amt) AS lly_trueday_reclass_in_cost_amt,
  SUM(a.lly_trueday_reclass_in_qty) AS lly_trueday_reclass_in_qty,
  SUM(a.lly_trueday_reclass_out_cost_amt) AS lly_trueday_reclass_out_cost_amt,
  SUM(a.lly_trueday_reclass_out_qty) AS lly_trueday_reclass_out_qty,
  SUM(a.lly_trueday_return_to_vendor_active_cost_amt) AS lly_trueday_return_to_vendor_active_cost_amt,
  SUM(a.lly_trueday_return_to_vendor_active_qty) AS lly_trueday_return_to_vendor_active_qty,
  SUM(a.lly_trueday_return_to_vendor_inactive_cost_amt) AS lly_trueday_return_to_vendor_inactive_cost_amt,
  SUM(a.lly_trueday_return_to_vendor_inactive_qty) AS lly_trueday_return_to_vendor_inactive_qty,
  SUM(a.lly_trueday_vendor_funds_cost_amt) AS lly_trueday_vendor_funds_cost_amt,
  SUM(a.lly_trueday_discount_terms_cost_amt) AS lly_trueday_discount_terms_cost_amt,
  SUM(a.lly_trueday_merch_margin_retail_amt) AS lly_trueday_merch_margin_retail_amt,
  SUM(a.lly_trueday_shrink_banner_cost_amt) AS lly_trueday_shrink_banner_cost_amt,
  CAST(SUM(a.lly_trueday_shrink_banner_qty) AS BIGINT) AS lly_trueday_shrink_banner_qty,
  SUM(a.lly_trueday_other_inventory_adj_cost) AS lly_trueday_other_inventory_adj_cost,
  SUM(a.lly_trueday_merch_margin_charges_cost) AS lly_trueday_merch_margin_charges_cost,
  MAX(a.dw_batch_dt) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', current_datetime('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT iifbv.dept_num,
     wcd.week_num,
     iifbv.banner_country_num,
     iifbv.fulfill_type_num,
     COALESCE(iifbv.cp_beginning_of_period_active_cost_amt, 0) AS ly_trueday_beginning_of_period_active_cost_amt,
     COALESCE(iifbv.cp_beginning_of_period_active_qty, 0) AS ly_trueday_beginning_of_period_active_qty,
     COALESCE(iifbv.cp_beginning_of_period_inactive_cost_amt, 0) AS ly_trueday_beginning_of_period_inactive_cost_amt,
     COALESCE(iifbv.cp_beginning_of_period_inactive_qty, 0) AS ly_trueday_beginning_of_period_inactive_qty,
     COALESCE(iifbv.cp_ending_of_period_active_cost_amt, 0) AS ly_trueday_ending_of_period_active_cost_amt,
     COALESCE(iifbv.cp_ending_of_period_active_qty, 0) AS ly_trueday_ending_of_period_active_qty,
     COALESCE(iifbv.cp_ending_of_period_inactive_cost_amt, 0) AS ly_trueday_ending_of_period_inactive_cost_amt,
     COALESCE(iifbv.cp_ending_of_period_inactive_qty, 0) AS ly_trueday_ending_of_period_inactive_qty,
     COALESCE(iifbv.cp_transfer_in_active_cost_amt, 0) AS ly_trueday_transfer_in_active_cost_amt,
     COALESCE(iifbv.cp_transfer_in_active_qty, 0) AS ly_trueday_transfer_in_active_qty,
     COALESCE(iifbv.cp_transfer_out_active_cost_amt, 0) AS ly_trueday_transfer_out_active_cost_amt,
     COALESCE(iifbv.cp_transfer_out_active_qty, 0) AS ly_trueday_transfer_out_active_qty,
     COALESCE(iifbv.cp_transfer_in_inactive_cost_amt, 0) AS ly_trueday_transfer_in_inactive_cost_amt,
     COALESCE(iifbv.cp_transfer_in_inactive_qty, 0) AS ly_trueday_transfer_in_inactive_qty,
     COALESCE(iifbv.cp_transfer_out_inactive_cost_amt, 0) AS ly_trueday_transfer_out_inactive_cost_amt,
     COALESCE(iifbv.cp_transfer_out_inactive_qty, 0) AS ly_trueday_transfer_out_inactive_qty,
     COALESCE(iifbv.cp_transfer_in_other_cost_amt, 0) AS ly_trueday_transfer_in_other_cost_amt,
     COALESCE(iifbv.cp_transfer_in_other_qty, 0) AS ly_trueday_transfer_in_other_qty,
     COALESCE(iifbv.cp_transfer_out_other_cost_amt, 0) AS ly_trueday_transfer_out_other_cost_amt,
     COALESCE(iifbv.cp_transfer_out_other_qty, 0) AS ly_trueday_transfer_out_other_qty,
     COALESCE(iifbv.cp_transfer_in_racking_cost_amt, 0) AS ly_trueday_transfer_in_racking_cost_amt,
     COALESCE(iifbv.cp_transfer_in_racking_qty, 0) AS ly_trueday_transfer_in_racking_qty,
     COALESCE(iifbv.cp_transfer_out_racking_cost_amt, 0) AS ly_trueday_transfer_out_racking_cost_amt,
     COALESCE(iifbv.cp_transfer_out_racking_qty, 0) AS ly_trueday_transfer_out_racking_qty,
     COALESCE(iifbv.cp_mark_out_of_stock_active_cost_amt, 0) AS ly_trueday_mark_out_of_stock_active_cost_amt,
     COALESCE(iifbv.cp_mark_out_of_stock_active_qty, 0) AS ly_trueday_mark_out_of_stock_active_qty,
     COALESCE(iifbv.cp_mark_out_of_stock_inactive_cost_amt, 0) AS ly_trueday_mark_out_of_stock_inactive_cost_amt,
     COALESCE(iifbv.cp_mark_out_of_stock_inactive_qty, 0) AS ly_trueday_mark_out_of_stock_inactive_qty,
     COALESCE(iifbv.cp_receipts_active_cost_amt, 0) AS ly_trueday_receipts_active_cost_amt,
     COALESCE(iifbv.cp_receipts_active_qty, 0) AS ly_trueday_receipts_active_qty,
     COALESCE(iifbv.cp_receipts_inactive_cost_amt, 0) AS ly_trueday_receipts_inactive_cost_amt,
     COALESCE(iifbv.cp_receipts_inactive_qty, 0) AS ly_trueday_receipts_inactive_qty,
     0 AS ly_trueday_rp_receipts_active_cost_amt,
     0 AS ly_trueday_non_rp_receipts_active_cost_amt,
     COALESCE(iifbv.cp_reclass_in_cost_amt, 0) AS ly_trueday_reclass_in_cost_amt,
     COALESCE(iifbv.cp_reclass_in_qty, 0) AS ly_trueday_reclass_in_qty,
     COALESCE(iifbv.cp_reclass_out_cost_amt, 0) AS ly_trueday_reclass_out_cost_amt,
     COALESCE(iifbv.cp_reclass_out_qty, 0) AS ly_trueday_reclass_out_qty,
     COALESCE(iifbv.cp_return_to_vendor_active_cost_amt, 0) AS ly_trueday_return_to_vendor_active_cost_amt,
     COALESCE(iifbv.cp_return_to_vendor_active_qty, 0) AS ly_trueday_return_to_vendor_active_qty,
     COALESCE(iifbv.cp_return_to_vendor_inactive_cost_amt, 0) AS ly_trueday_return_to_vendor_inactive_cost_amt,
     COALESCE(iifbv.cp_return_to_vendor_inactive_qty, 0) AS ly_trueday_return_to_vendor_inactive_qty,
     COALESCE(iifbv.cp_vendor_funds_cost_amt, 0) AS ly_trueday_vendor_funds_cost_amt,
     COALESCE(iifbv.cp_discount_terms_cost_amt, 0) AS ly_trueday_discount_terms_cost_amt,
     COALESCE(iifbv.cp_merch_margin_retail_amt, 0) AS ly_trueday_merch_margin_retail_amt,
     0 AS ly_trueday_shrink_banner_cost_amt,
     0 AS ly_trueday_shrink_banner_qty,
     COALESCE(iifbv.cp_other_inventory_adjustment_cost_amt, 0) AS ly_trueday_other_inventory_adj_cost,
     0 AS ly_trueday_merch_margin_charges_cost,
     0 AS lly_trueday_beginning_of_period_active_cost_amt,
     0 AS lly_trueday_beginning_of_period_active_qty,
     0 AS lly_trueday_beginning_of_period_inactive_cost_amt,
     0 AS lly_trueday_beginning_of_period_inactive_qty,
     0 AS lly_trueday_ending_of_period_active_cost_amt,
     0 AS lly_trueday_ending_of_period_active_qty,
     0 AS lly_trueday_ending_of_period_inactive_cost_amt,
     0 AS lly_trueday_ending_of_period_inactive_qty,
     0 AS lly_trueday_transfer_in_active_cost_amt,
     0 AS lly_trueday_transfer_in_active_qty,
     0 AS lly_trueday_transfer_out_active_cost_amt,
     0 AS lly_trueday_transfer_out_active_qty,
     0 AS lly_trueday_transfer_in_inactive_cost_amt,
     0 AS lly_trueday_transfer_in_inactive_qty,
     0 AS lly_trueday_transfer_out_inactive_cost_amt,
     0 AS lly_trueday_transfer_out_inactive_qty,
     0 AS lly_trueday_transfer_in_other_cost_amt,
     0 AS lly_trueday_transfer_in_other_qty,
     0 AS lly_trueday_transfer_out_other_cost_amt,
     0 AS lly_trueday_transfer_out_other_qty,
     0 AS lly_trueday_transfer_in_racking_cost_amt,
     0 AS lly_trueday_transfer_in_racking_qty,
     0 AS lly_trueday_transfer_out_racking_cost_amt,
     0 AS lly_trueday_transfer_out_racking_qty,
     0 AS lly_trueday_mark_out_of_stock_active_cost_amt,
     0 AS lly_trueday_mark_out_of_stock_active_qty,
     0 AS lly_trueday_mark_out_of_stock_inactive_cost_amt,
     0 AS lly_trueday_mark_out_of_stock_inactive_qty,
     0 AS lly_trueday_receipts_active_cost_amt,
     0 AS lly_trueday_receipts_active_qty,
     0 AS lly_trueday_receipts_inactive_cost_amt,
     0 AS lly_trueday_receipts_inactive_qty,
     0 AS lly_trueday_rp_receipts_active_cost_amt,
     0 AS lly_trueday_non_rp_receipts_active_cost_amt,
     0 AS lly_trueday_reclass_in_cost_amt,
     0 AS lly_trueday_reclass_in_qty,
     0 AS lly_trueday_reclass_out_cost_amt,
     0 AS lly_trueday_reclass_out_qty,
     0 AS lly_trueday_return_to_vendor_active_cost_amt,
     0 AS lly_trueday_return_to_vendor_active_qty,
     0 AS lly_trueday_return_to_vendor_inactive_cost_amt,
     0 AS lly_trueday_return_to_vendor_inactive_qty,
     0 AS lly_trueday_vendor_funds_cost_amt,
     0 AS lly_trueday_discount_terms_cost_amt,
     0 AS lly_trueday_merch_margin_retail_amt,
     0 AS lly_trueday_shrink_banner_cost_amt,
     0 AS lly_trueday_shrink_banner_qty,
     0 AS lly_trueday_other_inventory_adj_cost,
     0 AS lly_trueday_merch_margin_charges_cost,
     iifbv.dw_batch_dt
    FROM (SELECT mfcbpf.department_number AS dept_num,
       mfcbpf.week_id AS week_num,
       obcd.banner_country_num,
       aimd.fulfill_type_num,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN')) AND mfcbpf.week_id <> mtbv.current_fiscal_week_num
          
        THEN mfcbpf.active_beginning_inventory_dollar_amount
        ELSE 0
        END AS cp_beginning_of_period_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN')) AND mfcbpf.week_id <> mtbv.current_fiscal_week_num
          
        THEN mfcbpf.active_beginning_inventory_units
        ELSE 0
        END AS cp_beginning_of_period_active_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN')) AND mfcbpf.week_id <> mtbv.current_fiscal_week_num
          
        THEN mfcbpf.inactive_beginning_period_inventory_dollar_amount
        ELSE 0
        END AS cp_beginning_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN')) AND mfcbpf.week_id <> mtbv.current_fiscal_week_num
          
        THEN mfcbpf.inactive_beginning_period_inventory_units
        ELSE 0
        END AS cp_beginning_of_period_inactive_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_beginning_period_inventory_target_units
        ELSE 0
        END AS cp_beginning_of_period_active_inventory_target_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_ending_inventory_dollar_amount
        ELSE 0
        END AS cp_ending_of_period_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_ending_inventory_units
        ELSE 0
        END AS cp_ending_of_period_active_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_ending_inventory_dollar_amount
        ELSE 0
        END AS cp_ending_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_ending_inventory_units
        ELSE 0
        END AS cp_ending_of_period_inactive_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_inventory_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_inventory_in_units
        ELSE 0
        END AS cp_transfer_in_active_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_inventory_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_inventory_out_units
        ELSE 0
        END AS cp_transfer_out_active_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_inventory_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_inactive_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_inventory_in_units
        ELSE 0
        END AS cp_transfer_in_inactive_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_inventory_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_inactive_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_inventory_out_units
        ELSE 0
        END AS cp_transfer_out_inactive_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.other_inventory_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_other_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.other_inventory_in_units
        ELSE 0
        END AS cp_transfer_in_other_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.other_inventory_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_other_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.other_inventory_out_units
        ELSE 0
        END AS cp_transfer_out_other_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.racking_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_racking_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.racking_in_units
        ELSE 0
        END AS cp_transfer_in_racking_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.racking_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_racking_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.racking_out_units
        ELSE 0
        END AS cp_transfer_out_racking_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_mos_dollar_amount
        ELSE 0
        END AS cp_mark_out_of_stock_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_mos_units
        ELSE 0
        END AS cp_mark_out_of_stock_active_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_mos_dollar_amount
        ELSE 0
        END AS cp_mark_out_of_stock_inactive_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_mos_units
        ELSE 0
        END AS cp_mark_out_of_stock_inactive_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_receipts_dollar_amount
        ELSE 0
        END AS cp_receipts_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_receipts_units
        ELSE 0
        END AS cp_receipts_active_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_receipts_dollar_amount
        ELSE 0
        END AS cp_receipts_inactive_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_receipts_units
        ELSE 0
        END AS cp_receipts_inactive_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.reserve_receipts_dollar_amount
        ELSE 0
        END AS cp_receipts_reserve_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.reserve_receipts_units
        ELSE 0
        END AS cp_receipts_reserve_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.reclass_in_dollar_amount
        ELSE 0
        END AS cp_reclass_in_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.reclass_in_units
        ELSE 0
        END AS cp_reclass_in_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.reclass_out_dollar_amount
        ELSE 0
        END AS cp_reclass_out_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.reclass_out_units
        ELSE 0
        END AS cp_reclass_out_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_rtv_dollar_amount
        ELSE 0
        END AS cp_return_to_vendor_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_rtv_units
        ELSE 0
        END AS cp_return_to_vendor_active_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_rtv_dollar_amount
        ELSE 0
        END AS cp_return_to_vendor_inactive_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_rtv_units
        ELSE 0
        END AS cp_return_to_vendor_inactive_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.vendor_funds_cost_amount
        ELSE 0
        END AS cp_vendor_funds_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.discount_terms_cost_amount
        ELSE 0
        END AS cp_discount_terms_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.merch_margin_retail_amount
        ELSE 0
        END AS cp_merch_margin_retail_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inventory_movement_drop_ship_in_dollar_amount
        ELSE 0
        END AS cp_inventory_movement_in_dropship_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inventory_movement_drop_ship_in_units
        ELSE 0
        END AS cp_inventory_movement_in_dropship_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inventory_movement_drop_ship_out_dollar_amount
        ELSE 0
        END AS cp_inventory_movement_out_dropship_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inventory_movement_drop_ship_out_units
        ELSE 0
        END AS cp_inventory_movement_out_dropship_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.other_inventory_adjustments_dollar_amount
        ELSE 0
        END AS cp_other_inventory_adjustment_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.other_inventory_adjustments_units
        ELSE 0
        END AS cp_other_inventory_adjustment_qty,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.active_opentobuy_cost_amount
        ELSE 0
        END AS cp_open_to_buy_receipts_active_cost_amt,
        CASE
        WHEN LOWER(RTRIM(mfcbpf.financial_plan_version)) = LOWER(RTRIM('CURRENT_PLAN'))
        THEN mfcbpf.inactive_opentobuy_cost_amount
        ELSE 0
        END AS cp_open_to_buy_receipts_inactive_cost_amt,
       mtbv.dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_country_banner_plan_fct AS mfcbpf
       LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.org_banner_country_dim AS obcd ON LOWER(RTRIM(mfcbpf.channel_country)) = LOWER(RTRIM(obcd.country_code
           )) AND LOWER(RTRIM(mfcbpf.pricing_channel)) = LOWER(RTRIM(obcd.banner_desc))
       LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.alternate_inventory_model_dim AS aimd ON LOWER(RTRIM(mfcbpf.alternate_inventory_model
          )) = LOWER(RTRIM(aimd.fulfill_type_desc))
       INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv ON LOWER(RTRIM(mtbv.interface_code)) = LOWER(RTRIM('MFP_BANR_BLEND_WKLY'
          ))) AS iifbv
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd ON wcd.td_ly_week_num <> 444444 AND iifbv.week_num = wcd.td_ly_week_num
     WHERE wcd.TD_LY_WEEK_NUM  > (SELECT CASE WHEN DAY_NUM_OF_FISCAL_WEEK = 7
                        THEN l01.CURRENT_FISCAL_WEEK_NUM
                        ELSE l01.LAST_COMPLETED_FISCAL_WEEK_NUM
                        END
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw l01 
                    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim d01
                    ON l01.DW_BATCH_DT = d01.DAY_DATE
                    WHERE LOWER(l01.INTERFACE_CODE) = LOWER('MFP_BANR_BLEND_WKLY'))
       
    UNION ALL
    SELECT COALESCE(iifbv.department_num, iif.department_num, mtlif.department_num, pif.department_num, mrif.department_num
      , rtvif.department_num, vfmdt.department_num, mmif.department_num) AS dept_num,
     wcd0.week_num,
     COALESCE(iifbv.banner_num, iif.banner_num, mtlif.banner_num, pif.banner_num, mrif.banner_num, rtvif.banner_num,
      vfmdt.banner_num, mmif.banner_num) AS banner_country_num,
     COALESCE(iifbv.fulfilment_num, iif.fulfilment_num, mtlif.fulfilment_num, pif.fulfilment_num, mrif.fulfilment_num,
      rtvif.fulfilment_num, vfmdt.fulfilment_num, mmif.fulfilment_num) AS fulfill_type_num,
     COALESCE(iifbv.stock_on_hand_active_cost, 0) AS ly_trueday_beginning_of_period_active_cost_amt,
     COALESCE(iifbv.stock_on_hand_active_units, 0) AS ly_trueday_beginning_of_period_active_qty,
     COALESCE(iifbv.stock_on_hand_inactive_cost, 0) AS ly_trueday_beginning_of_period_inactive_cost_amt,
     COALESCE(iifbv.stock_on_hand_inactive_units, 0) AS ly_trueday_beginning_of_period_inactive_qty,
     COALESCE(iif.stock_on_hand_active_cost, 0) AS ly_trueday_ending_of_period_active_cost_amt,
     COALESCE(iif.stock_on_hand_active_units, 0) AS ly_trueday_ending_of_period_active_qty,
     COALESCE(iif.stock_on_hand_inactive_cost, 0) AS ly_trueday_ending_of_period_inactive_cost_amt,
     COALESCE(iif.stock_on_hand_inactive_units, 0) AS ly_trueday_ending_of_period_inactive_qty,
     COALESCE(mtlif.active_transfer_in_c, 0) AS ly_trueday_transfer_in_active_cost_amt,
     COALESCE(mtlif.active_transfer_in_u, 0) AS ly_trueday_transfer_in_active_qty,
     COALESCE(mtlif.active_transfer_out_c, 0) AS ly_trueday_transfer_out_active_cost_amt,
     COALESCE(mtlif.active_transfer_out_u, 0) AS ly_trueday_transfer_out_active_qty,
     COALESCE(mtlif.inactive_transfer_in_c, 0) AS ly_trueday_transfer_in_inactive_cost_amt,
     COALESCE(mtlif.inactive_transfer_in_u, 0) AS ly_trueday_transfer_in_inactive_qty,
     COALESCE(mtlif.inactive_transfer_out_c, 0) AS ly_trueday_transfer_out_inactive_cost_amt,
     COALESCE(mtlif.inactive_transfer_out_u, 0) AS ly_trueday_transfer_out_inactive_qty,
     COALESCE(mtlif.other_transfer_in_c, 0) AS ly_trueday_transfer_in_other_cost_amt,
     COALESCE(mtlif.other_transfer_in_u, 0) AS ly_trueday_transfer_in_other_qty,
     COALESCE(mtlif.other_transfer_out_c, 0) AS ly_trueday_transfer_out_other_cost_amt,
     COALESCE(mtlif.other_transfer_out_u, 0) AS ly_trueday_transfer_out_other_qty,
     COALESCE(mtlif.racking_transfer_in_c, 0) AS ly_trueday_transfer_in_racking_cost_amt,
     COALESCE(mtlif.racking_transfer_in_u, 0) AS ly_trueday_transfer_in_racking_qty,
     COALESCE(mtlif.racking_transfer_out_c, 0) AS ly_trueday_transfer_out_racking_cost_amt,
     COALESCE(mtlif.racking_transfer_out_u, 0) AS ly_trueday_transfer_out_racking_qty,
     COALESCE(mmif.mos_active_cost, 0) AS ly_trueday_mark_out_of_stock_active_cost_amt,
     COALESCE(mmif.mos_active_units, 0) AS ly_trueday_mark_out_of_stock_active_qty,
     COALESCE(mmif.mos_inactive_cost, 0) AS ly_trueday_mark_out_of_stock_inactive_cost_amt,
     COALESCE(mmif.mos_inactive_units, 0) AS ly_trueday_mark_out_of_stock_inactive_qty,
     COALESCE(pif.receipts_active_cost, 0) AS ly_trueday_receipts_active_cost_amt,
     COALESCE(pif.receipts_active_units, 0) AS ly_trueday_receipts_active_qty,
     COALESCE(pif.receipts_inactive_cost, 0) AS ly_trueday_receipts_inactive_cost_amt,
     COALESCE(pif.receipts_inactive_units, 0) AS ly_trueday_receipts_inactive_qty,
     COALESCE(pif.rp_receipts_active_cost, 0) AS ly_trueday_rp_receipts_active_cost_amt,
     COALESCE(pif.non_rp_receipts_active_cost, 0) AS ly_trueday_non_rp_receipts_active_cost_amt,
     COALESCE(mrif.reclass_in_cost, 0) AS ly_trueday_reclass_in_cost_amt,
     COALESCE(mrif.reclass_in_units, 0) AS ly_trueday_reclass_in_qty,
     COALESCE(mrif.reclass_out_cost, 0) AS ly_trueday_reclass_out_cost_amt,
     COALESCE(mrif.reclass_out_units, 0) AS ly_trueday_reclass_out_qty,
     COALESCE(rtvif.work_plan_rtv_active_cost, 0) AS ly_trueday_return_to_vendor_active_cost_amt,
     COALESCE(rtvif.work_plan_rtv_active_units, 0) AS ly_trueday_return_to_vendor_active_qty,
     COALESCE(rtvif.work_plan_rtv_inactive_cost, 0) AS ly_trueday_return_to_vendor_inactive_cost_amt,
     COALESCE(rtvif.work_plan_rtv_inactive_units, 0) AS ly_trueday_return_to_vendor_inactive_qty,
     COALESCE(vfmdt.vendor_funds_cost, 0) AS ly_trueday_vendor_funds_cost_amt,
     COALESCE(vfmdt.disc_terms_cost, 0) AS ly_trueday_discount_terms_cost_amt,
     COALESCE(mmif.merch_margin_retail, 0) AS ly_trueday_merch_margin_retail_amt,
     COALESCE(mmif.shrink_cost, 0) AS ly_trueday_shrink_banner_cost_amt,
     COALESCE(mmif.shrink_units, 0) AS ly_trueday_shrink_banner_qty,
     COALESCE(mmif.other_inventory_adj_cost, 0) AS ly_trueday_other_inventory_adj_cost,
     COALESCE(mmif.merch_margin_charges_cost, 0) AS ly_trueday_merch_margin_charges_cost,
     0 AS lly_trueday_beginning_of_period_active_cost_amt,
     0 AS lly_trueday_beginning_of_period_active_qty,
     0 AS lly_trueday_beginning_of_period_inactive_cost_amt,
     0 AS lly_trueday_beginning_of_period_inactive_qty,
     0 AS lly_trueday_ending_of_period_active_cost_amt,
     0 AS lly_trueday_ending_of_period_active_qty,
     0 AS lly_trueday_ending_of_period_inactive_cost_amt,
     0 AS lly_trueday_ending_of_period_inactive_qty,
     0 AS lly_trueday_transfer_in_active_cost_amt,
     0 AS lly_trueday_transfer_in_active_qty,
     0 AS lly_trueday_transfer_out_active_cost_amt,
     0 AS lly_trueday_transfer_out_active_qty,
     0 AS lly_trueday_transfer_in_inactive_cost_amt,
     0 AS lly_trueday_transfer_in_inactive_qty,
     0 AS lly_trueday_transfer_out_inactive_cost_amt,
     0 AS lly_trueday_transfer_out_inactive_qty,
     0 AS lly_trueday_transfer_in_other_cost_amt,
     0 AS lly_trueday_transfer_in_other_qty,
     0 AS lly_trueday_transfer_out_other_cost_amt,
     0 AS lly_trueday_transfer_out_other_qty,
     0 AS lly_trueday_transfer_in_racking_cost_amt,
     0 AS lly_trueday_transfer_in_racking_qty,
     0 AS lly_trueday_transfer_out_racking_cost_amt,
     0 AS lly_trueday_transfer_out_racking_qty,
     0 AS lly_trueday_mark_out_of_stock_active_cost_amt,
     0 AS lly_trueday_mark_out_of_stock_active_qty,
     0 AS lly_trueday_mark_out_of_stock_inactive_cost_amt,
     0 AS lly_trueday_mark_out_of_stock_inactive_qty,
     0 AS lly_trueday_receipts_active_cost_amt,
     0 AS lly_trueday_receipts_active_qty,
     0 AS lly_trueday_receipts_inactive_cost_amt,
     0 AS lly_trueday_receipts_inactive_qty,
     0 AS lly_trueday_rp_receipts_active_cost_amt,
     0 AS lly_trueday_non_rp_receipts_active_cost_amt,
     0 AS lly_trueday_reclass_in_cost_amt,
     0 AS lly_trueday_reclass_in_qty,
     0 AS lly_trueday_reclass_out_cost_amt,
     0 AS lly_trueday_reclass_out_qty,
     0 AS lly_trueday_return_to_vendor_active_cost_amt,
     0 AS lly_trueday_return_to_vendor_active_qty,
     0 AS lly_trueday_return_to_vendor_inactive_cost_amt,
     0 AS lly_trueday_return_to_vendor_inactive_qty,
     0 AS lly_trueday_vendor_funds_cost_amt,
     0 AS lly_trueday_discount_terms_cost_amt,
     0 AS lly_trueday_merch_margin_retail_amt,
     0 AS lly_trueday_shrink_banner_cost_amt,
     0 AS lly_trueday_shrink_banner_qty,
     0 AS lly_trueday_other_inventory_adj_cost,
     0 AS lly_trueday_merch_margin_charges_cost,
     mtbv0.dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_inventory_bop_insight_fact_vw AS iifbv
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_insight_fact AS iif ON iifbv.current_week_num = iif.week_num AND iifbv.department_num
          = iif.department_num AND iifbv.banner_num = iif.banner_num AND iifbv.fulfilment_num = iif.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_transfer_ledger_insight_fact AS mtlif ON iifbv.current_week_num = mtlif.week_num AND
         iifbv.department_num = mtlif.department_num AND iifbv.banner_num = mtlif.banner_num AND iifbv.fulfilment_num =
       mtlif.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.poreceipts_insight_fact AS pif ON iifbv.current_week_num = pif.week_num AND iifbv.department_num
          = pif.department_num AND iifbv.banner_num = pif.banner_num AND iifbv.fulfilment_num = pif.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_reclass_insight_fact AS mrif ON iifbv.current_week_num = mrif.week_num AND iifbv.department_num
          = mrif.department_num AND iifbv.banner_num = mrif.banner_num AND iifbv.fulfilment_num = mrif.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.return_to_vendor_insight_fact AS rtvif ON iifbv.current_week_num = rtvif.week_num AND iifbv.department_num
          = rtvif.department_num AND iifbv.banner_num = rtvif.banner_num AND iifbv.fulfilment_num = rtvif.fulfilment_num
       
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.vfm_discterms_ledger_insight_fact AS vfmdt ON iifbv.current_week_num = vfmdt.week_num AND
         iifbv.department_num = vfmdt.department_num AND iifbv.banner_num = vfmdt.banner_num AND iifbv.fulfilment_num =
       vfmdt.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_margin_insight_fact AS mmif ON iifbv.current_week_num = mmif.week_num AND iifbv.department_num
          = mmif.department_num AND iifbv.banner_num = mmif.banner_num AND iifbv.fulfilment_num = mmif.fulfilment_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd0 ON wcd0.td_ly_week_num <> 444444 AND (iifbv.current_week_num = wcd0
               .td_ly_week_num OR mmif.week_num = wcd0.td_ly_week_num OR vfmdt.week_num = wcd0.td_ly_week_num OR rtvif.week_num
              = wcd0.td_ly_week_num OR mrif.week_num = wcd0.td_ly_week_num OR pif.week_num = wcd0.td_ly_week_num OR
          mtlif.week_num = wcd0.td_ly_week_num OR iif.week_num = wcd0.td_ly_week_num)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv0 ON LOWER(RTRIM(mtbv0.interface_code)) = LOWER(RTRIM('MFP_BANR_BLEND_WKLY'
        ))
    UNION ALL
    SELECT COALESCE(iifbv0.department_num, iif0.department_num, mtlif0.department_num, pif0.department_num, mrif0.department_num
      , rtvif0.department_num, vfmdt0.department_num, mmif0.department_num) AS dept_num,
     wcd1.week_num,
     COALESCE(iifbv0.banner_num, iif0.banner_num, mtlif0.banner_num, pif0.banner_num, mrif0.banner_num, rtvif0.banner_num
      , vfmdt0.banner_num, mmif0.banner_num) AS banner_country_num,
     COALESCE(iifbv0.fulfilment_num, iif0.fulfilment_num, mtlif0.fulfilment_num, pif0.fulfilment_num, mrif0.fulfilment_num
      , rtvif0.fulfilment_num, vfmdt0.fulfilment_num, mmif0.fulfilment_num) AS fulfill_type_num,
     0 AS ly_trueday_beginning_of_period_active_cost_amt,
     0 AS ly_trueday_beginning_of_period_active_qty,
     0 AS ly_trueday_beginning_of_period_inactive_cost_amt,
     0 AS ly_trueday_beginning_of_period_inactive_qty,
     0 AS ly_trueday_ending_of_period_active_cost_amt,
     0 AS ly_trueday_ending_of_period_active_qty,
     0 AS ly_trueday_ending_of_period_inactive_cost_amt,
     0 AS ly_trueday_ending_of_period_inactive_qty,
     0 AS ly_trueday_transfer_in_active_cost_amt,
     0 AS ly_trueday_transfer_in_active_qty,
     0 AS ly_trueday_transfer_out_active_cost_amt,
     0 AS ly_trueday_transfer_out_active_qty,
     0 AS ly_trueday_transfer_in_inactive_cost_amt,
     0 AS ly_trueday_transfer_in_inactive_qty,
     0 AS ly_trueday_transfer_out_inactive_cost_amt,
     0 AS ly_trueday_transfer_out_inactive_qty,
     0 AS ly_trueday_transfer_in_other_cost_amt,
     0 AS ly_trueday_transfer_in_other_qty,
     0 AS ly_trueday_transfer_out_other_cost_amt,
     0 AS ly_trueday_transfer_out_other_qty,
     0 AS ly_trueday_transfer_in_racking_cost_amt,
     0 AS ly_trueday_transfer_in_racking_qty,
     0 AS ly_trueday_transfer_out_racking_cost_amt,
     0 AS ly_trueday_transfer_out_racking_qty,
     0 AS ly_trueday_mark_out_of_stock_active_cost_amt,
     0 AS ly_trueday_mark_out_of_stock_active_qty,
     0 AS ly_trueday_mark_out_of_stock_inactive_cost_amt,
     0 AS ly_trueday_mark_out_of_stock_inactive_qty,
     0 AS ly_trueday_receipts_active_cost_amt,
     0 AS ly_trueday_receipts_active_qty,
     0 AS ly_trueday_receipts_inactive_cost_amt,
     0 AS ly_trueday_receipts_inactive_qty,
     0 AS ly_trueday_rp_receipts_active_cost_amt,
     0 AS ly_trueday_non_rp_receipts_active_cost_amt,
     0 AS ly_trueday_reclass_in_cost_amt,
     0 AS ly_trueday_reclass_in_qty,
     0 AS ly_trueday_reclass_out_cost_amt,
     0 AS ly_trueday_reclass_out_qty,
     0 AS ly_trueday_return_to_vendor_active_cost_amt,
     0 AS ly_trueday_return_to_vendor_active_qty,
     0 AS ly_trueday_return_to_vendor_inactive_cost_amt,
     0 AS ly_trueday_return_to_vendor_inactive_qty,
     0 AS ly_trueday_vendor_funds_cost_amt,
     0 AS ly_trueday_discount_terms_cost_amt,
     0 AS ly_trueday_merch_margin_retail_amt,
     0 AS ly_trueday_shrink_banner_cost_amt,
     0 AS ly_trueday_shrink_banner_qty,
     0 AS ly_trueday_other_inventory_adj_cost,
     0 AS ly_trueday_merch_margin_charges_cost,
     COALESCE(iifbv0.stock_on_hand_active_cost, 0) AS lly_trueday_beginning_of_period_active_cost_amt,
     COALESCE(iifbv0.stock_on_hand_active_units, 0) AS lly_trueday_beginning_of_period_active_qty,
     COALESCE(iifbv0.stock_on_hand_inactive_cost, 0) AS lly_trueday_beginning_of_period_inactive_cost_amt,
     COALESCE(iifbv0.stock_on_hand_inactive_units, 0) AS lly_trueday_beginning_of_period_inactive_qty,
     COALESCE(iif0.stock_on_hand_active_cost, 0) AS lly_trueday_ending_of_period_active_cost_amt,
     COALESCE(iif0.stock_on_hand_active_units, 0) AS lly_trueday_ending_of_period_active_qty,
     COALESCE(iif0.stock_on_hand_inactive_cost, 0) AS lly_trueday_ending_of_period_inactive_cost_amt,
     COALESCE(iif0.stock_on_hand_inactive_units, 0) AS lly_trueday_ending_of_period_inactive_qty,
     COALESCE(mtlif0.active_transfer_in_c, 0) AS lly_trueday_transfer_in_active_cost_amt,
     COALESCE(mtlif0.active_transfer_in_u, 0) AS lly_trueday_transfer_in_active_qty,
     COALESCE(mtlif0.active_transfer_out_c, 0) AS lly_trueday_transfer_out_active_cost_amt,
     COALESCE(mtlif0.active_transfer_out_u, 0) AS lly_trueday_transfer_out_active_qty,
     COALESCE(mtlif0.inactive_transfer_in_c, 0) AS lly_trueday_transfer_in_inactive_cost_amt,
     COALESCE(mtlif0.inactive_transfer_in_u, 0) AS lly_trueday_transfer_in_inactive_qty,
     COALESCE(mtlif0.inactive_transfer_out_c, 0) AS lly_trueday_transfer_out_inactive_cost_amt,
     COALESCE(mtlif0.inactive_transfer_out_u, 0) AS lly_trueday_transfer_out_inactive_qty,
     COALESCE(mtlif0.other_transfer_in_c, 0) AS lly_trueday_transfer_in_other_cost_amt,
     COALESCE(mtlif0.other_transfer_in_u, 0) AS lly_trueday_transfer_in_other_qty,
     COALESCE(mtlif0.other_transfer_out_c, 0) AS lly_trueday_transfer_out_other_cost_amt,
     COALESCE(mtlif0.other_transfer_out_u, 0) AS lly_trueday_transfer_out_other_qty,
     COALESCE(mtlif0.racking_transfer_in_c, 0) AS lly_trueday_transfer_in_racking_cost_amt,
     COALESCE(mtlif0.racking_transfer_in_u, 0) AS lly_trueday_transfer_in_racking_qty,
     COALESCE(mtlif0.racking_transfer_out_c, 0) AS lly_trueday_transfer_out_racking_cost_amt,
     COALESCE(mtlif0.racking_transfer_out_u, 0) AS lly_trueday_transfer_out_racking_qty,
     COALESCE(mmif0.mos_active_cost, 0) AS lly_trueday_mark_out_of_stock_active_cost_amt,
     COALESCE(mmif0.mos_active_units, 0) AS lly_trueday_mark_out_of_stock_active_qty,
     COALESCE(mmif0.mos_inactive_cost, 0) AS lly_trueday_mark_out_of_stock_inactive_cost_amt,
     COALESCE(mmif0.mos_inactive_units, 0) AS lly_trueday_mark_out_of_stock_inactive_qty,
     COALESCE(pif0.receipts_active_cost, 0) AS lly_trueday_receipts_active_cost_amt,
     COALESCE(pif0.receipts_active_units, 0) AS lly_trueday_receipts_active_qty,
     COALESCE(pif0.receipts_inactive_cost, 0) AS lly_trueday_receipts_inactive_cost_amt,
     COALESCE(pif0.receipts_inactive_units, 0) AS lly_trueday_receipts_inactive_qty,
     COALESCE(pif0.rp_receipts_active_cost, 0) AS lly_trueday_rp_receipts_active_cost_amt,
     COALESCE(pif0.non_rp_receipts_active_cost, 0) AS lly_trueday_non_rp_receipts_active_cost_amt,
     COALESCE(mrif0.reclass_in_cost, 0) AS lly_trueday_reclass_in_cost_amt,
     COALESCE(mrif0.reclass_in_units, 0) AS lly_trueday_reclass_in_qty,
     COALESCE(mrif0.reclass_out_cost, 0) AS lly_trueday_reclass_out_cost_amt,
     COALESCE(mrif0.reclass_out_units, 0) AS lly_trueday_reclass_out_qty,
     COALESCE(rtvif0.work_plan_rtv_active_cost, 0) AS lly_trueday_return_to_vendor_active_cost_amt,
     COALESCE(rtvif0.work_plan_rtv_active_units, 0) AS lly_trueday_return_to_vendor_active_qty,
     COALESCE(rtvif0.work_plan_rtv_inactive_cost, 0) AS lly_trueday_return_to_vendor_inactive_cost_amt,
     COALESCE(rtvif0.work_plan_rtv_inactive_units, 0) AS lly_trueday_return_to_vendor_inactive_qty,
     COALESCE(vfmdt0.vendor_funds_cost, 0) AS lly_trueday_vendor_funds_cost_amt,
     COALESCE(vfmdt0.disc_terms_cost, 0) AS lly_trueday_discount_terms_cost_amt,
     COALESCE(mmif0.merch_margin_retail, 0) AS lly_trueday_merch_margin_retail_amt,
     COALESCE(mmif0.shrink_cost, 0) AS lly_trueday_shrink_banner_cost_amt,
     COALESCE(mmif0.shrink_units, 0) AS lly_trueday_shrink_banner_qty,
     COALESCE(mmif0.other_inventory_adj_cost, 0) AS lly_trueday_other_inventory_adj_cost,
     COALESCE(mmif0.merch_margin_charges_cost, 0) AS lly_trueday_merch_margin_charges_cost,
     mtbv1.dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_inventory_bop_insight_fact_vw AS iifbv0
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_insight_fact AS iif0 ON iifbv0.current_week_num = iif0.week_num AND iifbv0.department_num
          = iif0.department_num AND iifbv0.banner_num = iif0.banner_num AND iifbv0.fulfilment_num = iif0.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_transfer_ledger_insight_fact AS mtlif0 ON iifbv0.current_week_num = mtlif0.week_num AND
         iifbv0.department_num = mtlif0.department_num AND iifbv0.banner_num = mtlif0.banner_num AND iifbv0.fulfilment_num
        = mtlif0.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.poreceipts_insight_fact AS pif0 ON iifbv0.current_week_num = pif0.week_num AND iifbv0.department_num
          = pif0.department_num AND iifbv0.banner_num = pif0.banner_num AND iifbv0.fulfilment_num = pif0.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_reclass_insight_fact AS mrif0 ON iifbv0.current_week_num = mrif0.week_num AND iifbv0.department_num
          = mrif0.department_num AND iifbv0.banner_num = mrif0.banner_num AND iifbv0.fulfilment_num = mrif0.fulfilment_num
       
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.return_to_vendor_insight_fact AS rtvif0 ON iifbv0.current_week_num = rtvif0.week_num AND
         iifbv0.department_num = rtvif0.department_num AND iifbv0.banner_num = rtvif0.banner_num AND iifbv0.fulfilment_num
        = rtvif0.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.vfm_discterms_ledger_insight_fact AS vfmdt0 ON iifbv0.current_week_num = vfmdt0.week_num AND
         iifbv0.department_num = vfmdt0.department_num AND iifbv0.banner_num = vfmdt0.banner_num AND iifbv0.fulfilment_num
        = vfmdt0.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_margin_insight_fact AS mmif0 ON iifbv0.current_week_num = mmif0.week_num AND iifbv0.department_num
          = mmif0.department_num AND iifbv0.banner_num = mmif0.banner_num AND iifbv0.fulfilment_num = mmif0.fulfilment_num
       
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd1 ON wcd1.td_lly_week_num <> 444444 AND (iifbv0.current_week_num =
               wcd1.td_lly_week_num OR mmif0.week_num = wcd1.td_lly_week_num OR vfmdt0.week_num = wcd1.td_lly_week_num
            OR rtvif0.week_num = wcd1.td_lly_week_num OR mrif0.week_num = wcd1.td_lly_week_num OR pif0.week_num = wcd1.td_lly_week_num
            OR mtlif0.week_num = wcd1.td_lly_week_num OR iif0.week_num = wcd1.td_lly_week_num)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv1 ON LOWER(RTRIM(mtbv1.interface_code)) = LOWER(RTRIM('MFP_BANR_BLEND_WKLY'
        ))) AS a
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS c ON a.week_num = c.week_idnt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l01 ON LOWER(RTRIM(l01.interface_code)) = LOWER(RTRIM('MFP_BANR_BLEND_WKLY'
     ))
 GROUP BY a.dept_num,
  a.week_num,
  a.banner_country_num,
  a.fulfill_type_num);