BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=spp_supp_grp_fct;
---Task_Name=t0_1_spp_supp_grp_fact_load;'*/
---FOR SESSION VOLATILE;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_spp_suppgrp_dept_banner_month_fact;
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_spp_suppgrp_dept_banner_month_fact (supplier_group, dept_num, banner_num, channel_country
 , month_num, elapsed_month_flag, fiscal_year_num, ty_actual_net_sales_total_retail_amount,
 ty_actual_net_sales_total_cost_amount, ty_actual_vendor_funds_total_cost_amount, ty_actual_disc_terms_total_cost_amount
 , ty_actual_mos_total_cost_amount, ty_planned_net_sales_total_retail_amount, ty_planned_net_sales_total_cost_amount,
 ty_profitability_expectation_percentage, ty_actual_net_sales_regular_retail_amount,
 ty_actual_net_sales_promo_retail_amount, ty_actual_net_sales_clearance_retail_amount,
 ty_actual_net_sales_regular_cost_amount, ty_actual_net_sales_promo_cost_amount,
 ty_actual_net_sales_clearance_cost_amount, ty_actual_net_sales_regular_units, ty_actual_net_sales_promo_units,
 ty_actual_net_sales_clearance_units, ty_actual_net_sales_total_units, ty_actual_receipts_regular_retail_amount,
 ty_actual_receipts_clearance_retail_amount, ty_actual_receipts_total_retail_amount,
 ty_actual_receipts_regular_cost_amount, ty_actual_receipts_clearance_cost_amount, ty_actual_receipts_total_cost_amount
 , ty_actual_receipts_regular_units, ty_actual_receipts_clearance_units, ty_actual_receipts_total_units,
 ty_actual_onorder_regular_retail_amount, ty_actual_onorder_clearance_retail_amount,
 ty_actual_onorder_total_retail_amount, ty_actual_onorder_regular_cost_amount, ty_actual_onorder_clearance_cost_amount,
 ty_actual_onorder_total_cost_amount, ty_actual_onorder_regular_units, ty_actual_onorder_clearance_units,
 ty_actual_onorder_total_units, ty_actual_inv_bop_regular_retail_amount, ty_actual_inv_bop_clearance_retail_amount,
 ty_actual_inv_bop_total_retail_amount, ty_actual_inv_bop_regular_cost_amount, ty_actual_inv_bop_clearance_cost_amount,
 ty_actual_inv_bop_total_cost_amount, ty_actual_inv_bop_regular_units, ty_actual_inv_bop_clearance_units,
 ty_actual_inv_bop_total_units, ty_actual_inv_eop_regular_retail_amount, ty_actual_inv_eop_clearance_retail_amount,
 ty_actual_inv_eop_total_retail_amount, ty_actual_inv_eop_regular_cost_amount, ty_actual_inv_eop_clearance_cost_amount,
 ty_actual_inv_eop_total_cost_amount, ty_actual_inv_eop_regular_units, ty_actual_inv_eop_clearance_units,
 ty_actual_inv_eop_total_units, ty_actual_transfer_out_racking_retail_amount, ty_actual_transfer_out_racking_cost_amount
 , ty_actual_transfer_out_racking_units, ty_actual_rtv_total_cost_amount, ty_actual_rtv_total_units,
 ty_planned_net_sales_total_units, ty_planned_receipts_total_retail_amount, ty_planned_receipts_total_cost_amount,
 ty_planned_receipts_total_units, ty_planned_inv_total_retail_amount, ty_planned_inv_total_cost_amount,
 ty_planned_inv_total_units, ty_planned_inv_bop_total_units, ty_planned_inv_bop_total_cost_amount,
 ty_planned_rack_transfer_cost_amount, ty_planned_rack_transfer_units, ty_planned_rtv_total_cost_amount,
 ty_planned_rtv_total_units, ly_actual_net_sales_regular_retail_amount, ly_actual_net_sales_promo_retail_amount,
 ly_actual_net_sales_clearance_retail_amount, ly_actual_net_sales_regular_cost_amount,
 ly_actual_net_sales_promo_cost_amount, ly_actual_net_sales_clearance_cost_amount, ly_actual_net_sales_regular_units,
 ly_actual_net_sales_promo_units, ly_actual_net_sales_clearance_units, ly_actual_net_sales_total_units,
 ly_actual_receipts_regular_retail_amount, ly_actual_receipts_clearance_retail_amount,
 ly_actual_receipts_total_retail_amount, ly_actual_receipts_regular_cost_amount,
 ly_actual_receipts_clearance_cost_amount, ly_actual_receipts_total_cost_amount, ly_actual_receipts_regular_units,
 ly_actual_receipts_clearance_units, ly_actual_receipts_total_units, ly_actual_onorder_regular_retail_amount,
 ly_actual_onorder_clearance_retail_amount, ly_actual_onorder_total_retail_amount, ly_actual_onorder_regular_cost_amount
 , ly_actual_onorder_clearance_cost_amount, ly_actual_onorder_total_cost_amount, ly_actual_onorder_regular_units,
 ly_actual_onorder_clearance_units, ly_actual_onorder_total_units, ly_actual_inv_bop_regular_retail_amount,
 ly_actual_inv_bop_clearance_retail_amount, ly_actual_inv_bop_total_retail_amount, ly_actual_inv_bop_regular_cost_amount
 , ly_actual_inv_bop_clearance_cost_amount, ly_actual_inv_bop_total_cost_amount, ly_actual_inv_bop_regular_units,
 ly_actual_inv_bop_clearance_units, ly_actual_inv_bop_total_units, ly_actual_inv_eop_regular_retail_amount,
 ly_actual_inv_eop_clearance_retail_amount, ly_actual_inv_eop_total_retail_amount, ly_actual_inv_eop_regular_cost_amount
 , ly_actual_inv_eop_clearance_cost_amount, ly_actual_inv_eop_total_cost_amount, ly_actual_inv_eop_regular_units,
 ly_actual_inv_eop_clearance_units, ly_actual_inv_eop_total_units, ly_actual_transfer_out_racking_retail_amount,
 ly_actual_transfer_out_racking_cost_amount, ly_actual_transfer_out_racking_units, ly_actual_rtv_total_cost_amount,
 ly_actual_rtv_total_units, ly_planned_net_sales_total_units, ly_planned_receipts_total_retail_amount,
 ly_planned_receipts_total_cost_amount, ly_planned_receipts_total_units, ly_planned_inv_total_retail_amount,
 ly_planned_inv_total_cost_amount, ly_planned_inv_total_units, ly_planned_inv_bop_total_units,
 ly_planned_inv_bop_total_cost_amount, ly_planned_rack_transfer_cost_amount, ly_planned_rack_transfer_units,
 ly_actual_net_sales_total_retail_amount, ly_actual_net_sales_total_cost_amount,
 ly_actual_vendor_funds_total_cost_amount, ly_actual_disc_terms_total_cost_amount, ly_actual_mos_total_cost_amount,
 ly_planned_net_sales_total_retail_amount, ly_planned_net_sales_total_cost_amount, ly_planned_rtv_total_cost_amount,
 ly_planned_rtv_total_units, ly_profitability_expectation_percentage, dw_batch_date, dw_sys_load_tmstp)
(SELECT supplier_group,
  dept_num,
  banner_num,
  channel_country,
  month_num,
  elapsed_month_flag,
  fiscal_year_num,
  CAST(SUM(ty_actual_net_sales_total_retail_amount) AS NUMERIC) AS ty_actual_net_sales_total_retail_amount,
  CAST(SUM(ty_actual_net_sales_total_cost_amount) AS NUMERIC) AS ty_actual_net_sales_total_cost_amount,
  CAST(SUM(ty_actual_vendor_funds_total_cost_amount) AS NUMERIC) AS ty_actual_vendor_funds_total_cost_amount,
  CAST(SUM(ty_actual_disc_terms_total_cost_amount) AS NUMERIC) AS ty_actual_disc_terms_total_cost_amount,
  CAST(SUM(ty_actual_mos_total_cost_amount) AS NUMERIC) AS ty_actual_mos_total_cost_amount,
  SUM(ty_planned_net_sales_total_retail_amount) AS ty_planned_net_sales_total_retail_amount,
  SUM(ty_planned_net_sales_total_cost_amount) AS ty_planned_net_sales_total_cost_amount,
  SUM(ty_profitability_expectation_percentage) AS ty_profitability_expectation_percentage,
  CAST(SUM(ty_actual_net_sales_regular_retail_amount) AS NUMERIC) AS ty_actual_net_sales_regular_retail_amount,
  CAST(SUM(ty_actual_net_sales_promo_retail_amount) AS NUMERIC) AS ty_actual_net_sales_promo_retail_amount,
  CAST(SUM(ty_actual_net_sales_clearance_retail_amount) AS NUMERIC) AS ty_actual_net_sales_clearance_retail_amount,
  CAST(SUM(ty_actual_net_sales_regular_cost_amount) AS NUMERIC) AS ty_actual_net_sales_regular_cost_amount,
  CAST(SUM(ty_actual_net_sales_promo_cost_amount) AS NUMERIC) AS ty_actual_net_sales_promo_cost_amount,
  CAST(SUM(ty_actual_net_sales_clearance_cost_amount) AS NUMERIC) AS ty_actual_net_sales_clearance_cost_amount,
  CAST(SUM(ty_actual_net_sales_regular_units) AS INTEGER) AS ty_actual_net_sales_regular_units,
  CAST(SUM(ty_actual_net_sales_promo_units) AS INTEGER) AS ty_actual_net_sales_promo_units,
  CAST(SUM(ty_actual_net_sales_clearance_units) AS INTEGER) AS ty_actual_net_sales_clearance_units,
  CAST(SUM(ty_actual_net_sales_total_units) AS INTEGER) AS ty_actual_net_sales_total_units,
  CAST(SUM(ty_actual_receipts_regular_retail_amount) AS NUMERIC) AS ty_actual_receipts_regular_retail_amount,
  CAST(SUM(ty_actual_receipts_clearance_retail_amount) AS NUMERIC) AS ty_actual_receipts_clearance_retail_amount,
  CAST(SUM(ty_actual_receipts_total_retail_amount) AS NUMERIC) AS ty_actual_receipts_total_retail_amount,
  CAST(SUM(ty_actual_receipts_regular_cost_amount) AS NUMERIC) AS ty_actual_receipts_regular_cost_amount,
  CAST(SUM(ty_actual_receipts_clearance_cost_amount) AS NUMERIC) AS ty_actual_receipts_clearance_cost_amount,
  CAST(SUM(ty_actual_receipts_total_cost_amount) AS NUMERIC) AS ty_actual_receipts_total_cost_amount,
  CAST(SUM(ty_actual_receipts_regular_units) AS INTEGER) AS ty_actual_receipts_regular_units,
  CAST(SUM(ty_actual_receipts_clearance_units) AS INTEGER) AS ty_actual_receipts_clearance_units,
  CAST(SUM(ty_actual_receipts_total_units) AS INTEGER) AS ty_actual_receipts_total_units,
  SUM(ty_actual_onorder_regular_retail_amount) AS ty_actual_onorder_regular_retail_amount,
  SUM(ty_actual_onorder_clearance_retail_amount) AS ty_actual_onorder_clearance_retail_amount,
  SUM(ty_actual_onorder_total_retail_amount) AS ty_actual_onorder_total_retail_amount,
  SUM(ty_actual_onorder_regular_cost_amount) AS ty_actual_onorder_regular_cost_amount,
  SUM(ty_actual_onorder_clearance_cost_amount) AS ty_actual_onorder_clearance_cost_amount,
  SUM(ty_actual_onorder_total_cost_amount) AS ty_actual_onorder_total_cost_amount,
  CAST(SUM(ty_actual_onorder_regular_units) AS INTEGER) AS ty_actual_onorder_regular_units,
  CAST(SUM(ty_actual_onorder_clearance_units) AS INTEGER) AS ty_actual_onorder_clearance_units,
  CAST(SUM(ty_actual_onorder_total_units) AS INTEGER) AS ty_actual_onorder_total_units,
  CAST(SUM(ty_actual_inv_bop_regular_retail_amount) AS NUMERIC) AS ty_actual_inv_bop_regular_retail_amount,
  CAST(SUM(ty_actual_inv_bop_clearance_retail_amount) AS NUMERIC) AS ty_actual_inv_bop_clearance_retail_amount,
  CAST(SUM(ty_actual_inv_bop_total_retail_amount) AS NUMERIC) AS ty_actual_inv_bop_total_retail_amount,
  CAST(SUM(ty_actual_inv_bop_regular_cost_amount) AS NUMERIC) AS ty_actual_inv_bop_regular_cost_amount,
  CAST(SUM(ty_actual_inv_bop_clearance_cost_amount) AS NUMERIC) AS ty_actual_inv_bop_clearance_cost_amount,
  CAST(SUM(ty_actual_inv_bop_total_cost_amount) AS NUMERIC) AS ty_actual_inv_bop_total_cost_amount,
  CAST(SUM(ty_actual_inv_bop_regular_units) AS INTEGER) AS ty_actual_inv_bop_regular_units,
  CAST(SUM(ty_actual_inv_bop_clearance_units) AS INTEGER) AS ty_actual_inv_bop_clearance_units,
  CAST(SUM(ty_actual_inv_bop_total_units) AS INTEGER) AS ty_actual_inv_bop_total_units,
  CAST(SUM(ty_actual_inv_eop_regular_retail_amount) AS NUMERIC) AS ty_actual_inv_eop_regular_retail_amount,
  CAST(SUM(ty_actual_inv_eop_clearance_retail_amount) AS NUMERIC) AS ty_actual_inv_eop_clearance_retail_amount,
  CAST(SUM(ty_actual_inv_eop_total_retail_amount) AS NUMERIC) AS ty_actual_inv_eop_total_retail_amount,
  CAST(SUM(ty_actual_inv_eop_regular_cost_amount) AS NUMERIC) AS ty_actual_inv_eop_regular_cost_amount,
  CAST(SUM(ty_actual_inv_eop_clearance_cost_amount) AS NUMERIC) AS ty_actual_inv_eop_clearance_cost_amount,
  CAST(SUM(ty_actual_inv_eop_total_cost_amount) AS NUMERIC) AS ty_actual_inv_eop_total_cost_amount,
  CAST(SUM(ty_actual_inv_eop_regular_units) AS INTEGER) AS ty_actual_inv_eop_regular_units,
  CAST(SUM(ty_actual_inv_eop_clearance_units) AS INTEGER) AS ty_actual_inv_eop_clearance_units,
  CAST(SUM(ty_actual_inv_eop_total_units) AS INTEGER) AS ty_actual_inv_eop_total_units,
  CAST(SUM(ty_actual_transfer_out_racking_retail_amount) AS NUMERIC) AS ty_actual_transfer_out_racking_retail_amount,
  CAST(SUM(ty_actual_transfer_out_racking_cost_amount) AS NUMERIC) AS ty_actual_transfer_out_racking_cost_amount,
  CAST(SUM(ty_actual_transfer_out_racking_units) AS INTEGER) AS ty_actual_transfer_out_racking_units,
  CAST(SUM(ty_actual_rtv_total_cost_amount) AS NUMERIC) AS ty_actual_rtv_total_cost_amount,
  CAST(SUM(ty_actual_rtv_total_units) AS INTEGER) AS ty_actual_rtv_total_units,
  ROUND(CAST(SUM(ty_planned_net_sales_total_units) AS NUMERIC), 4) AS ty_planned_net_sales_total_units,
  SUM(ty_planned_receipts_total_retail_amount) AS ty_planned_receipts_total_retail_amount,
  SUM(ty_planned_receipts_total_cost_amount) AS ty_planned_receipts_total_cost_amount,
  ROUND(CAST(SUM(ty_planned_receipts_total_units) AS NUMERIC), 4) AS ty_planned_receipts_total_units,
  SUM(ty_planned_inv_total_retail_amount) AS ty_planned_inv_total_retail_amount,
  SUM(ty_planned_inv_total_cost_amount) AS ty_planned_inv_total_cost_amount,
  ROUND(CAST(SUM(ty_planned_inv_total_units) AS NUMERIC), 4) AS ty_planned_inv_total_units,
  ROUND(CAST(SUM(ty_planned_inv_bop_total_units) AS NUMERIC), 4) AS ty_planned_inv_bop_total_units,
  SUM(ty_planned_inv_bop_total_cost_amount) AS ty_planned_inv_bop_total_cost_amount,
  SUM(ty_planned_rack_transfer_cost_amount) AS ty_planned_rack_transfer_cost_amount,
  ROUND(CAST(SUM(ty_planned_rack_transfer_units) AS NUMERIC), 4) AS ty_planned_rack_transfer_units,
  SUM(ty_planned_rtv_total_cost_amount) AS ty_planned_rtv_total_cost_amount,
  ROUND(CAST(SUM(ty_planned_rtv_total_units) AS NUMERIC), 4) AS ty_planned_rtv_total_units,
  CAST(SUM(ly_actual_net_sales_regular_retail_amount) AS NUMERIC) AS ly_actual_net_sales_regular_retail_amount,
  CAST(SUM(ly_actual_net_sales_promo_retail_amount) AS NUMERIC) AS ly_actual_net_sales_promo_retail_amount,
  CAST(SUM(ly_actual_net_sales_clearance_retail_amount) AS NUMERIC) AS ly_actual_net_sales_clearance_retail_amount,
  CAST(SUM(ly_actual_net_sales_regular_cost_amount) AS NUMERIC) AS ly_actual_net_sales_regular_cost_amount,
  CAST(SUM(ly_actual_net_sales_promo_cost_amount) AS NUMERIC) AS ly_actual_net_sales_promo_cost_amount,
  CAST(SUM(ly_actual_net_sales_clearance_cost_amount) AS NUMERIC) AS ly_actual_net_sales_clearance_cost_amount,
  CAST(SUM(ly_actual_net_sales_regular_units) AS INTEGER) AS ly_actual_net_sales_regular_units,
  CAST(SUM(ly_actual_net_sales_promo_units) AS INTEGER) AS ly_actual_net_sales_promo_units,
  CAST(SUM(ly_actual_net_sales_clearance_units) AS INTEGER) AS ly_actual_net_sales_clearance_units,
  CAST(SUM(ly_actual_net_sales_total_units) AS INTEGER) AS ly_actual_net_sales_total_units,
  CAST(SUM(ly_actual_receipts_regular_retail_amount) AS NUMERIC) AS ly_actual_receipts_regular_retail_amount,
  CAST(SUM(ly_actual_receipts_clearance_retail_amount) AS NUMERIC) AS ly_actual_receipts_clearance_retail_amount,
  CAST(SUM(ly_actual_receipts_total_retail_amount) AS NUMERIC) AS ly_actual_receipts_total_retail_amount,
  CAST(SUM(ly_actual_receipts_regular_cost_amount) AS NUMERIC) AS ly_actual_receipts_regular_cost_amount,
  CAST(SUM(ly_actual_receipts_clearance_cost_amount) AS NUMERIC) AS ly_actual_receipts_clearance_cost_amount,
  CAST(SUM(ly_actual_receipts_total_cost_amount) AS NUMERIC) AS ly_actual_receipts_total_cost_amount,
  CAST(SUM(ly_actual_receipts_regular_units) AS INTEGER) AS ly_actual_receipts_regular_units,
  CAST(SUM(ly_actual_receipts_clearance_units) AS INTEGER) AS ly_actual_receipts_clearance_units,
  CAST(SUM(ly_actual_receipts_total_units) AS INTEGER) AS ly_actual_receipts_total_units,
  SUM(ly_actual_onorder_regular_retail_amount) AS ly_actual_onorder_regular_retail_amount,
  SUM(ly_actual_onorder_clearance_retail_amount) AS ly_actual_onorder_clearance_retail_amount,
  SUM(ly_actual_onorder_total_retail_amount) AS ly_actual_onorder_total_retail_amount,
  SUM(ly_actual_onorder_regular_cost_amount) AS ly_actual_onorder_regular_cost_amount,
  SUM(ly_actual_onorder_clearance_cost_amount) AS ly_actual_onorder_clearance_cost_amount,
  SUM(ly_actual_onorder_total_cost_amount) AS ly_actual_onorder_total_cost_amount,
  CAST(SUM(ly_actual_onorder_regular_units) AS INTEGER) AS ly_actual_onorder_regular_units,
  CAST(SUM(ly_actual_onorder_clearance_units) AS INTEGER) AS ly_actual_onorder_clearance_units,
  CAST(SUM(ly_actual_onorder_total_units) AS INTEGER) AS ly_actual_onorder_total_units,
  CAST(SUM(ly_actual_inv_bop_regular_retail_amount) AS NUMERIC) AS ly_actual_inv_bop_regular_retail_amount,
  CAST(SUM(ly_actual_inv_bop_clearance_retail_amount) AS NUMERIC) AS ly_actual_inv_bop_clearance_retail_amount,
  CAST(SUM(ly_actual_inv_bop_total_retail_amount) AS NUMERIC) AS ly_actual_inv_bop_total_retail_amount,
  CAST(SUM(ly_actual_inv_bop_regular_cost_amount) AS NUMERIC) AS ly_actual_inv_bop_regular_cost_amount,
  CAST(SUM(ly_actual_inv_bop_clearance_cost_amount) AS NUMERIC) AS ly_actual_inv_bop_clearance_cost_amount,
  CAST(SUM(ly_actual_inv_bop_total_cost_amount) AS NUMERIC) AS ly_actual_inv_bop_total_cost_amount,
  CAST(SUM(ly_actual_inv_bop_regular_units) AS INTEGER) AS ly_actual_inv_bop_regular_units,
  CAST(SUM(ly_actual_inv_bop_clearance_units) AS INTEGER) AS ly_actual_inv_bop_clearance_units,
  CAST(SUM(ly_actual_inv_bop_total_units) AS INTEGER) AS ly_actual_inv_bop_total_units,
  CAST(SUM(ly_actual_inv_eop_regular_retail_amount) AS NUMERIC) AS ly_actual_inv_eop_regular_retail_amount,
  CAST(SUM(ly_actual_inv_eop_clearance_retail_amount) AS NUMERIC) AS ly_actual_inv_eop_clearance_retail_amount,
  CAST(SUM(ly_actual_inv_eop_total_retail_amount) AS NUMERIC) AS ly_actual_inv_eop_total_retail_amount,
  CAST(SUM(ly_actual_inv_eop_regular_cost_amount) AS NUMERIC) AS ly_actual_inv_eop_regular_cost_amount,
  CAST(SUM(ly_actual_inv_eop_clearance_cost_amount) AS NUMERIC) AS ly_actual_inv_eop_clearance_cost_amount,
  CAST(SUM(ly_actual_inv_eop_total_cost_amount) AS NUMERIC) AS ly_actual_inv_eop_total_cost_amount,
  CAST(SUM(ly_actual_inv_eop_regular_units) AS INTEGER) AS ly_actual_inv_eop_regular_units,
  CAST(SUM(ly_actual_inv_eop_clearance_units) AS INTEGER) AS ly_actual_inv_eop_clearance_units,
  CAST(SUM(ly_actual_inv_eop_total_units) AS INTEGER) AS ly_actual_inv_eop_total_units,
  CAST(SUM(ly_actual_transfer_out_racking_retail_amount) AS NUMERIC) AS ly_actual_transfer_out_racking_retail_amount,
  CAST(SUM(ly_actual_transfer_out_racking_cost_amount) AS NUMERIC) AS ly_actual_transfer_out_racking_cost_amount,
  CAST(SUM(ly_actual_transfer_out_racking_units) AS INTEGER) AS ly_actual_transfer_out_racking_units,
  CAST(SUM(ly_actual_rtv_total_cost_amount) AS NUMERIC) AS ly_actual_rtv_total_cost_amount,
  CAST(SUM(ly_actual_rtv_total_units) AS INTEGER) AS ly_actual_rtv_total_units,
  ROUND(CAST(SUM(ly_planned_net_sales_total_units) AS NUMERIC), 4) AS ly_planned_net_sales_total_units,
  SUM(ly_planned_receipts_total_retail_amount) AS ly_planned_receipts_total_retail_amount,
  SUM(ly_planned_receipts_total_cost_amount) AS ly_planned_receipts_total_cost_amount,
  ROUND(CAST(SUM(ly_planned_receipts_total_units) AS NUMERIC), 4) AS ly_planned_receipts_total_units,
  SUM(ly_planned_inv_total_retail_amount) AS ly_planned_inv_total_retail_amount,
  SUM(ly_planned_inv_total_cost_amount) AS ly_planned_inv_total_cost_amount,
  ROUND(CAST(SUM(ly_planned_inv_total_units) AS NUMERIC), 4) AS ly_planned_inv_total_units,
  ROUND(CAST(SUM(ly_planned_inv_bop_total_units) AS NUMERIC), 4) AS ly_planned_inv_bop_total_units,
  SUM(ly_planned_inv_bop_total_cost_amount) AS ly_planned_inv_bop_total_cost_amount,
  SUM(ly_planned_rack_transfer_cost_amount) AS ly_planned_rack_transfer_cost_amount,
  ROUND(CAST(SUM(ly_planned_rack_transfer_units) AS NUMERIC), 4) AS ly_planned_rack_transfer_units,
  CAST(SUM(ly_actual_net_sales_total_retail_amount) AS NUMERIC) AS ly_actual_net_sales_total_retail_amount,
  CAST(SUM(ly_actual_net_sales_total_cost_amount) AS NUMERIC) AS ly_actual_net_sales_total_cost_amount,
  CAST(SUM(ly_actual_vendor_funds_total_cost_amount) AS NUMERIC) AS ly_actual_vendor_funds_total_cost_amount,
  CAST(SUM(ly_actual_disc_terms_total_cost_amount) AS NUMERIC) AS ly_actual_disc_terms_total_cost_amount,
  CAST(SUM(ly_actual_mos_total_cost_amount) AS NUMERIC) AS ly_actual_mos_total_cost_amount,
  SUM(ly_planned_net_sales_total_retail_amount) AS ly_planned_net_sales_total_retail_amount,
  SUM(ly_planned_net_sales_total_cost_amount) AS ly_planned_net_sales_total_cost_amount,
  SUM(ly_planned_rtv_total_cost_amount) AS ly_planned_rtv_total_cost_amount,
  ROUND(CAST(SUM(ly_planned_rtv_total_units) AS NUMERIC), 4) AS ly_planned_rtv_total_units,
  CAST(SUM(ly_profitability_expectation_percentage) AS INTEGER) AS ly_profitability_expectation_percentage,
  MAX(dw_batch_date) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT a01.supplier_group,
     a01.dept_num,
     a01.banner_num,
     a01.channel_country,
     a01.month_num,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN 'Y'
      ELSE 'N'
      END AS elapsed_month_flag,
     a01.fiscal_year_num,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_total_retail_amount
      ELSE 0
      END AS ty_actual_net_sales_total_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_total_cost_amount
      ELSE 0
      END AS ty_actual_net_sales_total_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_vendor_funds_total_cost_amount
      ELSE 0
      END AS ty_actual_vendor_funds_total_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_disc_terms_total_cost_amount
      ELSE 0
      END AS ty_actual_disc_terms_total_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_mos_total_cost_amount
      ELSE 0
      END AS ty_actual_mos_total_cost_amount,
     a01.planned_net_sales_total_retail_amount AS ty_planned_net_sales_total_retail_amount,
     a01.planned_net_sales_total_cost_amount AS ty_planned_net_sales_total_cost_amount,
     COALESCE(mspe.planned_profitability_expectation_percent, 0) AS ty_profitability_expectation_percentage,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_regular_retail_amount
      ELSE 0
      END AS ty_actual_net_sales_regular_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_promo_retail_amount
      ELSE 0
      END AS ty_actual_net_sales_promo_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_clearance_retail_amount
      ELSE 0
      END AS ty_actual_net_sales_clearance_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_regular_cost_amount
      ELSE 0
      END AS ty_actual_net_sales_regular_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_promo_cost_amount
      ELSE 0
      END AS ty_actual_net_sales_promo_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_clearance_cost_amount
      ELSE 0
      END AS ty_actual_net_sales_clearance_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_regular_units
      ELSE 0
      END AS ty_actual_net_sales_regular_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_promo_units
      ELSE 0
      END AS ty_actual_net_sales_promo_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_clearance_units
      ELSE 0
      END AS ty_actual_net_sales_clearance_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_net_sales_total_units
      ELSE 0
      END AS ty_actual_net_sales_total_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_regular_retail_amount
      ELSE 0
      END AS ty_actual_receipts_regular_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_clearance_retail_amount
      ELSE 0
      END AS ty_actual_receipts_clearance_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_total_retail_amount
      ELSE 0
      END AS ty_actual_receipts_total_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_regular_cost_amount
      ELSE 0
      END AS ty_actual_receipts_regular_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_clearance_cost_amount
      ELSE 0
      END AS ty_actual_receipts_clearance_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_total_cost_amount
      ELSE 0
      END AS ty_actual_receipts_total_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_regular_units
      ELSE 0
      END AS ty_actual_receipts_regular_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_clearance_units
      ELSE 0
      END AS ty_actual_receipts_clearance_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_receipts_total_units
      ELSE 0
      END AS ty_actual_receipts_total_units,
     a01.actual_onorder_regular_retail_amount AS ty_actual_onorder_regular_retail_amount,
     a01.actual_onorder_clearance_retail_amount AS ty_actual_onorder_clearance_retail_amount,
     a01.actual_onorder_total_retail_amount AS ty_actual_onorder_total_retail_amount,
     a01.actual_onorder_regular_cost_amount AS ty_actual_onorder_regular_cost_amount,
     a01.actual_onorder_clearance_cost_amount AS ty_actual_onorder_clearance_cost_amount,
     a01.actual_onorder_total_cost_amount AS ty_actual_onorder_total_cost_amount,
     a01.actual_onorder_regular_units AS ty_actual_onorder_regular_units,
     a01.actual_onorder_clearance_units AS ty_actual_onorder_clearance_units,
     a01.actual_onorder_total_units AS ty_actual_onorder_total_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_regular_retail_amount
      ELSE 0
      END AS ty_actual_inv_bop_regular_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_clearance_retail_amount
      ELSE 0
      END AS ty_actual_inv_bop_clearance_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_total_retail_amount
      ELSE 0
      END AS ty_actual_inv_bop_total_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_regular_cost_amount
      ELSE 0
      END AS ty_actual_inv_bop_regular_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_clearance_cost_amount
      ELSE 0
      END AS ty_actual_inv_bop_clearance_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_total_cost_amount
      ELSE 0
      END AS ty_actual_inv_bop_total_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_regular_units
      ELSE 0
      END AS ty_actual_inv_bop_regular_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_clearance_units
      ELSE 0
      END AS ty_actual_inv_bop_clearance_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_bop_total_units
      ELSE 0
      END AS ty_actual_inv_bop_total_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_regular_retail_amount
      ELSE 0
      END AS ty_actual_inv_eop_regular_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_clearance_retail_amount
      ELSE 0
      END AS ty_actual_inv_eop_clearance_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_total_retail_amount
      ELSE 0
      END AS ty_actual_inv_eop_total_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_regular_cost_amount
      ELSE 0
      END AS ty_actual_inv_eop_regular_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_clearance_cost_amount
      ELSE 0
      END AS ty_actual_inv_eop_clearance_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_total_cost_amount
      ELSE 0
      END AS ty_actual_inv_eop_total_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_regular_units
      ELSE 0
      END AS ty_actual_inv_eop_regular_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_clearance_units
      ELSE 0
      END AS ty_actual_inv_eop_clearance_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_inv_eop_total_units
      ELSE 0
      END AS ty_actual_inv_eop_total_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_transfer_out_racking_retail_amount
      ELSE 0
      END AS ty_actual_transfer_out_racking_retail_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_transfer_out_racking_cost_amount
      ELSE 0
      END AS ty_actual_transfer_out_racking_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_transfer_out_racking_units
      ELSE 0
      END AS ty_actual_transfer_out_racking_units,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_rtv_total_cost_amount
      ELSE 0
      END AS ty_actual_rtv_total_cost_amount,
      CASE
      WHEN a01.month_num <= l01.last_completed_fiscal_month_num
      THEN a01.actual_rtv_total_units
      ELSE 0
      END AS ty_actual_rtv_total_units,
     a01.planned_net_sales_total_units AS ty_planned_net_sales_total_units,
     a01.planned_receipts_total_retail_amount AS ty_planned_receipts_total_retail_amount,
     a01.planned_receipts_total_cost_amount AS ty_planned_receipts_total_cost_amount,
     a01.planned_receipts_total_units AS ty_planned_receipts_total_units,
     a01.planned_inv_total_retail_amount AS ty_planned_inv_total_retail_amount,
     a01.planned_inv_total_cost_amount AS ty_planned_inv_total_cost_amount,
     a01.planned_inv_total_units AS ty_planned_inv_total_units,
     a01.planned_inv_bop_total_units AS ty_planned_inv_bop_total_units,
     a01.planned_inv_bop_total_cost_amount AS ty_planned_inv_bop_total_cost_amount,
     a01.planned_rack_transfer_cost_amount AS ty_planned_rack_transfer_cost_amount,
     a01.planned_rack_transfer_units AS ty_planned_rack_transfer_units,
     a01.planned_rtv_total_cost_amount AS ty_planned_rtv_total_cost_amount,
     a01.planned_rtv_total_units AS ty_planned_rtv_total_units,
     0 AS ly_actual_net_sales_total_retail_amount,
     0 AS ly_actual_net_sales_total_cost_amount,
     0 AS ly_actual_vendor_funds_total_cost_amount,
     0 AS ly_actual_disc_terms_total_cost_amount,
     0 AS ly_actual_mos_total_cost_amount,
     0 AS ly_planned_net_sales_total_retail_amount,
     0 AS ly_planned_net_sales_total_cost_amount,
     0 AS ly_profitability_expectation_percentage,
     0 AS ly_actual_net_sales_regular_retail_amount,
     0 AS ly_actual_net_sales_promo_retail_amount,
     0 AS ly_actual_net_sales_clearance_retail_amount,
     0 AS ly_actual_net_sales_regular_cost_amount,
     0 AS ly_actual_net_sales_promo_cost_amount,
     0 AS ly_actual_net_sales_clearance_cost_amount,
     0 AS ly_actual_net_sales_regular_units,
     0 AS ly_actual_net_sales_promo_units,
     0 AS ly_actual_net_sales_clearance_units,
     0 AS ly_actual_net_sales_total_units,
     0 AS ly_actual_receipts_regular_retail_amount,
     0 AS ly_actual_receipts_clearance_retail_amount,
     0 AS ly_actual_receipts_total_retail_amount,
     0 AS ly_actual_receipts_regular_cost_amount,
     0 AS ly_actual_receipts_clearance_cost_amount,
     0 AS ly_actual_receipts_total_cost_amount,
     0 AS ly_actual_receipts_regular_units,
     0 AS ly_actual_receipts_clearance_units,
     0 AS ly_actual_receipts_total_units,
     0 AS ly_actual_onorder_regular_retail_amount,
     0 AS ly_actual_onorder_clearance_retail_amount,
     0 AS ly_actual_onorder_total_retail_amount,
     0 AS ly_actual_onorder_regular_cost_amount,
     0 AS ly_actual_onorder_clearance_cost_amount,
     0 AS ly_actual_onorder_total_cost_amount,
     0 AS ly_actual_onorder_regular_units,
     0 AS ly_actual_onorder_clearance_units,
     0 AS ly_actual_onorder_total_units,
     0 AS ly_actual_inv_bop_regular_retail_amount,
     0 AS ly_actual_inv_bop_clearance_retail_amount,
     0 AS ly_actual_inv_bop_total_retail_amount,
     0 AS ly_actual_inv_bop_regular_cost_amount,
     0 AS ly_actual_inv_bop_clearance_cost_amount,
     0 AS ly_actual_inv_bop_total_cost_amount,
     0 AS ly_actual_inv_bop_regular_units,
     0 AS ly_actual_inv_bop_clearance_units,
     0 AS ly_actual_inv_bop_total_units,
     0 AS ly_actual_inv_eop_regular_retail_amount,
     0 AS ly_actual_inv_eop_clearance_retail_amount,
     0 AS ly_actual_inv_eop_total_retail_amount,
     0 AS ly_actual_inv_eop_regular_cost_amount,
     0 AS ly_actual_inv_eop_clearance_cost_amount,
     0 AS ly_actual_inv_eop_total_cost_amount,
     0 AS ly_actual_inv_eop_regular_units,
     0 AS ly_actual_inv_eop_clearance_units,
     0 AS ly_actual_inv_eop_total_units,
     0 AS ly_actual_transfer_out_racking_retail_amount,
     0 AS ly_actual_transfer_out_racking_cost_amount,
     0 AS ly_actual_transfer_out_racking_units,
     0 AS ly_actual_rtv_total_cost_amount,
     0 AS ly_actual_rtv_total_units,
     0 AS ly_planned_net_sales_total_units,
     0 AS ly_planned_receipts_total_retail_amount,
     0 AS ly_planned_receipts_total_cost_amount,
     0 AS ly_planned_receipts_total_units,
     0 AS ly_planned_inv_total_retail_amount,
     0 AS ly_planned_inv_total_cost_amount,
     0 AS ly_planned_inv_total_units,
     0 AS ly_planned_inv_bop_total_units,
     0 AS ly_planned_inv_bop_total_cost_amount,
     0 AS ly_planned_rack_transfer_cost_amount,
     0 AS ly_planned_rack_transfer_units,
     0 AS ly_planned_rtv_total_cost_amount,
     0 AS ly_planned_rtv_total_units,
     l01.dw_batch_dt AS dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_ap_plan_actual_suppgrp_banner_fact AS a01
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_spe_profitability_expectation_vw AS mspe 
     ON LOWER(a01.supplier_group) = LOWER(mspe.supplier_group) 
      AND a01.dept_num = mspe.department_number 
      AND a01.banner_num = mspe.banner_num 
      AND a01.fiscal_year_num = mspe.year_num 
      AND LOWER(a01.channel_country) = LOWER(mspe.selling_country)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.month_cal_vw AS c01 
      ON a01.month_num = c01.month_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l01 
      ON LOWER(l01.interface_code) = LOWER('MERCH_NAP_SPP_DLY')
    WHERE a01.fiscal_year_num >= (SELECT current_fiscal_year_num - 1
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPP_DLY'))
     AND a01.fiscal_year_num <= (SELECT current_fiscal_year_num + 2
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPP_DLY'))
    UNION ALL
    SELECT a010.supplier_group,
     a010.dept_num,
     a010.banner_num,
     a010.channel_country,
     c010.month_num,
      CASE
      WHEN c010.month_num <= l010.last_completed_fiscal_month_num
      THEN 'Y'
      ELSE 'N'
      END AS elapsed_month_flag,
     c010.year_num AS fiscal_year_num,
     0 AS ty_actual_net_sales_total_retail_amount,
     0 AS ty_actual_net_sales_total_cost_amount,
     0 AS ty_actual_vendor_funds_total_cost_amount,
     0 AS ty_actual_disc_terms_total_cost_amount,
     0 AS ty_actual_mos_total_cost_amount,
     0 AS ty_planned_net_sales_total_retail_amount,
     0 AS ty_planned_net_sales_total_cost_amount,
     0 AS ty_profitability_expectation_percentage,
     0 AS ty_actual_net_sales_regular_retail_amount,
     0 AS ty_actual_net_sales_promo_retail_amount,
     0 AS ty_actual_net_sales_clearance_retail_amount,
     0 AS ty_actual_net_sales_regular_cost_amount,
     0 AS ty_actual_net_sales_promo_cost_amount,
     0 AS ty_actual_net_sales_clearance_cost_amount,
     0 AS ty_actual_net_sales_regular_units,
     0 AS ty_actual_net_sales_promo_units,
     0 AS ty_actual_net_sales_clearance_units,
     0 AS ty_actual_net_sales_total_units,
     0 AS ty_actual_receipts_regular_retail_amount,
     0 AS ty_actual_receipts_clearance_retail_amount,
     0 AS ty_actual_receipts_total_retail_amount,
     0 AS ty_actual_receipts_regular_cost_amount,
     0 AS ty_actual_receipts_clearance_cost_amount,
     0 AS ty_actual_receipts_total_cost_amount,
     0 AS ty_actual_receipts_regular_units,
     0 AS ty_actual_receipts_clearance_units,
     0 AS ty_actual_receipts_total_units,
     0 AS ty_actual_onorder_regular_retail_amount,
     0 AS ty_actual_onorder_clearance_retail_amount,
     0 AS ty_actual_onorder_total_retail_amount,
     0 AS ty_actual_onorder_regular_cost_amount,
     0 AS ty_actual_onorder_clearance_cost_amount,
     0 AS ty_actual_onorder_total_cost_amount,
     0 AS ty_actual_onorder_regular_units,
     0 AS ty_actual_onorder_clearance_units,
     0 AS ty_actual_onorder_total_units,
     0 AS ty_actual_inv_bop_regular_retail_amount,
     0 AS ty_actual_inv_bop_clearance_retail_amount,
     0 AS ty_actual_inv_bop_total_retail_amount,
     0 AS ty_actual_inv_bop_regular_cost_amount,
     0 AS ty_actual_inv_bop_clearance_cost_amount,
     0 AS ty_actual_inv_bop_total_cost_amount,
     0 AS ty_actual_inv_bop_regular_units,
     0 AS ty_actual_inv_bop_clearance_units,
     0 AS ty_actual_inv_bop_total_units,
     0 AS ty_actual_inv_eop_regular_retail_amount,
     0 AS ty_actual_inv_eop_clearance_retail_amount,
     0 AS ty_actual_inv_eop_total_retail_amount,
     0 AS ty_actual_inv_eop_regular_cost_amount,
     0 AS ty_actual_inv_eop_clearance_cost_amount,
     0 AS ty_actual_inv_eop_total_cost_amount,
     0 AS ty_actual_inv_eop_regular_units,
     0 AS ty_actual_inv_eop_clearance_units,
     0 AS ty_actual_inv_eop_total_units,
     0 AS ty_actual_transfer_out_racking_retail_amount,
     0 AS ty_actual_transfer_out_racking_cost_amount,
     0 AS ty_actual_transfer_out_racking_units,
     0 AS ty_actual_rtv_total_cost_amount,
     0 AS ty_actual_rtv_total_units,
     0 AS ty_planned_net_sales_total_units,
     0 AS ty_planned_receipts_total_retail_amount,
     0 AS ty_planned_receipts_total_cost_amount,
     0 AS ty_planned_receipts_total_units,
     0 AS ty_planned_inv_total_retail_amount,
     0 AS ty_planned_inv_total_cost_amount,
     0 AS ty_planned_inv_total_units,
     0 AS ty_planned_inv_bop_total_units,
     0 AS ty_planned_inv_bop_total_cost_amount,
     0 AS ty_planned_rack_transfer_cost_amount,
     0 AS ty_planned_rack_transfer_units,
     0 AS ty_planned_rtv_total_cost_amount,
     0 AS ty_planned_rtv_total_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_total_retail_amount
      ELSE 0
      END AS ly_actual_net_sales_total_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_total_cost_amount
      ELSE 0
      END AS ly_actual_net_sales_total_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_vendor_funds_total_cost_amount
      ELSE 0
      END AS ly_actual_vendor_funds_total_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_disc_terms_total_cost_amount
      ELSE 0
      END AS ly_actual_disc_terms_total_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_mos_total_cost_amount
      ELSE 0
      END AS ly_actual_mos_total_cost_amount,
     a010.planned_net_sales_total_retail_amount AS ly_planned_net_sales_total_retail_amount,
     a010.planned_net_sales_total_cost_amount AS ly_planned_net_sales_total_cost_amount,
     COALESCE(mspe0.planned_profitability_expectation_percent, 0) AS ly_profitability_expectation_percentage,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_regular_retail_amount
      ELSE 0
      END AS ly_actual_net_sales_regular_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_promo_retail_amount
      ELSE 0
      END AS ly_actual_net_sales_promo_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_clearance_retail_amount
      ELSE 0
      END AS ly_actual_net_sales_clearance_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_regular_cost_amount
      ELSE 0
      END AS ly_actual_net_sales_regular_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_promo_cost_amount
      ELSE 0
      END AS ly_actual_net_sales_promo_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_clearance_cost_amount
      ELSE 0
      END AS ly_actual_net_sales_clearance_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_regular_units
      ELSE 0
      END AS ly_actual_net_sales_regular_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_promo_units
      ELSE 0
      END AS ly_actual_net_sales_promo_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_clearance_units
      ELSE 0
      END AS ly_actual_net_sales_clearance_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_net_sales_total_units
      ELSE 0
      END AS ly_actual_net_sales_total_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_regular_retail_amount
      ELSE 0
      END AS ly_actual_receipts_regular_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_clearance_retail_amount
      ELSE 0
      END AS ly_actual_receipts_clearance_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_total_retail_amount
      ELSE 0
      END AS ly_actual_receipts_total_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_regular_cost_amount
      ELSE 0
      END AS ly_actual_receipts_regular_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_clearance_cost_amount
      ELSE 0
      END AS ly_actual_receipts_clearance_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_total_cost_amount
      ELSE 0
      END AS ly_actual_receipts_total_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_regular_units
      ELSE 0
      END AS ly_actual_receipts_regular_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_clearance_units
      ELSE 0
      END AS ly_actual_receipts_clearance_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_receipts_total_units
      ELSE 0
      END AS ly_actual_receipts_total_units,
     a010.actual_onorder_regular_retail_amount AS ly_actual_onorder_regular_retail_amount,
     a010.actual_onorder_clearance_retail_amount AS ly_actual_onorder_clearance_retail_amount,
     a010.actual_onorder_total_retail_amount AS ly_actual_onorder_total_retail_amount,
     a010.actual_onorder_regular_cost_amount AS ly_actual_onorder_regular_cost_amount,
     a010.actual_onorder_clearance_cost_amount AS ly_actual_onorder_clearance_cost_amount,
     a010.actual_onorder_total_cost_amount AS ly_actual_onorder_total_cost_amount,
     a010.actual_onorder_regular_units AS ly_actual_onorder_regular_units,
     a010.actual_onorder_clearance_units AS ly_actual_onorder_clearance_units,
     a010.actual_onorder_total_units AS ly_actual_onorder_total_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_regular_retail_amount
      ELSE 0
      END AS ly_actual_inv_bop_regular_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_clearance_retail_amount
      ELSE 0
      END AS ly_actual_inv_bop_clearance_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_total_retail_amount
      ELSE 0
      END AS ly_actual_inv_bop_total_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_regular_cost_amount
      ELSE 0
      END AS ly_actual_inv_bop_regular_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_clearance_cost_amount
      ELSE 0
      END AS ly_actual_inv_bop_clearance_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_total_cost_amount
      ELSE 0
      END AS ly_actual_inv_bop_total_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_regular_units
      ELSE 0
      END AS ly_actual_inv_bop_regular_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_clearance_units
      ELSE 0
      END AS ly_actual_inv_bop_clearance_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_bop_total_units
      ELSE 0
      END AS ly_actual_inv_bop_total_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_regular_retail_amount
      ELSE 0
      END AS ly_actual_inv_eop_regular_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_clearance_retail_amount
      ELSE 0
      END AS ly_actual_inv_eop_clearance_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_total_retail_amount
      ELSE 0
      END AS ly_actual_inv_eop_total_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_regular_cost_amount
      ELSE 0
      END AS ly_actual_inv_eop_regular_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_clearance_cost_amount
      ELSE 0
      END AS ly_actual_inv_eop_clearance_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_total_cost_amount
      ELSE 0
      END AS ly_actual_inv_eop_total_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_regular_units
      ELSE 0
      END AS ly_actual_inv_eop_regular_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_clearance_units
      ELSE 0
      END AS ly_actual_inv_eop_clearance_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_inv_eop_total_units
      ELSE 0
      END AS ly_actual_inv_eop_total_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_transfer_out_racking_retail_amount
      ELSE 0
      END AS ly_actual_transfer_out_racking_retail_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_transfer_out_racking_cost_amount
      ELSE 0
      END AS ly_actual_transfer_out_racking_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_transfer_out_racking_units
      ELSE 0
      END AS ly_actual_transfer_out_racking_units,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_rtv_total_cost_amount
      ELSE 0
      END AS ly_actual_rtv_total_cost_amount,
      CASE
      WHEN a010.month_num <= l010.last_completed_fiscal_month_num
      THEN a010.actual_rtv_total_units
      ELSE 0
      END AS ly_actual_rtv_total_units,
     a010.planned_net_sales_total_units AS ly_planned_net_sales_total_units,
     a010.planned_receipts_total_retail_amount AS ly_planned_receipts_total_retail_amount,
     a010.planned_receipts_total_cost_amount AS ly_planned_receipts_total_cost_amount,
     a010.planned_receipts_total_units AS ly_planned_receipts_total_units,
     a010.planned_inv_total_retail_amount AS ly_planned_inv_total_retail_amount,
     a010.planned_inv_total_cost_amount AS ly_planned_inv_total_cost_amount,
     a010.planned_inv_total_units AS ly_planned_inv_total_units,
     a010.planned_inv_bop_total_units AS ly_planned_inv_bop_total_units,
     a010.planned_inv_bop_total_cost_amount AS ly_planned_inv_bop_total_cost_amount,
     a010.planned_rack_transfer_cost_amount AS ly_planned_rack_transfer_cost_amount,
     a010.planned_rack_transfer_units AS ly_planned_rack_transfer_units,
     a010.planned_rtv_total_cost_amount AS ly_planned_rtv_total_cost_amount,
     a010.planned_rtv_total_units AS ly_planned_rtv_total_units,
     l010.dw_batch_dt AS dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_ap_plan_actual_suppgrp_banner_fact AS a010
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_spe_profitability_expectation_vw AS mspe0 
     ON LOWER(a010.supplier_group) = LOWER(mspe0.supplier_group) 
     AND a010.dept_num = mspe0.department_number 
     AND a010.banner_num = mspe0.banner_num 
     AND a010.fiscal_year_num = mspe0.year_num 
     AND LOWER(a010.channel_country) = LOWER(mspe0.selling_country)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.month_cal_vw AS c010 
     ON a010.month_num = c010.last_year_month_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l010 
     ON LOWER(l010.interface_code) = LOWER('MERCH_NAP_SPP_DLY')
    WHERE a010.fiscal_year_num >= (SELECT current_fiscal_year_num - 2
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPP_DLY'))
     AND a010.fiscal_year_num <= (SELECT current_fiscal_year_num + 1
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPP_DLY'))) AS I01
 GROUP BY supplier_group,
  dept_num,
  banner_num,
  channel_country,
  month_num,
  elapsed_month_flag,
  fiscal_year_num);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
