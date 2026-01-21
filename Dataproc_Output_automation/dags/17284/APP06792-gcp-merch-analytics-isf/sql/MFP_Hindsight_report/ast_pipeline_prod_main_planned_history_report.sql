--main script that when it runs, either everyday or weekly to update the table 

/*
 * 
Name: Planned history report
Project:  Planned History Report
Purpose: Query that builds the backend data for Planned History Report in Cost
Variable(s):    {{params.environment_schema}} T2DL_DAS_OPEN_TO_BUY
                {{params.env_suffix}} '' or '_dev' tablesuffix for prod testing
ast_pipeline_prod_main_planned_history_report.sql
Author: Paria Avij
Date Created: 02/03/23
Date Last Updated: 07/27/23

Datalab: t2dl_das_open_to_buy

*/

-- creating a week to month mapping for fiscal_year_num>=2022

--drop table hist_date;


BEGIN TRANSACTION;
CREATE TEMPORARY TABLE IF NOT EXISTS hist_date
CLUSTER BY week_idnt
AS
SELECT week_idnt,
 month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
    'FY' || SUBSTR(SUBSTR(CAST(fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || quarter_abrv AS quarter,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE fiscal_year_num >= 2022
GROUP BY week_idnt,
 month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 quarter,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_loc_base
CLUSTER BY channel_num
AS
SELECT channel_num,
 channel_desc,
 channel_brand,
 channel_country,
   FORMAT('%11d', channel_num) || ',' || channel_desc AS channel,
  CASE
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Rack'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Rack'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(channel_brand) IN (LOWER('Nordstrom'), LOWER('Nordstrom_Rack'))
 AND channel_num IN (110, 120, 140, 210, 220, 240, 250, 260, 310, 930, 940, 990, 111, 121, 211, 221, 261, 311)
GROUP BY channel_brand,
 channel_country,
 channel_num,
 channel_desc;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_receipt
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(sp_receipts_reserve_cost_amt) AS sp_rcpts_reserve_c,
 SUM(sp_receipts_active_cost_amt) AS sp_rcpts_active_c,
 SUM(sp_receipts_inactive_cost_amt) AS sp_rcpts_inactive_c,
 SUM(op_receipts_active_cost_amt) AS op_rcpts_active_c,
 SUM(op_receipts_inactive_cost_amt) AS op_rcpts_inactive_c,
 SUM(op_receipts_reserve_cost_amt) AS op_rcpts_reserve_c,
  SUM(sp_receipts_active_cost_amt) - SUM(sp_receipts_reserve_cost_amt) AS sp_rcpts_less_reserve_c,
  SUM(op_receipts_active_cost_amt) - SUM(op_receipts_reserve_cost_amt) AS op_rcpts_less_reserve_c,
 SUM(op_beginning_of_period_active_cost_amt) AS op_bop_active_c,
 SUM(sp_beginning_of_period_active_cost_amt) AS sp_bop_active_c
FROM (SELECT a.snapshot_plan_month_idnt AS month_plans,
   a.dept_num AS dept_id,
   a.week_num AS week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
    CASE
    WHEN a.banner_country_num = 1
    THEN 'US Nordstrom'
    WHEN a.banner_country_num = 2
    THEN 'CA Nordstrom'
    WHEN a.banner_country_num = 3
    THEN 'US Rack'
    WHEN a.banner_country_num = 4
    THEN 'CA Rack'
    ELSE FORMAT('%4d', 0)
    END AS banner,
    CASE
    WHEN a.banner_country_num IN (1, 3)
    THEN 'US'
    WHEN a.banner_country_num IN (2, 4)
    THEN 'CA'
    ELSE FORMAT('%4d', 0)
    END AS country,
   a.sp_receipts_reserve_cost_amt,
   a.sp_receipts_active_cost_amt,
   a.sp_receipts_inactive_cost_amt,
   a.op_receipts_active_cost_amt,
   a.op_receipts_reserve_cost_amt,
   a.op_receipts_inactive_cost_amt,
   a.op_beginning_of_period_active_cost_amt,
   a.sp_beginning_of_period_active_cost_amt
  FROM t2dl_das_open_to_buy.mfp_cost_plan_actual_banner_country_history AS a
   INNER JOIN hist_date AS b ON a.week_num = b.week_idnt
  WHERE a.fulfill_type_num = 3
  GROUP BY month_plans,
   dept_id,
   week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   banner,
   country,
   a.sp_receipts_reserve_cost_amt,
   a.sp_receipts_active_cost_amt,
   a.sp_receipts_inactive_cost_amt,
   a.op_receipts_active_cost_amt,
   a.op_receipts_reserve_cost_amt,
   a.op_receipts_inactive_cost_amt,
   a.op_beginning_of_period_active_cost_amt,
   a.sp_beginning_of_period_active_cost_amt) AS c
GROUP BY month_plans,
 dept_id,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 banner,
 country;


CREATE TEMPORARY TABLE IF NOT EXISTS month_plan_add AS
SELECT dept_num,
 week_num,
 banner_country_num,
 fulfill_type_num,
 cp_beginning_of_period_active_cost_amt,
 cp_beginning_of_period_active_inventory_target_qty,
 cp_beginning_of_period_active_qty,
 cp_beginning_of_period_inactive_cost_amt,
 cp_beginning_of_period_inactive_qty,
 cp_ending_of_period_active_cost_amt,
 cp_ending_of_period_active_qty,
 cp_ending_of_period_inactive_cost_amt,
 cp_ending_of_period_inactive_qty,
 cp_transfer_in_active_cost_amt,
 cp_transfer_in_active_qty,
 cp_transfer_out_active_cost_amt,
 cp_transfer_out_active_qty,
 cp_transfer_in_inactive_cost_amt,
 cp_transfer_in_inactive_qty,
 cp_transfer_out_inactive_cost_amt,
 cp_transfer_out_inactive_qty,
 cp_transfer_in_other_cost_amt,
 cp_transfer_in_other_qty,
 cp_transfer_out_other_cost_amt,
 cp_transfer_out_other_qty,
 cp_transfer_in_racking_cost_amt,
 cp_transfer_in_racking_qty,
 cp_transfer_out_racking_cost_amt,
 cp_transfer_out_racking_qty,
 cp_mark_out_of_stock_active_cost_amt,
 cp_mark_out_of_stock_active_qty,
 cp_mark_out_of_stock_inactive_cost_amt,
 cp_mark_out_of_stock_inactive_qty,
 cp_receipts_active_cost_amt,
 cp_receipts_active_qty,
 cp_receipts_inactive_cost_amt,
 cp_receipts_inactive_qty,
 cp_receipts_reserve_cost_amt,
 cp_receipts_reserve_qty,
 cp_reclass_in_cost_amt,
 cp_reclass_in_qty,
 cp_reclass_out_cost_amt,
 cp_reclass_out_qty,
 cp_return_to_vendor_active_cost_amt,
 cp_return_to_vendor_active_qty,
 cp_return_to_vendor_inactive_cost_amt,
 cp_return_to_vendor_inactive_qty,
 cp_vendor_funds_cost_amt,
 cp_discount_terms_cost_amt,
 cp_merch_margin_retail_amt,
 cp_inventory_movement_in_dropship_cost_amt,
 cp_inventory_movement_in_dropship_qty,
 cp_inventory_movement_out_dropship_cost_amt,
 cp_inventory_movement_out_dropship_qty,
 cp_other_inventory_adjustment_cost_amt,
 cp_other_inventory_adjustment_qty,
 cp_open_to_buy_receipts_active_cost_amt,
 cp_open_to_buy_receipts_inactive_cost_amt,
 op_beginning_of_period_active_cost_amt,
 op_beginning_of_period_active_inventory_target_qty,
 op_beginning_of_period_active_qty,
 op_beginning_of_period_inactive_cost_amt,
 op_beginning_of_period_inactive_qty,
 op_ending_of_period_active_cost_amt,
 op_ending_of_period_active_qty,
 op_ending_of_period_inactive_cost_amt,
 op_ending_of_period_inactive_qty,
 op_transfer_in_active_cost_amt,
 op_transfer_in_active_qty,
 op_transfer_out_active_cost_amt,
 op_transfer_out_active_qty,
 op_transfer_in_inactive_cost_amt,
 op_transfer_in_inactive_qty,
 op_transfer_out_inactive_cost_amt,
 op_transfer_out_inactive_qty,
 op_transfer_in_other_cost_amt,
 op_transfer_in_other_qty,
 op_transfer_out_other_cost_amt,
 op_transfer_out_other_qty,
 op_transfer_in_racking_cost_amt,
 op_transfer_in_racking_qty,
 op_transfer_out_racking_cost_amt,
 op_transfer_out_racking_qty,
 op_mark_out_of_stock_active_cost_amt,
 op_mark_out_of_stock_active_qty,
 op_mark_out_of_stock_inactive_cost_amt,
 op_mark_out_of_stock_inactive_qty,
 op_receipts_active_cost_amt,
 op_receipts_active_qty,
 op_receipts_inactive_cost_amt,
 op_receipts_inactive_qty,
 op_receipts_reserve_cost_amt,
 op_receipts_reserve_qty,
 op_reclass_in_cost_amt,
 op_reclass_in_qty,
 op_reclass_out_cost_amt,
 op_reclass_out_qty,
 op_return_to_vendor_active_cost_amt,
 op_return_to_vendor_active_qty,
 op_return_to_vendor_inactive_cost_amt,
 op_return_to_vendor_inactive_qty,
 op_vendor_funds_cost_amt,
 op_discount_terms_cost_amt,
 op_merch_margin_retail_amt,
 op_inventory_movement_in_dropship_cost_amt,
 op_inventory_movement_in_dropship_qty,
 op_inventory_movement_out_dropship_cost_amt,
 op_inventory_movement_out_dropship_qty,
 op_other_inventory_adjustment_cost_amt,
 op_other_inventory_adjustment_qty,
 sp_beginning_of_period_active_cost_amt,
 sp_beginning_of_period_active_inventory_target_qty,
 sp_beginning_of_period_active_qty,
 sp_beginning_of_period_inactive_cost_amt,
 sp_beginning_of_period_inactive_qty,
 sp_ending_of_period_active_cost_amt,
 sp_ending_of_period_active_qty,
 sp_ending_of_period_inactive_cost_amt,
 sp_ending_of_period_inactive_qty,
 sp_transfer_in_active_cost_amt,
 sp_transfer_in_active_qty,
 sp_transfer_out_active_cost_amt,
 sp_transfer_out_active_qty,
 sp_transfer_in_inactive_cost_amt,
 sp_transfer_in_inactive_qty,
 sp_transfer_out_inactive_cost_amt,
 sp_transfer_out_inactive_qty,
 sp_transfer_in_other_cost_amt,
 sp_transfer_in_other_qty,
 sp_transfer_out_other_cost_amt,
 sp_transfer_out_other_qty,
 sp_transfer_in_racking_cost_amt,
 sp_transfer_in_racking_qty,
 sp_transfer_out_racking_cost_amt,
 sp_transfer_out_racking_qty,
 sp_mark_out_of_stock_active_cost_amt,
 sp_mark_out_of_stock_active_qty,
 sp_mark_out_of_stock_inactive_cost_amt,
 sp_mark_out_of_stock_inactive_qty,
 sp_receipts_active_cost_amt,
 sp_receipts_active_qty,
 sp_receipts_inactive_cost_amt,
 sp_receipts_inactive_qty,
 sp_receipts_reserve_cost_amt,
 sp_receipts_reserve_qty,
 sp_reclass_in_cost_amt,
 sp_reclass_in_qty,
 sp_reclass_out_cost_amt,
 sp_reclass_out_qty,
 sp_return_to_vendor_active_cost_amt,
 sp_return_to_vendor_active_qty,
 sp_return_to_vendor_inactive_cost_amt,
 sp_return_to_vendor_inactive_qty,
 sp_vendor_funds_cost_amt,
 sp_discount_terms_cost_amt,
 sp_merch_margin_retail_amt,
 sp_inventory_movement_in_dropship_cost_amt,
 sp_inventory_movement_in_dropship_qty,
 sp_inventory_movement_out_dropship_cost_amt,
 sp_inventory_movement_out_dropship_qty,
 sp_other_inventory_adjustment_cost_amt,
 sp_other_inventory_adjustment_qty,
 sp_open_to_buy_receipts_active_cost_amt,
 sp_open_to_buy_receipts_inactive_cost_amt,
 ty_beginning_of_period_active_cost_amt,
 ty_beginning_of_period_active_qty,
 ty_beginning_of_period_inactive_cost_amt,
 ty_beginning_of_period_inactive_qty,
 ty_ending_of_period_active_cost_amt,
 ty_ending_of_period_active_qty,
 ty_ending_of_period_inactive_cost_amt,
 ty_ending_of_period_inactive_qty,
 ty_transfer_in_active_cost_amt,
 ty_transfer_in_active_qty,
 ty_transfer_out_active_cost_amt,
 ty_transfer_out_active_qty,
 ty_transfer_in_inactive_cost_amt,
 ty_transfer_in_inactive_qty,
 ty_transfer_out_inactive_cost_amt,
 ty_transfer_out_inactive_qty,
 ty_transfer_in_other_cost_amt,
 ty_transfer_in_other_qty,
 ty_transfer_out_other_cost_amt,
 ty_transfer_out_other_qty,
 ty_transfer_in_racking_cost_amt,
 ty_transfer_in_racking_qty,
 ty_transfer_out_racking_cost_amt,
 ty_transfer_out_racking_qty,
 ty_mark_out_of_stock_active_cost_amt,
 ty_mark_out_of_stock_active_qty,
 ty_mark_out_of_stock_inactive_cost_amt,
 ty_mark_out_of_stock_inactive_qty,
 ty_receipts_active_cost_amt,
 ty_receipts_active_qty,
 ty_receipts_inactive_cost_amt,
 ty_receipts_inactive_qty,
 ty_rp_receipts_active_cost_amt,
 ty_non_rp_receipts_active_cost_amt,
 ty_reclass_in_cost_amt,
 ty_reclass_in_qty,
 ty_reclass_out_cost_amt,
 ty_reclass_out_qty,
 ty_return_to_vendor_active_cost_amt,
 ty_return_to_vendor_active_qty,
 ty_return_to_vendor_inactive_cost_amt,
 ty_return_to_vendor_inactive_qty,
 ty_vendor_funds_cost_amt,
 ty_discount_terms_cost_amt,
 ty_merch_margin_retail_amt,
 ty_shrink_banner_cost_amt,
 ty_shrink_banner_qty,
 ty_other_inventory_adj_cost,
 ty_merch_margin_charges_cost,
 ly_beginning_of_period_active_cost_amt,
 ly_beginning_of_period_active_qty,
 ly_beginning_of_period_inactive_cost_amt,
 ly_beginning_of_period_inactive_qty,
 ly_ending_of_period_active_cost_amt,
 ly_ending_of_period_active_qty,
 ly_ending_of_period_inactive_cost_amt,
 ly_ending_of_period_inactive_qty,
 ly_transfer_in_active_cost_amt,
 ly_transfer_in_active_qty,
 ly_transfer_out_active_cost_amt,
 ly_transfer_out_active_qty,
 ly_transfer_in_inactive_cost_amt,
 ly_transfer_in_inactive_qty,
 ly_transfer_out_inactive_cost_amt,
 ly_transfer_out_inactive_qty,
 ly_transfer_in_other_cost_amt,
 ly_transfer_in_other_qty,
 ly_transfer_out_other_cost_amt,
 ly_transfer_out_other_qty,
 ly_transfer_in_racking_cost_amt,
 ly_transfer_in_racking_qty,
 ly_transfer_out_racking_cost_amt,
 ly_transfer_out_racking_qty,
 ly_mark_out_of_stock_active_cost_amt,
 ly_mark_out_of_stock_active_qty,
 ly_mark_out_of_stock_inactive_cost_amt,
 ly_mark_out_of_stock_inactive_qty,
 ly_receipts_active_cost_amt,
 ly_receipts_active_qty,
 ly_receipts_inactive_cost_amt,
 ly_receipts_inactive_qty,
 ly_rp_receipts_active_cost_amt,
 ly_non_rp_receipts_active_cost_amt,
 ly_reclass_in_cost_amt,
 ly_reclass_in_qty,
 ly_reclass_out_cost_amt,
 ly_reclass_out_qty,
 ly_return_to_vendor_active_cost_amt,
 ly_return_to_vendor_active_qty,
 ly_return_to_vendor_inactive_cost_amt,
 ly_return_to_vendor_inactive_qty,
 ly_vendor_funds_cost_amt,
 ly_discount_terms_cost_amt,
 ly_merch_margin_retail_amt,
 ly_shrink_banner_cost_amt,
 ly_shrink_banner_qty,
 ly_other_inventory_adj_cost,
 ly_merch_margin_charges_cost,
 lly_beginning_of_period_active_cost_amt,
 lly_beginning_of_period_active_qty,
 lly_beginning_of_period_inactive_cost_amt,
 lly_beginning_of_period_inactive_qty,
 lly_ending_of_period_active_cost_amt,
 lly_ending_of_period_active_qty,
 lly_ending_of_period_inactive_cost_amt,
 lly_ending_of_period_inactive_qty,
 lly_transfer_in_active_cost_amt,
 lly_transfer_in_active_qty,
 lly_transfer_out_active_cost_amt,
 lly_transfer_out_active_qty,
 lly_transfer_in_inactive_cost_amt,
 lly_transfer_in_inactive_qty,
 lly_transfer_out_inactive_cost_amt,
 lly_transfer_out_inactive_qty,
 lly_transfer_in_other_cost_amt,
 lly_transfer_in_other_qty,
 lly_transfer_out_other_cost_amt,
 lly_transfer_out_other_qty,
 lly_transfer_in_racking_cost_amt,
 lly_transfer_in_racking_qty,
 lly_transfer_out_racking_cost_amt,
 lly_transfer_out_racking_qty,
 lly_mark_out_of_stock_active_cost_amt,
 lly_mark_out_of_stock_active_qty,
 lly_mark_out_of_stock_inactive_cost_amt,
 lly_mark_out_of_stock_inactive_qty,
 lly_receipts_active_cost_amt,
 lly_receipts_active_qty,
 lly_receipts_inactive_cost_amt,
 lly_receipts_inactive_qty,
 lly_rp_receipts_active_cost_amt,
 lly_non_rp_receipts_active_cost_amt,
 lly_reclass_in_cost_amt,
 lly_reclass_in_qty,
 lly_reclass_out_cost_amt,
 lly_reclass_out_qty,
 lly_return_to_vendor_active_cost_amt,
 lly_return_to_vendor_active_qty,
 lly_return_to_vendor_inactive_cost_amt,
 lly_return_to_vendor_inactive_qty,
 lly_vendor_funds_cost_amt,
 lly_discount_terms_cost_amt,
 lly_merch_margin_retail_amt,
 lly_shrink_banner_cost_amt,
 lly_shrink_banner_qty,
 lly_other_inventory_adj_cost,
 lly_merch_margin_charges_cost,
 ly_trueday_beginning_of_period_active_cost_amt,
 ly_trueday_beginning_of_period_active_qty,
 ly_trueday_beginning_of_period_inactive_cost_amt,
 ly_trueday_beginning_of_period_inactive_qty,
 ly_trueday_ending_of_period_active_cost_amt,
 ly_trueday_ending_of_period_active_qty,
 ly_trueday_ending_of_period_inactive_cost_amt,
 ly_trueday_ending_of_period_inactive_qty,
 ly_trueday_transfer_in_active_cost_amt,
 ly_trueday_transfer_in_active_qty,
 ly_trueday_transfer_out_active_cost_amt,
 ly_trueday_transfer_out_active_qty,
 ly_trueday_transfer_in_inactive_cost_amt,
 ly_trueday_transfer_in_inactive_qty,
 ly_trueday_transfer_out_inactive_cost_amt,
 ly_trueday_transfer_out_inactive_qty,
 ly_trueday_transfer_in_other_cost_amt,
 ly_trueday_transfer_in_other_qty,
 ly_trueday_transfer_out_other_cost_amt,
 ly_trueday_transfer_out_other_qty,
 ly_trueday_transfer_in_racking_cost_amt,
 ly_trueday_transfer_in_racking_qty,
 ly_trueday_transfer_out_racking_cost_amt,
 ly_trueday_transfer_out_racking_qty,
 ly_trueday_mark_out_of_stock_active_cost_amt,
 ly_trueday_mark_out_of_stock_active_qty,
 ly_trueday_mark_out_of_stock_inactive_cost_amt,
 ly_trueday_mark_out_of_stock_inactive_qty,
 ly_trueday_receipts_active_cost_amt,
 ly_trueday_receipts_active_qty,
 ly_trueday_receipts_inactive_cost_amt,
 ly_trueday_receipts_inactive_qty,
 ly_trueday_rp_receipts_active_cost_amt,
 ly_trueday_non_rp_receipts_active_cost_amt,
 ly_trueday_reclass_in_cost_amt,
 ly_trueday_reclass_in_qty,
 ly_trueday_reclass_out_cost_amt,
 ly_trueday_reclass_out_qty,
 ly_trueday_return_to_vendor_active_cost_amt,
 ly_trueday_return_to_vendor_active_qty,
 ly_trueday_return_to_vendor_inactive_cost_amt,
 ly_trueday_return_to_vendor_inactive_qty,
 ly_trueday_vendor_funds_cost_amt,
 ly_trueday_discount_terms_cost_amt,
 ly_trueday_merch_margin_retail_amt,
 ly_trueday_shrink_banner_cost_amt,
 ly_trueday_shrink_banner_qty,
 ly_trueday_other_inventory_adj_cost,
 ly_trueday_merch_margin_charges_cost,
 lly_trueday_beginning_of_period_active_cost_amt,
 lly_trueday_beginning_of_period_active_qty,
 lly_trueday_beginning_of_period_inactive_cost_amt,
 lly_trueday_beginning_of_period_inactive_qty,
 lly_trueday_ending_of_period_active_cost_amt,
 lly_trueday_ending_of_period_active_qty,
 lly_trueday_ending_of_period_inactive_cost_amt,
 lly_trueday_ending_of_period_inactive_qty,
 lly_trueday_transfer_in_active_cost_amt,
 lly_trueday_transfer_in_active_qty,
 lly_trueday_transfer_out_active_cost_amt,
 lly_trueday_transfer_out_active_qty,
 lly_trueday_transfer_in_inactive_cost_amt,
 lly_trueday_transfer_in_inactive_qty,
 lly_trueday_transfer_out_inactive_cost_amt,
 lly_trueday_transfer_out_inactive_qty,
 lly_trueday_transfer_in_other_cost_amt,
 lly_trueday_transfer_in_other_qty,
 lly_trueday_transfer_out_other_cost_amt,
 lly_trueday_transfer_out_other_qty,
 lly_trueday_transfer_in_racking_cost_amt,
 lly_trueday_transfer_in_racking_qty,
 lly_trueday_transfer_out_racking_cost_amt,
 lly_trueday_transfer_out_racking_qty,
 lly_trueday_mark_out_of_stock_active_cost_amt,
 lly_trueday_mark_out_of_stock_active_qty,
 lly_trueday_mark_out_of_stock_inactive_cost_amt,
 lly_trueday_mark_out_of_stock_inactive_qty,
 lly_trueday_receipts_active_cost_amt,
 lly_trueday_receipts_active_qty,
 lly_trueday_receipts_inactive_cost_amt,
 lly_trueday_receipts_inactive_qty,
 lly_trueday_rp_receipts_active_cost_amt,
 lly_trueday_non_rp_receipts_active_cost_amt,
 lly_trueday_reclass_in_cost_amt,
 lly_trueday_reclass_in_qty,
 lly_trueday_reclass_out_cost_amt,
 lly_trueday_reclass_out_qty,
 lly_trueday_return_to_vendor_active_cost_amt,
 lly_trueday_return_to_vendor_active_qty,
 lly_trueday_return_to_vendor_inactive_cost_amt,
 lly_trueday_return_to_vendor_inactive_qty,
 lly_trueday_vendor_funds_cost_amt,
 lly_trueday_discount_terms_cost_amt,
 lly_trueday_merch_margin_retail_amt,
 lly_trueday_shrink_banner_cost_amt,
 lly_trueday_shrink_banner_qty,
 lly_trueday_other_inventory_adj_cost,
 lly_trueday_merch_margin_charges_cost,
 dw_batch_date,
 dw_sys_load_tmstp,
  (SELECT month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE CURRENT_DATE('PST8PDT') = day_date) AS month_plans
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_banner_country_fact AS a;


CREATE TEMPORARY TABLE IF NOT EXISTS metric_cur_month
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(sp_receipts_reserve_cost_amt) AS sp_rcpts_reserve_c,
 SUM(sp_receipts_active_cost_amt) AS sp_rcpts_active_c,
 SUM(sp_receipts_inactive_cost_amt) AS sp_rcpts_inactive_c,
 SUM(op_receipts_active_cost_amt) AS op_rcpts_active_c,
 SUM(op_receipts_inactive_cost_amt) AS op_rcpts_inactive_c,
 SUM(op_receipts_reserve_cost_amt) AS op_rcpts_reserve_c,
  SUM(sp_receipts_active_cost_amt) - SUM(sp_receipts_reserve_cost_amt) AS sp_rcpts_less_reserve_c,
  SUM(op_receipts_active_cost_amt) - SUM(op_receipts_reserve_cost_amt) AS op_rcpts_less_reserve_c,
 SUM(op_beginning_of_period_active_cost_amt) AS op_bop_active_c,
 SUM(sp_beginning_of_period_active_cost_amt) AS sp_bop_active_c
FROM (SELECT a.month_plans,
   a.dept_num AS dept_id,
   a.week_num AS week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
    CASE
    WHEN a.banner_country_num = 1
    THEN 'US Nordstrom'
    WHEN a.banner_country_num = 2
    THEN 'CA Nordstrom'
    WHEN a.banner_country_num = 3
    THEN 'US Rack'
    WHEN a.banner_country_num = 4
    THEN 'CA Rack'
    ELSE FORMAT('%4d', 0)
    END AS banner,
    CASE
    WHEN a.banner_country_num IN (1, 3)
    THEN 'US'
    WHEN a.banner_country_num IN (2, 4)
    THEN 'CA'
    ELSE FORMAT('%4d', 0)
    END AS country,
   a.sp_receipts_reserve_cost_amt,
   a.sp_receipts_active_cost_amt,
   a.sp_receipts_inactive_cost_amt,
   a.op_receipts_active_cost_amt,
   a.op_receipts_reserve_cost_amt,
   a.op_receipts_inactive_cost_amt,
   a.op_beginning_of_period_active_cost_amt,
   a.sp_beginning_of_period_active_cost_amt
  FROM month_plan_add AS a
   INNER JOIN hist_date AS b ON a.week_num = b.week_idnt
  WHERE a.fulfill_type_num = 3
  GROUP BY a.month_plans,
   dept_id,
   week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   banner,
   country,
   a.sp_receipts_reserve_cost_amt,
   a.sp_receipts_active_cost_amt,
   a.sp_receipts_inactive_cost_amt,
   a.op_receipts_active_cost_amt,
   a.op_receipts_reserve_cost_amt,
   a.op_receipts_inactive_cost_amt,
   a.op_beginning_of_period_active_cost_amt,
   a.sp_beginning_of_period_active_cost_amt) AS c
GROUP BY month_plans,
 dept_id,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 banner,
 country;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_cur_comb
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT *
FROM metric_cur_month
WHERE month_plans = month_idnt
UNION ALL
SELECT *
FROM hist_receipt
WHERE month_plans NOT IN (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE CURRENT_DATE('PST8PDT') = day_date)
 OR month_plans <> month_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_sale
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(sp_net_sales_cost_amt) AS sp_net_sales_c,
 SUM(ly_net_sales_cost_amt) AS ly_net_sales_c,
 SUM(op_net_sales_cost_amt) AS op_net_sales_c,
 SUM(ty_net_sales_cost_amt) AS ty_net_sales_c
FROM (SELECT a.snapshot_plan_month_idnt AS month_plans,
   a.dept_num AS dept_id,
   a.week_num AS week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   c.banner,
   c.channel_country AS country,
   c.channel,
   c.channel_num,
   a.sp_net_sales_cost_amt,
   a.ly_net_sales_cost_amt,
   a.op_net_sales_cost_amt,
   a.ty_net_sales_cost_amt
  FROM t2dl_das_open_to_buy.mfp_cost_plan_actual_channel_history AS a
   INNER JOIN hist_date AS b ON a.week_num = b.week_idnt
   INNER JOIN hist_loc_base AS c ON a.channel_num = c.channel_num
  WHERE a.fulfill_type_num = 3
  GROUP BY month_plans,
   dept_id,
   week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   c.banner,
   country,
   c.channel,
   c.channel_num,
   a.sp_net_sales_cost_amt,
   a.ly_net_sales_cost_amt,
   a.op_net_sales_cost_amt,
   a.ty_net_sales_cost_amt) AS d
GROUP BY month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS cur_mnth_add AS
SELECT dept_num,
 week_num,
 channel_num,
 fulfill_type_num,
 cp_demand_flash_retail_amt,
 cp_demand_persistent_retail_amt,
 cp_demand_total_qty,
 cp_demand_total_retail_amt,
 cp_gross_margin_retail_amt,
 cp_gross_sales_retail_amt,
 cp_gross_sales_qty,
 cp_net_sales_retail_amt,
 cp_net_sales_qty,
 cp_net_sales_cost_amt,
 cp_net_sales_trunk_club_retail_amt,
 cp_net_sales_trunk_club_qty,
 cp_returns_retail_amt,
 cp_returns_qty,
 cp_shrink_cost_amt,
 cp_shrink_qty,
 op_demand_flash_retail_amt,
 op_demand_persistent_retail_amt,
 op_demand_total_qty,
 op_demand_total_retail_amt,
 op_gross_margin_retail_amt,
 op_gross_sales_retail_amt,
 op_gross_sales_qty,
 op_net_sales_retail_amt,
 op_net_sales_qty,
 op_net_sales_cost_amt,
 op_net_sales_trunk_club_retail_amt,
 op_net_sales_trunk_club_qty,
 op_returns_retail_amt,
 op_returns_qty,
 op_shrink_cost_amt,
 op_shrink_qty,
 sp_demand_flash_retail_amt,
 sp_demand_persistent_retail_amt,
 sp_demand_total_qty,
 sp_demand_total_retail_amt,
 sp_gross_margin_retail_amt,
 sp_gross_sales_retail_amt,
 sp_gross_sales_qty,
 sp_net_sales_retail_amt,
 sp_net_sales_qty,
 sp_net_sales_cost_amt,
 sp_net_sales_trunk_club_retail_amt,
 sp_net_sales_trunk_club_qty,
 sp_returns_retail_amt,
 sp_returns_qty,
 sp_shrink_cost_amt,
 sp_shrink_qty,
 ty_demand_flash_retail_amt,
 ty_demand_persistent_retail_amt,
 ty_demand_total_qty,
 ty_demand_total_retail_amt,
 ty_gross_margin_retail_amt,
 ty_gross_sales_retail_amt,
 ty_gross_sales_qty,
 ty_net_sales_retail_amt,
 ty_net_sales_qty,
 ty_net_sales_cost_amt,
 ty_net_sales_trunk_club_retail_amt,
 ty_net_sales_trunk_club_qty,
 ty_returns_retail_amt,
 ty_returns_qty,
 ty_shrink_cost_amt,
 ty_shrink_qty,
 ly_demand_flash_retail_amt,
 ly_demand_persistent_retail_amt,
 ly_demand_total_qty,
 ly_demand_total_retail_amt,
 ly_gross_margin_retail_amt,
 ly_gross_sales_retail_amt,
 ly_gross_sales_qty,
 ly_net_sales_retail_amt,
 ly_net_sales_qty,
 ly_net_sales_cost_amt,
 ly_net_sales_trunk_club_retail_amt,
 ly_net_sales_trunk_club_qty,
 ly_returns_retail_amt,
 ly_returns_qty,
 ly_shrink_cost_amt,
 ly_shrink_qty,
 lly_demand_flash_retail_amt,
 lly_demand_persistent_retail_amt,
 lly_demand_total_qty,
 lly_demand_total_retail_amt,
 lly_gross_margin_retail_amt,
 lly_gross_sales_retail_amt,
 lly_gross_sales_qty,
 lly_net_sales_retail_amt,
 lly_net_sales_qty,
 lly_net_sales_cost_amt,
 lly_net_sales_trunk_club_retail_amt,
 lly_net_sales_trunk_club_qty,
 lly_returns_retail_amt,
 lly_returns_qty,
 lly_shrink_cost_amt,
 lly_shrink_qty,
 ly_trueday_demand_flash_retail_amt,
 ly_trueday_demand_persistent_retail_amt,
 ly_trueday_demand_total_qty,
 ly_trueday_demand_total_retail_amt,
 ly_trueday_gross_margin_retail_amt,
 ly_trueday_gross_sales_retail_amt,
 ly_trueday_gross_sales_qty,
 ly_trueday_net_sales_retail_amt,
 ly_trueday_net_sales_qty,
 ly_trueday_net_sales_cost_amt,
 ly_trueday_net_sales_trunk_club_retail_amt,
 ly_trueday_net_sales_trunk_club_qty,
 ly_trueday_returns_retail_amt,
 ly_trueday_returns_qty,
 ly_trueday_shrink_cost_amt,
 ly_trueday_shrink_qty,
 lly_trueday_demand_flash_retail_amt,
 lly_trueday_demand_persistent_retail_amt,
 lly_trueday_demand_total_qty,
 lly_trueday_demand_total_retail_amt,
 lly_trueday_gross_margin_retail_amt,
 lly_trueday_gross_sales_retail_amt,
 lly_trueday_gross_sales_qty,
 lly_trueday_net_sales_retail_amt,
 lly_trueday_net_sales_qty,
 lly_trueday_net_sales_cost_amt,
 lly_trueday_net_sales_trunk_club_retail_amt,
 lly_trueday_net_sales_trunk_club_qty,
 lly_trueday_returns_retail_amt,
 lly_trueday_returns_qty,
 lly_trueday_shrink_cost_amt,
 lly_trueday_shrink_qty,
 dw_batch_date,
 dw_sys_load_tmstp,
  (SELECT month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE CURRENT_DATE('PST8PDT') = day_date) AS month_plans
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_channel_fact AS a;


CREATE TEMPORARY TABLE IF NOT EXISTS cur_month_sale
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(sp_net_sales_cost_amt) AS sp_net_sales_c,
 SUM(ly_net_sales_cost_amt) AS ly_net_sales_c,
 SUM(op_net_sales_cost_amt) AS op_net_sales_c,
 SUM(ty_net_sales_cost_amt) AS ty_net_sales_c
FROM (SELECT a.month_plans,
   a.dept_num AS dept_id,
   a.week_num AS week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   c.banner,
   c.channel_country AS country,
   c.channel,
   c.channel_num,
   a.sp_net_sales_cost_amt,
   a.ly_net_sales_cost_amt,
   a.op_net_sales_cost_amt,
   a.ty_net_sales_cost_amt
  FROM cur_mnth_add AS a
   INNER JOIN hist_date AS b ON a.week_num = b.week_idnt
   INNER JOIN hist_loc_base AS c ON a.channel_num = c.channel_num
  WHERE a.fulfill_type_num = 3
  GROUP BY a.month_plans,
   dept_id,
   week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   c.banner,
   country,
   c.channel,
   c.channel_num,
   a.sp_net_sales_cost_amt,
   a.ly_net_sales_cost_amt,
   a.op_net_sales_cost_amt,
   a.ty_net_sales_cost_amt) AS d
GROUP BY month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS com_cur_hist_sale
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT *
FROM cur_month_sale
WHERE month_plans = month_idnt
UNION ALL
SELECT *
FROM hist_sale
WHERE month_plans NOT IN (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE CURRENT_DATE('PST8PDT') = day_date)
 OR month_plans <> month_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_rp_ant_spnd
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT dept_id,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 banner,
 country,
 month_plan AS month_plans,
 SUM(rp_antspnd_c) AS rp_anticipated_spend_c,
 SUM(rp_antspnd_u) AS rp_anticipated_spend_u
FROM (SELECT a.week_idnt AS week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   a.dept_idnt AS dept_id,
    CASE
    WHEN a.banner_id = 1
    THEN 'US Nordstrom'
    WHEN a.banner_id = 2
    THEN 'CA Nordstrom'
    WHEN a.banner_id = 3
    THEN 'US Rack'
    WHEN a.banner_id = 4
    THEN 'CA Rack'
    ELSE FORMAT('%4d', 0)
    END AS banner,
    CASE
    WHEN a.banner_id IN (1, 3)
    THEN 'US'
    WHEN a.banner_id IN (2, 4)
    THEN 'CA'
    ELSE FORMAT('%4d', 0)
    END AS country,
   a.category,
   a.supp_idnt,
   a.rp_antspnd_u,
   a.rp_antspnd_c,
   d.month_idnt AS month_plan
  FROM t2dl_das_open_to_buy.rp_anticipated_spend_hist AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS d ON a.rcd_load_date = d.day_date
   INNER JOIN hist_date AS b ON a.week_idnt = b.week_idnt
  GROUP BY week_id,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   dept_id,
   banner,
   country,
   a.category,
   a.supp_idnt,
   a.rp_antspnd_u,
   a.rp_antspnd_c,
   month_plan) AS c
GROUP BY dept_id,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 banner,
 country,
 month_plans;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_m_date
CLUSTER BY month_idnt
AS
SELECT month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
    'FY' || SUBSTR(SUBSTR(CAST(fiscal_year_num AS STRING), 1, 5), 3, 2) || ' ' || quarter_abrv AS quarter,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE fiscal_year_num >= 2022
GROUP BY month_idnt,
 month_label,
 quarter_idnt,
 quarter_label,
 quarter,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS month_to_max_dt AS
SELECT d.month_idnt,
 MAX(a.process_dt) AS load_date
FROM t2dl_das_open_to_buy.ab_cm_orders_historical AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS d ON a.process_dt = d.day_date
GROUP BY d.month_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_cm_f_1
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 fiscal_month_id,
 month_label,
 quarter,
 fiscal_year_num,
 quarter_idnt,
 SUM(`A121812`) AS inactive_cm_c,
 SUM(`A121813`) AS active_cm_c,
 SUM(`A121814`) AS nrp_cm_c,
 SUM(`A121815`) AS rp_cm_c
FROM (SELECT d.month_idnt AS month_plans,
   a.dept_id,
   b.banner,
   b.channel_country AS country,
   a.fiscal_month_id,
   e.month_label,
   e.quarter,
   e.fiscal_year_num,
   e.quarter_idnt,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('RKPACKHOLD')) AND LOWER(b.banner) IN (LOWER('US NORDSTROM'), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('RKPACKHOLD')) AND LOWER(b.banner) IN (LOWER('CA NORDSTROM'), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121812`,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER'), LOWER('REPLNSHMNT')) AND LOWER(b
       .banner) IN (LOWER('US NORDSTROM'), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER'), LOWER('REPLNSHMNT')) AND LOWER(b
       .banner) IN (LOWER('CA NORDSTROM'), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121813`,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER')) AND LOWER(b.banner) IN (LOWER('US NORDSTROM'
        ), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('BACKUP'), LOWER('FASHION'), LOWER('NRPREORDER')) AND LOWER(b.banner) IN (LOWER('CA NORDSTROM'
        ), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121814`,
    CASE
    WHEN LOWER(a.plan_type) IN (LOWER('REPLNSHMNT')) AND LOWER(b.banner) IN (LOWER('US NORDSTROM'), LOWER('US RACK'))
    THEN a.ttl_cost_us
    WHEN LOWER(a.plan_type) IN (LOWER('REPLNSHMNT')) AND LOWER(b.banner) IN (LOWER('CA NORDSTROM'), LOWER('CA RACK'))
    THEN a.ttl_cost_ca
    ELSE 0
    END AS `A121815`
  FROM t2dl_das_open_to_buy.ab_cm_orders_historical AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS d ON a.process_dt = d.day_date
   INNER JOIN hist_loc_base AS b ON a.org_id = b.channel_num
   INNER JOIN hist_m_date AS e ON CAST(a.fiscal_month_id AS FLOAT64) = e.month_idnt
  WHERE a.process_dt IN (SELECT load_date
     FROM month_to_max_dt)
  GROUP BY a.fiscal_month_id,
   a.dept_id,
   b.banner,
   b.channel_country,
   a.class_name,
   a.subclass_name,
   a.supp_id,
   a.otb_eow_date,
   a.vpn,
   a.supp_color,
   a.plan_type,
   a.po_key,
   a.plan_key,
   a.nrf_color_code,
   a.style_id,
   a.style_group_id,
   a.ttl_cost_us,
   a.ttl_cost_ca,
   a.cm_dollars,
   a.process_dt,
   month_plans,
   e.month_label,
   e.quarter,
   e.fiscal_year_num,
   e.quarter_idnt) AS t3
GROUP BY month_plans,
 dept_id,
 banner,
 country,
 fiscal_month_id,
 month_label,
 quarter,
 fiscal_year_num,
 quarter_idnt;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_loc_base_oo
CLUSTER BY store_num
AS
SELECT store_num,
 channel_num,
 channel_desc,
 channel_brand,
 channel_country,
   FORMAT('%11d', channel_num) || ',' || channel_desc AS channel,
  CASE
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Nordstrom'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('US')
  THEN 'US Rack'
  WHEN LOWER(channel_brand) = LOWER('Nordstrom_Rack') AND LOWER(channel_country) = LOWER('CA')
  THEN 'CA Rack'
  ELSE NULL
  END AS banner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw
WHERE LOWER(channel_brand) IN (LOWER('Nordstrom'), LOWER('Nordstrom_Rack'))
 AND channel_num IN (110, 120, 140, 210, 220, 240, 250, 260, 310, 930, 940, 990, 111, 121, 211, 221, 261, 311)
GROUP BY store_num,
 channel_brand,
 channel_country,
 channel_num,
 channel_desc;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_oo_base
CLUSTER BY rms_sku_num, store_num
AS
SELECT a.snapshot_plan_month_idnt AS month_plans,
 a.purchase_order_number,
 a.rms_sku_num,
 a.rms_casepack_num,
 b.channel_num,
 b.channel_desc,
 b.channel_country,
 b.banner,
 a.store_num,
 a.week_num,
 a.order_type,
 a.latest_approval_date,
 a.quantity_open,
 a.total_estimated_landing_cost
FROM t2dl_das_open_to_buy.merch_on_order_hist_plans AS a
 INNER JOIN hist_loc_base_oo AS b ON a.store_num = b.store_num
WHERE a.quantity_open > 0
 AND (LOWER(a.status) = LOWER('CLOSED') AND a.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY) OR LOWER(a.status
     ) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
 AND a.first_approval_date IS NOT NULL;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_oo_hierarchy
CLUSTER BY rms_sku_num
AS
SELECT b.rms_sku_num,
 b.channel_country,
 a.dept_num,
 a.dept_desc
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.product_sku_dim_vw AS a
 INNER JOIN (SELECT DISTINCT rms_sku_num,
   channel_country
  FROM hist_oo_base) AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(a.channel_country) = LOWER(b.channel_country
    )
GROUP BY b.rms_sku_num,
 b.channel_country,
 a.dept_num,
 a.dept_desc;


CREATE TEMPORARY TABLE IF NOT EXISTS hist_oo_final
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 fiscal_month_id,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(`A121811`) AS rcpts_oo_inactive_c,
 SUM(`A121812`) AS rcpts_oo_active_rp_c,
 SUM(`A121813`) AS rcpts_oo_active_nrp_c
FROM (SELECT a.month_plans,
   b.dept_num AS dept_id,
   a.banner,
   a.channel_country AS country,
   d.month_idnt AS fiscal_month_id,
   d.month_label,
   d.quarter_idnt,
   d.fiscal_year_num,
    CASE
    WHEN a.channel_num IN (930, 220, 221)
    THEN a.total_estimated_landing_cost
    ELSE 0
    END AS `A121811`,
    CASE
    WHEN LOWER(a.order_type) IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER')) AND a.channel_num NOT IN (930, 220
       , 221)
    THEN a.total_estimated_landing_cost
    ELSE 0
    END AS `A121812`,
    CASE
    WHEN a.channel_num NOT IN (930, 220, 221) AND LOWER(a.order_type) NOT IN (LOWER('AUTOMATIC_REORDER'), LOWER('BUYER_REORDER'
        ))
    THEN a.total_estimated_landing_cost
    ELSE 0
    END AS `A121813`
  FROM hist_oo_base AS a
   INNER JOIN hist_oo_hierarchy AS b ON LOWER(a.rms_sku_num) = LOWER(b.rms_sku_num) AND LOWER(a.channel_country) = LOWER(b
      .channel_country)
   INNER JOIN hist_date AS d ON a.week_num = d.week_idnt
  GROUP BY a.month_plans,
   a.purchase_order_number,
   a.rms_sku_num,
   a.rms_casepack_num,
   b.dept_num,
   b.dept_desc,
   a.channel_num,
   a.channel_desc,
   a.store_num,
   a.banner,
   a.channel_country,
   a.week_num,
   d.month_idnt,
   d.month_label,
   d.quarter_idnt,
   d.fiscal_year_num,
   a.order_type,
   a.latest_approval_date,
   a.quantity_open,
   a.total_estimated_landing_cost) AS t1
GROUP BY month_plans,
 dept_id,
 banner,
 country,
 fiscal_month_id,
 month_label,
 quarter_idnt,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS rp_rcpt_plan
CLUSTER BY month_plans, dept_num, banner, country
AS
SELECT month_plans,
 dept_num,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(replenishment_receipts_less_reserve_cost_amount) AS rp_rcpt_plan_c
FROM (SELECT a.snapshot_plan_month_idnt AS month_plans,
   a.dept_idnt AS dept_num,
    CASE
    WHEN LOWER(a.selling_brand) = LOWER('Nordstrom') AND LOWER(a.selling_country) = LOWER('US')
    THEN 'US Nordstrom'
    WHEN LOWER(a.selling_brand) = LOWER('Nordstrom') AND LOWER(a.selling_country) = LOWER('CA')
    THEN 'CA Nordstrom'
    WHEN LOWER(a.selling_brand) = LOWER('Nordstrom_Rack') AND LOWER(a.selling_country) = LOWER('US')
    THEN 'US Rack'
    WHEN LOWER(a.selling_brand) = LOWER('Nordstrom_Rack') AND LOWER(a.selling_country) = LOWER('CA')
    THEN 'CA Rack'
    ELSE NULL
    END AS banner,
   a.selling_country AS country,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   a.replenishment_receipts_less_reserve_cost_amount
  FROM t2dl_das_apt_cost_reporting.merch_assortment_supplier_cluster_plan_history AS a
   INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.fiscal_month_idnt = b.month_idnt
  WHERE LOWER(a.alternate_inventory_model) = LOWER('OWN')
  GROUP BY month_plans,
   country,
   b.month_idnt,
   b.month_label,
   b.quarter_idnt,
   b.fiscal_year_num,
   banner,
   dept_num,
   a.selling_brand,
   a.supplier_group,
   a.category,
   a.alternate_inventory_model,
   a.replenishment_receipts_less_reserve_cost_amount) AS e
GROUP BY month_plans,
 dept_num,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS planned_hist_report_mfp
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 SUM(sp_rcpts_reserve_c) AS sp_rcpts_reserve_c,
 SUM(sp_rcpts_active_c) AS sp_rcpts_active_c,
 SUM(sp_rcpts_active_c - sp_rcpts_reserve_c) AS sp_rcpts_less_reserve_c,
 SUM(op_rcpts_reserve_c) AS op_rcpts_reserve_c,
 SUM(op_rcpts_active_c) AS op_rcpts_active_c,
 SUM(op_rcpts_active_c - op_rcpts_reserve_c) AS op_rcpts_less_reserve_c,
 SUM(op_bop_active_c) AS op_bop_active_c,
 SUM(sp_bop_active_c) AS sp_bop_active_c,
 SUM(sp_net_sales_c) AS sp_net_sales_c,
 SUM(ly_net_sales_c) AS ly_net_sales_c,
 SUM(op_net_sales_c) AS op_net_sales_c,
 SUM(ty_net_sales_c) AS ty_net_sales_c,
  SUM(sp_bop_active_c) / NULLIF(SUM(sp_net_sales_c), 0) AS sp_wfc_active_c,
  SUM(op_bop_active_c) / NULLIF(SUM(op_net_sales_c), 0) AS op_wfc_active_c,
 SUM(rp_anticipated_spend_c) AS rp_anticipated_spend_c,
 SUM(rp_anticipated_spend_u) AS rp_anticipated_spend_u,
 SUM(inactive_cm_c) AS inactive_cm_c,
 SUM(active_cm_c) AS active_cm_c,
 SUM(nrp_cm_c) AS nrp_cm_c,
 SUM(rp_cm_c) AS rp_cm_c,
 SUM(inactive_cm_c + active_cm_c) AS cm_c,
 SUM(rcpts_oo_inactive_c) AS rcpts_oo_inactive_c,
 SUM(rcpts_oo_active_rp_c) AS rcpts_oo_active_rp_c,
 SUM(rcpts_oo_active_nrp_c) AS rcpts_oo_active_nrp_c,
 SUM(rp_rcpt_plan_c) AS rp_rcpt_plan_c,
 SUM(rcpts_oo_active_rp_c + rp_cm_c) AS rp_oo_and_cm_c,
 SUM(rcpts_oo_active_nrp_c + nrp_cm_c) AS nrp_oo_and_cm_c,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS rcd_update_timestamp
FROM (SELECT SUBSTR(CAST(month_plans AS STRING), 1, 10) AS month_plans,
    CAST(trunc(dept_id) AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(CAST(quarter_idnt AS STRING), 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    CAST(sp_rcpts_reserve_c AS NUMERIC) AS sp_rcpts_reserve_c,
    CAST(sp_rcpts_active_c AS NUMERIC) AS sp_rcpts_active_c,
    CAST(sp_rcpts_inactive_c AS NUMERIC) AS sp_rcpts_inactive_c,
    CAST(op_rcpts_active_c AS NUMERIC) AS op_rcpts_active_c,
    CAST(op_rcpts_inactive_c AS NUMERIC) AS op_rcpts_inactive_c,
    CAST(op_rcpts_reserve_c AS NUMERIC) AS op_rcpts_reserve_c,
    CAST(op_bop_active_c AS NUMERIC) AS op_bop_active_c,
    CAST(sp_bop_active_c AS NUMERIC) AS sp_bop_active_c,
    0 AS sp_net_sales_c,
    0 AS ly_net_sales_c,
    0 AS op_net_sales_c,
    0 AS ty_net_sales_c,
    0 AS rp_anticipated_spend_c,
    0 AS rp_anticipated_spend_u,
    0 AS inactive_cm_c,
    0 AS active_cm_c,
    0 AS rp_cm_c,
    0 AS nrp_cm_c,
    0 AS rcpts_oo_inactive_c,
    0 AS rcpts_oo_active_rp_c,
    0 AS rcpts_oo_active_nrp_c,
    0 AS rp_rcpt_plan_c
   FROM hist_cur_comb
   UNION ALL
   SELECT SUBSTR(CAST(month_plans AS STRING), 1, 10) AS month_plans,
    CAST(trunc(dept_id) AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(CAST(quarter_idnt AS STRING), 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS sp_rcpts_reserve_c,
    0 AS sp_rcpts_active_c,
    0 AS sp_rcpts_inactive_c,
    0 AS op_rcpts_active_c,
    0 AS op_rcpts_inactive_c,
    0 AS op_rcpts_reserve_c,
    0 AS op_bop_active_c,
    0 AS sp_bop_active_c,
    CAST(sp_net_sales_c AS NUMERIC) AS sp_net_sales_c,
    CAST(ly_net_sales_c AS NUMERIC) AS ly_net_sales_c,
    CAST(op_net_sales_c AS NUMERIC) AS op_net_sales_c,
    CAST(ty_net_sales_c AS NUMERIC) AS ty_net_sales_c,
    0 AS rp_anticipated_spend_c,
    0 AS rp_anticipated_spend_u,
    0 AS inactive_cm_c,
    0 AS active_cm_c,
    0 AS rp_cm_c,
    0 AS nrp_cm_c,
    0 AS rcpts_oo_inactive_c,
    0 AS rcpts_oo_active_rp_c,
    0 AS rcpts_oo_active_nrp_c,
    0 AS rp_rcpt_plan_c
   FROM com_cur_hist_sale
   UNION ALL
   SELECT SUBSTR(CAST(month_plans AS STRING), 1, 10) AS month_plans,
    CAST(trunc(dept_id) AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(CAST(quarter_idnt AS STRING), 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS sp_rcpts_reserve_c,
    0 AS sp_rcpts_active_c,
    0 AS sp_rcpts_inactive_c,
    0 AS op_rcpts_active_c,
    0 AS op_rcpts_inactive_c,
    0 AS op_rcpts_reserve_c,
    0 AS op_bop_active_c,
    0 AS sp_bop_active_c,
    0 AS sp_net_sales_c,
    0 AS ly_net_sales_c,
    0 AS op_net_sales_c,
    0 AS ty_net_sales_c,
    CAST(rp_anticipated_spend_c AS NUMERIC) AS rp_anticipated_spend_c,
    CAST(rp_anticipated_spend_u AS NUMERIC) AS rp_anticipated_spend_u,
    0 AS inactive_cm_c,
    0 AS active_cm_c,
    0 AS rp_cm_c,
    0 AS nrp_cm_c,
    0 AS rcpts_oo_inactive_c,
    0 AS rcpts_oo_active_rp_c,
    0 AS rcpts_oo_active_nrp_c,
    0 AS rp_rcpt_plan_c
   FROM hist_rp_ant_spnd
   UNION ALL
   SELECT SUBSTR(CAST(month_plans AS STRING), 1, 10) AS month_plans,
    CAST(trunc(dept_id) AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(CAST(quarter_idnt AS STRING), 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(fiscal_month_id, 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS sp_rcpts_reserve_c,
    0 AS sp_rcpts_active_c,
    0 AS sp_rcpts_inactive_c,
    0 AS op_rcpts_active_c,
    0 AS op_rcpts_inactive_c,
    0 AS op_rcpts_reserve_c,
    0 AS op_bop_active_c,
    0 AS sp_bop_active_c,
    0 AS sp_net_sales_c,
    0 AS ly_net_sales_c,
    0 AS op_net_sales_c,
    0 AS ty_net_sales_c,
    0 AS rp_anticipated_spend_c,
    0 AS rp_anticipated_spend_u,
    CAST(inactive_cm_c AS NUMERIC) AS inactive_cm_c,
    CAST(active_cm_c AS NUMERIC) AS active_cm_c,
    CAST(rp_cm_c AS NUMERIC) AS rp_cm_c,
    CAST(nrp_cm_c AS NUMERIC) AS nrp_cm_c,
    0 AS rcpts_oo_inactive_c,
    0 AS rcpts_oo_active_rp_c,
    0 AS rcpts_oo_active_nrp_c,
    0 AS rp_rcpt_plan_c
   FROM hist_cm_f_1
   UNION ALL
   SELECT SUBSTR(CAST(month_plans AS STRING), 1, 10) AS month_plans,
    CAST(trunc(dept_id) AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(CAST(quarter_idnt AS STRING), 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(fiscal_month_id AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS sp_rcpts_reserve_c,
    0 AS sp_rcpts_active_c,
    0 AS sp_rcpts_inactive_c,
    0 AS op_rcpts_active_c,
    0 AS op_rcpts_inactive_c,
    0 AS op_rcpts_reserve_c,
    0 AS op_bop_active_c,
    0 AS sp_bop_active_c,
    0 AS sp_net_sales_c,
    0 AS ly_net_sales_c,
    0 AS op_net_sales_c,
    0 AS ty_net_sales_c,
    0 AS rp_anticipated_spend_c,
    0 AS rp_anticipated_spend_u,
    0 AS inactive_cm_c,
    0 AS active_cm_c,
    0 AS rp_cm_c,
    0 AS nrp_cm_c,
    CAST(rcpts_oo_inactive_c AS NUMERIC) AS rcpts_oo_inactive_c,
    CAST(rcpts_oo_active_rp_c AS NUMERIC) AS rcpts_oo_active_rp_c,
    CAST(rcpts_oo_active_nrp_c AS NUMERIC) AS rcpts_oo_active_nrp_c,
    0 AS rp_rcpt_plan_c
   FROM hist_oo_final
   UNION ALL
   SELECT SUBSTR(CAST(month_plans AS STRING), 1, 10) AS month_plans,
    CAST(trunc(dept_num) AS INTEGER) AS dept_id,
    SUBSTR(banner, 1, 20) AS banner,
    SUBSTR(country, 1, 5) AS country,
    SUBSTR(CAST(quarter_idnt AS STRING), 1, 10) AS quarter_idnt,
    SUBSTR(CAST(fiscal_year_num AS STRING), 1, 10) AS fiscal_year_num,
    SUBSTR(CAST(month_idnt AS STRING), 1, 10) AS month_idnt,
    SUBSTR(month_label, 1, 10) AS month_label,
    0 AS sp_rcpts_reserve_c,
    0 AS sp_rcpts_active_c,
    0 AS sp_rcpts_inactive_c,
    0 AS op_rcpts_active_c,
    0 AS op_rcpts_inactive_c,
    0 AS op_rcpts_reserve_c,
    0 AS op_bop_active_c,
    0 AS sp_bop_active_c,
    0 AS sp_net_sales_c,
    0 AS ly_net_sales_c,
    0 AS op_net_sales_c,
    0 AS ty_net_sales_c,
    0 AS rp_anticipated_spend_c,
    0 AS rp_anticipated_spend_u,
    0 AS inactive_cm_c,
    0 AS active_cm_c,
    0 AS rp_cm_c,
    0 AS nrp_cm_c,
    0 AS rcpts_oo_inactive_c,
    0 AS rcpts_oo_active_rp_c,
    0 AS rcpts_oo_active_nrp_c,
    ROUND(CAST(rp_rcpt_plan_c AS NUMERIC), 4) AS rp_rcpt_plan_c
   FROM rp_rcpt_plan) AS a
GROUP BY month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num;


CREATE TEMPORARY TABLE IF NOT EXISTS planned_hist_report_mfp_1
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 sp_rcpts_reserve_c,
 sp_rcpts_active_c,
 sp_rcpts_less_reserve_c,
 op_rcpts_reserve_c,
 op_rcpts_active_c,
 op_rcpts_less_reserve_c,
 op_bop_active_c,
 sp_bop_active_c,
 sp_net_sales_c,
 ly_net_sales_c,
 op_net_sales_c,
 ty_net_sales_c,
 sp_wfc_active_c,
 op_wfc_active_c,
 rp_anticipated_spend_c,
 rp_anticipated_spend_u,
 inactive_cm_c,
 active_cm_c,
 nrp_cm_c,
 rp_cm_c,
 cm_c,
 rcpts_oo_inactive_c,
 rcpts_oo_active_rp_c,
 rcpts_oo_active_nrp_c,
 rp_rcpt_plan_c,
 rp_oo_and_cm_c,
 nrp_oo_and_cm_c,
 rcd_update_timestamp,
  CASE
  WHEN rp_oo_and_cm_c > rp_anticipated_spend_c
  THEN rp_oo_and_cm_c + nrp_oo_and_cm_c
  WHEN rp_oo_and_cm_c < rp_anticipated_spend_c
  THEN rp_anticipated_spend_c + nrp_oo_and_cm_c
  ELSE 0
  END AS ttl_non_rp_oo_cm_rp_spent_c
FROM planned_hist_report_mfp AS a;


CREATE TEMPORARY TABLE IF NOT EXISTS mfp_plan_hist_report_dept_h48v
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT TRIM(FORMAT('%11d', b.division_num) || ',' || b.division_name) AS division,
 TRIM(FORMAT('%11d', b.subdivision_num) || ',' || b.subdivision_name) AS subdivision,
 TRIM(FORMAT('%11d', a.dept_id) || ',' || b.dept_name) AS department,
 b.active_store_ind AS `active vs inactive`,
 c.month_label AS snap_month_label,
 a.month_plans,
 a.dept_id,
 a.banner,
 a.country,
 a.month_idnt,
 a.month_label,
 a.quarter_idnt,
 a.fiscal_year_num,
 a.sp_rcpts_reserve_c,
 a.sp_rcpts_active_c,
 a.sp_rcpts_less_reserve_c,
 a.op_rcpts_reserve_c,
 a.op_rcpts_active_c,
 a.op_rcpts_less_reserve_c,
 a.op_bop_active_c,
 a.sp_bop_active_c,
 a.sp_net_sales_c,
 a.ly_net_sales_c,
 a.op_net_sales_c,
 a.ty_net_sales_c,
 a.sp_wfc_active_c,
 a.op_wfc_active_c,
 a.rp_anticipated_spend_c,
 a.rp_anticipated_spend_u,
 a.inactive_cm_c,
 a.active_cm_c,
 a.nrp_cm_c,
 a.rp_cm_c,
 a.cm_c,
 a.rcpts_oo_inactive_c,
 a.rcpts_oo_active_rp_c,
 a.rcpts_oo_active_nrp_c,
 a.rp_rcpt_plan_c,
 a.rp_oo_and_cm_c,
 a.nrp_oo_and_cm_c,
 a.rcd_update_timestamp
FROM planned_hist_report_mfp AS a
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS b ON a.dept_id = b.dept_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c ON c.month_idnt = CAST(a.month_plans AS FLOAT64)
WHERE LOWER(b.active_store_ind) = LOWER('A')
 AND LOWER(a.month_idnt) >= LOWER(a.month_plans)
GROUP BY division,
 subdivision,
 department,
 `active vs inactive`,
 snap_month_label,
 a.month_plans,
 a.dept_id,
 a.banner,
 a.country,
 a.month_idnt,
 a.month_label,
 a.quarter_idnt,
 a.fiscal_year_num,
 a.sp_rcpts_reserve_c,
 a.sp_rcpts_active_c,
 a.sp_rcpts_less_reserve_c,
 a.op_rcpts_reserve_c,
 a.op_rcpts_active_c,
 a.op_rcpts_less_reserve_c,
 a.op_bop_active_c,
 a.sp_bop_active_c,
 a.sp_net_sales_c,
 a.ly_net_sales_c,
 a.op_net_sales_c,
 a.ty_net_sales_c,
 a.sp_wfc_active_c,
 a.op_wfc_active_c,
 a.rp_anticipated_spend_c,
 a.rp_anticipated_spend_u,
 a.inactive_cm_c,
 a.active_cm_c,
 a.nrp_cm_c,
 a.rp_cm_c,
 a.cm_c,
 a.rcpts_oo_inactive_c,
 a.rcpts_oo_active_rp_c,
 a.rcpts_oo_active_nrp_c,
 a.rp_rcpt_plan_c,
 a.rp_oo_and_cm_c,
 a.nrp_oo_and_cm_c,
 a.rcd_update_timestamp;


CREATE TEMPORARY TABLE IF NOT EXISTS mfp_plan_hist_report_dept_h48v_m
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT *
FROM mfp_plan_hist_report_dept_h48v AS a
WHERE LOWER(month_idnt) > LOWER(month_plans)
UNION ALL
SELECT *
FROM mfp_plan_hist_report_dept_h48v AS a
WHERE LOWER(month_idnt) = LOWER(month_plans)
 AND CAST(trunc(cast(month_plans as float64)) AS INTEGER) IN (SELECT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE CURRENT_DATE('PST8PDT') = day_date);


CREATE TEMPORARY TABLE IF NOT EXISTS mfp_plan_hist_report_dept_h48v_1
CLUSTER BY month_plans, dept_id, banner, country
AS
SELECT SUBSTR(division, 1, 20) AS division,
 SUBSTR(subdivision, 1, 20) AS subdivision,
 SUBSTR(department, 1, 20) AS department,
 SUBSTR(`active vs inactive`, 1, 10) AS `active vs inactive`,
 SUBSTR(snap_month_label, 1, 10) AS snap_month_label,
 month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 sp_rcpts_reserve_c,
 sp_rcpts_active_c,
 sp_rcpts_less_reserve_c,
 op_rcpts_reserve_c,
 op_rcpts_active_c,
 op_rcpts_less_reserve_c,
 op_bop_active_c,
 sp_bop_active_c,
 sp_net_sales_c,
 ly_net_sales_c,
 op_net_sales_c,
 ty_net_sales_c,
 sp_wfc_active_c,
 op_wfc_active_c,
 rp_anticipated_spend_c,
 rp_anticipated_spend_u,
 inactive_cm_c,
 active_cm_c,
 nrp_cm_c,
 rp_cm_c,
 cm_c,
 rcpts_oo_inactive_c,
 rcpts_oo_active_rp_c,
 rcpts_oo_active_nrp_c,
 rp_rcpt_plan_c,
 rp_oo_and_cm_c,
 nrp_oo_and_cm_c,
 rcd_update_timestamp
FROM mfp_plan_hist_report_dept_h48v_m
WHERE sp_rcpts_reserve_c <> 0
 OR sp_rcpts_active_c <> 0
 OR sp_rcpts_less_reserve_c <> 0
 OR op_rcpts_reserve_c <> 0
 OR op_rcpts_active_c <> 0
 OR op_rcpts_less_reserve_c <> 0
 OR op_bop_active_c <> 0
 OR sp_bop_active_c <> 0
 OR sp_net_sales_c <> 0
 OR ly_net_sales_c <> 0
 OR op_net_sales_c <> 0
 OR ty_net_sales_c <> 0
 OR sp_wfc_active_c <> 0
 OR op_wfc_active_c <> 0
 OR rp_anticipated_spend_c <> 0
 OR rp_anticipated_spend_u <> 0
 OR inactive_cm_c <> 0
 OR active_cm_c <> 0
 OR rp_cm_c <> 0
 OR nrp_cm_c <> 0
 OR cm_c <> 0
 OR rcpts_oo_inactive_c <> 0
 OR rcpts_oo_active_rp_c <> 0
 OR rcpts_oo_active_nrp_c <> 0
 OR rp_rcpt_plan_c <> 0
 OR rp_oo_and_cm_c <> 0
 OR nrp_oo_and_cm_c <> 0;



Truncate table `{{params.gcp_project_id}}`.{{params.environment_schema}}.planned_hist_report_mfp{{params.env_suffix}};


INSERT INTO  `{{params.gcp_project_id}}`.{{params.environment_schema}}.planned_hist_report_mfp{{params.env_suffix}}

SELECT 
SUBSTR(division, 1, 20) AS division,
 SUBSTR(subdivision, 1, 20) AS subdivision,
 SUBSTR(department, 1, 20) AS department,
 SUBSTR(`active vs inactive`, 1, 10) AS `active vs inactive`,
 SUBSTR(snap_month_label, 1, 10) AS snap_month_label,
 month_plans,
 dept_id,
 banner,
 country,
 month_idnt,
 month_label,
 quarter_idnt,
 fiscal_year_num,
 sp_rcpts_reserve_c,
 sp_rcpts_active_c,
 sp_rcpts_less_reserve_c,
 op_rcpts_reserve_c,
 op_rcpts_active_c,
 op_rcpts_less_reserve_c,
 op_bop_active_c,
 sp_bop_active_c,
 sp_net_sales_c,
 ly_net_sales_c,
 op_net_sales_c,
 ty_net_sales_c,
 sp_wfc_active_c,
 op_wfc_active_c,
 rp_anticipated_spend_c,
 rp_anticipated_spend_u,
 inactive_cm_c,
 active_cm_c,
 nrp_cm_c,
 rp_cm_c,
 cm_c,
 rcpts_oo_inactive_c,
 rcpts_oo_active_rp_c,
 rcpts_oo_active_nrp_c,
 rp_rcpt_plan_c,
 rp_oo_and_cm_c,
 nrp_oo_and_cm_c,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) AS rcd_update_timestamp,
 
`{{params.gcp_project_id}}`.jwn_udf.default_tz_pst() AS rcd_update_timestamp_tz
FROM mfp_plan_hist_report_dept_h48v_1;

COMMIT TRANSACTION;
