/* SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=mfp_cost_country_banner_blend_week_fact_load;
Task_Name=country_banner_blend_data_load_country_banner_week_wrk_load;'
FOR SESSION VOLATILE;*/


-- Delete all the data



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.mfp_cost_plan_actual_banner_country_wrk;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.mfp_cost_plan_actual_banner_country_wrk (dept_num, week_num, banner_country_num,
 fulfill_type_num, cp_beginning_of_period_active_cost_amt, cp_beginning_of_period_active_inventory_target_qty,
 cp_beginning_of_period_active_qty, cp_beginning_of_period_inactive_cost_amt, cp_beginning_of_period_inactive_qty,
 cp_ending_of_period_active_cost_amt, cp_ending_of_period_active_qty, cp_ending_of_period_inactive_cost_amt,
 cp_ending_of_period_inactive_qty, cp_transfer_in_active_cost_amt, cp_transfer_in_active_qty,
 cp_transfer_out_active_cost_amt, cp_transfer_out_active_qty, cp_transfer_in_inactive_cost_amt,
 cp_transfer_in_inactive_qty, cp_transfer_out_inactive_cost_amt, cp_transfer_out_inactive_qty,
 cp_transfer_in_other_cost_amt, cp_transfer_in_other_qty, cp_transfer_out_other_cost_amt, cp_transfer_out_other_qty,
 cp_transfer_in_racking_cost_amt, cp_transfer_in_racking_qty, cp_transfer_out_racking_cost_amt,
 cp_transfer_out_racking_qty, cp_mark_out_of_stock_active_cost_amt, cp_mark_out_of_stock_active_qty,
 cp_mark_out_of_stock_inactive_cost_amt, cp_mark_out_of_stock_inactive_qty, cp_receipts_active_cost_amt,
 cp_receipts_active_qty, cp_receipts_inactive_cost_amt, cp_receipts_inactive_qty, cp_receipts_reserve_cost_amt,
 cp_receipts_reserve_qty, cp_reclass_in_cost_amt, cp_reclass_in_qty, cp_reclass_out_cost_amt, cp_reclass_out_qty,
 cp_return_to_vendor_active_cost_amt, cp_return_to_vendor_active_qty, cp_return_to_vendor_inactive_cost_amt,
 cp_return_to_vendor_inactive_qty, cp_vendor_funds_cost_amt, cp_discount_terms_cost_amt, cp_merch_margin_retail_amt,
 cp_inventory_movement_in_dropship_cost_amt, cp_inventory_movement_in_dropship_qty,
 cp_inventory_movement_out_dropship_cost_amt, cp_inventory_movement_out_dropship_qty,
 cp_other_inventory_adjustment_cost_amt, cp_other_inventory_adjustment_qty, cp_open_to_buy_receipts_active_cost_amt,
 cp_open_to_buy_receipts_inactive_cost_amt, op_beginning_of_period_active_cost_amt,
 op_beginning_of_period_active_inventory_target_qty, op_beginning_of_period_active_qty,
 op_beginning_of_period_inactive_cost_amt, op_beginning_of_period_inactive_qty, op_ending_of_period_active_cost_amt,
 op_ending_of_period_active_qty, op_ending_of_period_inactive_cost_amt, op_ending_of_period_inactive_qty,
 op_transfer_in_active_cost_amt, op_transfer_in_active_qty, op_transfer_out_active_cost_amt, op_transfer_out_active_qty
 , op_transfer_in_inactive_cost_amt, op_transfer_in_inactive_qty, op_transfer_out_inactive_cost_amt,
 op_transfer_out_inactive_qty, op_transfer_in_other_cost_amt, op_transfer_in_other_qty, op_transfer_out_other_cost_amt,
 op_transfer_out_other_qty, op_transfer_in_racking_cost_amt, op_transfer_in_racking_qty,
 op_transfer_out_racking_cost_amt, op_transfer_out_racking_qty, op_mark_out_of_stock_active_cost_amt,
 op_mark_out_of_stock_active_qty, op_mark_out_of_stock_inactive_cost_amt, op_mark_out_of_stock_inactive_qty,
 op_receipts_active_cost_amt, op_receipts_active_qty, op_receipts_inactive_cost_amt, op_receipts_inactive_qty,
 op_receipts_reserve_cost_amt, op_receipts_reserve_qty, op_reclass_in_cost_amt, op_reclass_in_qty,
 op_reclass_out_cost_amt, op_reclass_out_qty, op_return_to_vendor_active_cost_amt, op_return_to_vendor_active_qty,
 op_return_to_vendor_inactive_cost_amt, op_return_to_vendor_inactive_qty, op_vendor_funds_cost_amt,
 op_discount_terms_cost_amt, op_merch_margin_retail_amt, op_inventory_movement_in_dropship_cost_amt,
 op_inventory_movement_in_dropship_qty, op_inventory_movement_out_dropship_cost_amt,
 op_inventory_movement_out_dropship_qty, op_other_inventory_adjustment_cost_amt, op_other_inventory_adjustment_qty,
 sp_beginning_of_period_active_cost_amt, sp_beginning_of_period_active_inventory_target_qty,
 sp_beginning_of_period_active_qty, sp_beginning_of_period_inactive_cost_amt, sp_beginning_of_period_inactive_qty,
 sp_ending_of_period_active_cost_amt, sp_ending_of_period_active_qty, sp_ending_of_period_inactive_cost_amt,
 sp_ending_of_period_inactive_qty, sp_transfer_in_active_cost_amt, sp_transfer_in_active_qty,
 sp_transfer_out_active_cost_amt, sp_transfer_out_active_qty, sp_transfer_in_inactive_cost_amt,
 sp_transfer_in_inactive_qty, sp_transfer_out_inactive_cost_amt, sp_transfer_out_inactive_qty,
 sp_transfer_in_other_cost_amt, sp_transfer_in_other_qty, sp_transfer_out_other_cost_amt, sp_transfer_out_other_qty,
 sp_transfer_in_racking_cost_amt, sp_transfer_in_racking_qty, sp_transfer_out_racking_cost_amt,
 sp_transfer_out_racking_qty, sp_mark_out_of_stock_active_cost_amt, sp_mark_out_of_stock_active_qty,
 sp_mark_out_of_stock_inactive_cost_amt, sp_mark_out_of_stock_inactive_qty, sp_receipts_active_cost_amt,
 sp_receipts_active_qty, sp_receipts_inactive_cost_amt, sp_receipts_inactive_qty, sp_receipts_reserve_cost_amt,
 sp_receipts_reserve_qty, sp_reclass_in_cost_amt, sp_reclass_in_qty, sp_reclass_out_cost_amt, sp_reclass_out_qty,
 sp_return_to_vendor_active_cost_amt, sp_return_to_vendor_active_qty, sp_return_to_vendor_inactive_cost_amt,
 sp_return_to_vendor_inactive_qty, sp_vendor_funds_cost_amt, sp_discount_terms_cost_amt, sp_merch_margin_retail_amt,
 sp_inventory_movement_in_dropship_cost_amt, sp_inventory_movement_in_dropship_qty,
 sp_inventory_movement_out_dropship_cost_amt, sp_inventory_movement_out_dropship_qty,
 sp_other_inventory_adjustment_cost_amt, sp_other_inventory_adjustment_qty, sp_open_to_buy_receipts_active_cost_amt,
 sp_open_to_buy_receipts_inactive_cost_amt, ty_beginning_of_period_active_cost_amt, ty_beginning_of_period_active_qty,
 ty_beginning_of_period_inactive_cost_amt, ty_beginning_of_period_inactive_qty, ty_ending_of_period_active_cost_amt,
 ty_ending_of_period_active_qty, ty_ending_of_period_inactive_cost_amt, ty_ending_of_period_inactive_qty,
 ty_transfer_in_active_cost_amt, ty_transfer_in_active_qty, ty_transfer_out_active_cost_amt, ty_transfer_out_active_qty
 , ty_transfer_in_inactive_cost_amt, ty_transfer_in_inactive_qty, ty_transfer_out_inactive_cost_amt,
 ty_transfer_out_inactive_qty, ty_transfer_in_other_cost_amt, ty_transfer_in_other_qty, ty_transfer_out_other_cost_amt,
 ty_transfer_out_other_qty, ty_transfer_in_racking_cost_amt, ty_transfer_in_racking_qty,
 ty_transfer_out_racking_cost_amt, ty_transfer_out_racking_qty, ty_mark_out_of_stock_active_cost_amt,
 ty_mark_out_of_stock_active_qty, ty_mark_out_of_stock_inactive_cost_amt, ty_mark_out_of_stock_inactive_qty,
 ty_receipts_active_cost_amt, ty_receipts_active_qty, ty_receipts_inactive_cost_amt, ty_receipts_inactive_qty,
 ty_rp_receipts_active_cost_amt, ty_non_rp_receipts_active_cost_amt, ty_reclass_in_cost_amt, ty_reclass_in_qty,
 ty_reclass_out_cost_amt, ty_reclass_out_qty, ty_return_to_vendor_active_cost_amt, ty_return_to_vendor_active_qty,
 ty_return_to_vendor_inactive_cost_amt, ty_return_to_vendor_inactive_qty, ty_vendor_funds_cost_amt,
 ty_discount_terms_cost_amt, ty_merch_margin_retail_amt, ty_shrink_banner_cost_amt, ty_shrink_banner_qty,
 ty_other_inventory_adj_cost, ty_merch_margin_charges_cost, ly_beginning_of_period_active_cost_amt,
 ly_beginning_of_period_active_qty, ly_beginning_of_period_inactive_cost_amt, ly_beginning_of_period_inactive_qty,
 ly_ending_of_period_active_cost_amt, ly_ending_of_period_active_qty, ly_ending_of_period_inactive_cost_amt,
 ly_ending_of_period_inactive_qty, ly_transfer_in_active_cost_amt, ly_transfer_in_active_qty,
 ly_transfer_out_active_cost_amt, ly_transfer_out_active_qty, ly_transfer_in_inactive_cost_amt,
 ly_transfer_in_inactive_qty, ly_transfer_out_inactive_cost_amt, ly_transfer_out_inactive_qty,
 ly_transfer_in_other_cost_amt, ly_transfer_in_other_qty, ly_transfer_out_other_cost_amt, ly_transfer_out_other_qty,
 ly_transfer_in_racking_cost_amt, ly_transfer_in_racking_qty, ly_transfer_out_racking_cost_amt,
 ly_transfer_out_racking_qty, ly_mark_out_of_stock_active_cost_amt, ly_mark_out_of_stock_active_qty,
 ly_mark_out_of_stock_inactive_cost_amt, ly_mark_out_of_stock_inactive_qty, ly_receipts_active_cost_amt,
 ly_receipts_active_qty, ly_receipts_inactive_cost_amt, ly_receipts_inactive_qty, ly_rp_receipts_active_cost_amt,
 ly_non_rp_receipts_active_cost_amt, ly_reclass_in_cost_amt, ly_reclass_in_qty, ly_reclass_out_cost_amt,
 ly_reclass_out_qty, ly_return_to_vendor_active_cost_amt, ly_return_to_vendor_active_qty,
 ly_return_to_vendor_inactive_cost_amt, ly_return_to_vendor_inactive_qty, ly_vendor_funds_cost_amt,
 ly_discount_terms_cost_amt, ly_merch_margin_retail_amt, ly_shrink_banner_cost_amt, ly_shrink_banner_qty,
 ly_other_inventory_adj_cost, ly_merch_margin_charges_cost, lly_beginning_of_period_active_cost_amt,
 lly_beginning_of_period_active_qty, lly_beginning_of_period_inactive_cost_amt, lly_beginning_of_period_inactive_qty,
 lly_ending_of_period_active_cost_amt, lly_ending_of_period_active_qty, lly_ending_of_period_inactive_cost_amt,
 lly_ending_of_period_inactive_qty, lly_transfer_in_active_cost_amt, lly_transfer_in_active_qty,
 lly_transfer_out_active_cost_amt, lly_transfer_out_active_qty, lly_transfer_in_inactive_cost_amt,
 lly_transfer_in_inactive_qty, lly_transfer_out_inactive_cost_amt, lly_transfer_out_inactive_qty,
 lly_transfer_in_other_cost_amt, lly_transfer_in_other_qty, lly_transfer_out_other_cost_amt, lly_transfer_out_other_qty
 , lly_transfer_in_racking_cost_amt, lly_transfer_in_racking_qty, lly_transfer_out_racking_cost_amt,
 lly_transfer_out_racking_qty, lly_mark_out_of_stock_active_cost_amt, lly_mark_out_of_stock_active_qty,
 lly_mark_out_of_stock_inactive_cost_amt, lly_mark_out_of_stock_inactive_qty, lly_receipts_active_cost_amt,
 lly_receipts_active_qty, lly_receipts_inactive_cost_amt, lly_receipts_inactive_qty, lly_rp_receipts_active_cost_amt,
 lly_non_rp_receipts_active_cost_amt, lly_reclass_in_cost_amt, lly_reclass_in_qty, lly_reclass_out_cost_amt,
 lly_reclass_out_qty, lly_return_to_vendor_active_cost_amt, lly_return_to_vendor_active_qty,
 lly_return_to_vendor_inactive_cost_amt, lly_return_to_vendor_inactive_qty, lly_vendor_funds_cost_amt,
 lly_discount_terms_cost_amt, lly_merch_margin_retail_amt, lly_shrink_banner_cost_amt, lly_shrink_banner_qty,
 lly_other_inventory_adj_cost, lly_merch_margin_charges_cost, dw_batch_date, dw_sys_load_tmstp)
(SELECT a.dept_num,
  a.week_num,
  a.banner_country_num,
  a.fulfill_type_num,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_beginning_of_period_active_cost_amt
     ELSE a.cp_beginning_of_period_active_cost_amt
     END) AS NUMERIC) AS cp_beginning_of_period_active_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_beginning_of_period_active_inventory_target_qty
     END) AS NUMERIC) AS cp_beginning_of_period_active_inventory_target_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_beginning_of_period_active_qty AS BIGNUMERIC)
     ELSE a.cp_beginning_of_period_active_qty
     END) AS NUMERIC) AS cp_beginning_of_period_active_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_beginning_of_period_inactive_cost_amt
     ELSE a.cp_beginning_of_period_inactive_cost_amt
     END) AS NUMERIC) AS cp_beginning_of_period_inactive_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_beginning_of_period_inactive_qty AS BIGNUMERIC)
     ELSE a.cp_beginning_of_period_inactive_qty
     END) AS NUMERIC) AS cp_beginning_of_period_inactive_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_ending_of_period_active_cost_amt
     ELSE a.cp_ending_of_period_active_cost_amt
     END) AS NUMERIC) AS cp_ending_of_period_active_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_ending_of_period_active_qty AS BIGNUMERIC)
     ELSE a.cp_ending_of_period_active_qty
     END) AS NUMERIC) AS cp_ending_of_period_active_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_ending_of_period_inactive_cost_amt
     ELSE a.cp_ending_of_period_inactive_cost_amt
     END) AS NUMERIC) AS cp_ending_of_period_inactive_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_ending_of_period_inactive_qty AS BIGNUMERIC)
     ELSE a.cp_ending_of_period_inactive_qty
     END) AS NUMERIC) AS cp_ending_of_period_inactive_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_in_active_cost_amt
     ELSE a.cp_transfer_in_active_cost_amt
     END) AS NUMERIC) AS cp_transfer_in_active_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_in_active_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_in_active_qty
     END) AS NUMERIC) AS cp_transfer_in_active_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_out_active_cost_amt
     ELSE a.cp_transfer_out_active_cost_amt
     END) AS NUMERIC) AS cp_transfer_out_active_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_out_active_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_out_active_qty
     END) AS NUMERIC) AS cp_transfer_out_active_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_in_inactive_cost_amt
     ELSE a.cp_transfer_in_inactive_cost_amt
     END) AS NUMERIC) AS cp_transfer_in_inactive_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_in_inactive_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_in_inactive_qty
     END) AS NUMERIC) AS cp_transfer_in_inactive_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_out_inactive_cost_amt
     ELSE a.cp_transfer_out_inactive_cost_amt
     END) AS NUMERIC) AS cp_transfer_out_inactive_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_out_inactive_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_out_inactive_qty
     END) AS NUMERIC) AS cp_transfer_out_inactive_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_in_other_cost_amt
     ELSE a.cp_transfer_in_other_cost_amt
     END) AS NUMERIC) AS cp_transfer_in_other_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_in_other_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_in_other_qty
     END) AS NUMERIC) AS cp_transfer_in_other_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_out_other_cost_amt
     ELSE a.cp_transfer_out_other_cost_amt
     END) AS NUMERIC) AS cp_transfer_out_other_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_out_other_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_out_other_qty
     END) AS NUMERIC) AS cp_transfer_out_other_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_in_racking_cost_amt
     ELSE a.cp_transfer_in_racking_cost_amt
     END) AS NUMERIC) AS cp_transfer_in_racking_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_in_racking_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_in_racking_qty
     END) AS NUMERIC) AS cp_transfer_in_racking_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_transfer_out_racking_cost_amt
     ELSE a.cp_transfer_out_racking_cost_amt
     END) AS NUMERIC) AS cp_transfer_out_racking_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_transfer_out_racking_qty AS BIGNUMERIC)
     ELSE a.cp_transfer_out_racking_qty
     END) AS NUMERIC) AS cp_transfer_out_racking_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_mark_out_of_stock_active_cost_amt
     ELSE a.cp_mark_out_of_stock_active_cost_amt
     END) AS NUMERIC) AS cp_mark_out_of_stock_active_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_mark_out_of_stock_active_qty AS BIGNUMERIC)
     ELSE a.cp_mark_out_of_stock_active_qty
     END) AS NUMERIC) AS cp_mark_out_of_stock_active_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_mark_out_of_stock_inactive_cost_amt
     ELSE a.cp_mark_out_of_stock_inactive_cost_amt
     END) AS NUMERIC) AS cp_mark_out_of_stock_inactive_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_mark_out_of_stock_inactive_qty AS BIGNUMERIC)
     ELSE a.cp_mark_out_of_stock_inactive_qty
     END) AS NUMERIC) AS cp_mark_out_of_stock_inactive_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_receipts_active_cost_amt
     ELSE a.cp_receipts_active_cost_amt
     END) AS NUMERIC) AS cp_receipts_active_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_receipts_active_qty AS BIGNUMERIC)
     ELSE a.cp_receipts_active_qty
     END) AS NUMERIC) AS cp_receipts_active_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_receipts_inactive_cost_amt
     ELSE a.cp_receipts_inactive_cost_amt
     END) AS NUMERIC) AS cp_receipts_inactive_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_receipts_inactive_qty AS BIGNUMERIC)
     ELSE a.cp_receipts_inactive_qty
     END) AS NUMERIC) AS cp_receipts_inactive_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_receipts_reserve_cost_amt
     END) AS NUMERIC) AS cp_receipts_reserve_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_receipts_reserve_qty
     END) AS NUMERIC) AS cp_receipts_reserve_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_reclass_in_cost_amt
     ELSE a.cp_reclass_in_cost_amt
     END) AS NUMERIC) AS cp_reclass_in_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_reclass_in_qty AS BIGNUMERIC)
     ELSE a.cp_reclass_in_qty
     END) AS NUMERIC) AS cp_reclass_in_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_reclass_out_cost_amt
     ELSE a.cp_reclass_out_cost_amt
     END) AS NUMERIC) AS cp_reclass_out_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_reclass_out_qty AS BIGNUMERIC)
     ELSE a.cp_reclass_out_qty
     END) AS NUMERIC) AS cp_reclass_out_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_return_to_vendor_active_cost_amt
     ELSE a.cp_return_to_vendor_active_cost_amt
     END) AS NUMERIC) AS cp_return_to_vendor_active_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_return_to_vendor_active_qty AS BIGNUMERIC)
     ELSE a.cp_return_to_vendor_active_qty
     END) AS NUMERIC) AS cp_return_to_vendor_active_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_return_to_vendor_inactive_cost_amt
     ELSE a.cp_return_to_vendor_inactive_cost_amt
     END) AS NUMERIC) AS cp_return_to_vendor_inactive_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN CAST(a.ty_return_to_vendor_inactive_qty AS BIGNUMERIC)
     ELSE a.cp_return_to_vendor_inactive_qty
     END) AS NUMERIC) AS cp_return_to_vendor_inactive_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_vendor_funds_cost_amt
     ELSE a.cp_vendor_funds_cost_amt
     END) AS NUMERIC) AS cp_vendor_funds_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_discount_terms_cost_amt
     ELSE a.cp_discount_terms_cost_amt
     END) AS NUMERIC) AS cp_discount_terms_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_merch_margin_retail_amt
     ELSE a.cp_merch_margin_retail_amt
     END) AS NUMERIC) AS cp_merch_margin_retail_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_inventory_movement_in_dropship_cost_amt
     END) AS NUMERIC) AS cp_inventory_movement_in_dropship_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_inventory_movement_in_dropship_qty
     END) AS NUMERIC) AS cp_inventory_movement_in_dropship_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_inventory_movement_out_dropship_cost_amt
     END) AS NUMERIC) AS cp_inventory_movement_out_dropship_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_inventory_movement_out_dropship_qty
     END) AS NUMERIC) AS cp_inventory_movement_out_dropship_qty,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN a.ty_other_inventory_adj_cost
     ELSE a.cp_other_inventory_adjustment_cost_amt
     END) AS NUMERIC) AS cp_other_inventory_adjustment_cost_amt,
  CAST(SUM(CASE
     WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
     THEN 0
     ELSE a.cp_other_inventory_adjustment_qty
     END) AS NUMERIC) AS cp_other_inventory_adjustment_qty,
  CAST(SUM(a.cp_open_to_buy_receipts_active_cost_amt) AS NUMERIC) AS cp_open_to_buy_receipts_active_cost_amt,
  CAST(SUM(a.cp_open_to_buy_receipts_inactive_cost_amt) AS NUMERIC) AS cp_open_to_buy_receipts_inactive_cost_amt,
  CAST(SUM(a.op_beginning_of_period_active_cost_amt) AS NUMERIC) AS op_beginning_of_period_active_cost_amt,
  CAST(SUM(a.op_beginning_of_period_active_inventory_target_qty) AS NUMERIC) AS
  op_beginning_of_period_active_inventory_target_qty,
  CAST(SUM(a.op_beginning_of_period_active_qty) AS NUMERIC) AS op_beginning_of_period_active_qty,
  CAST(SUM(a.op_beginning_of_period_inactive_cost_amt) AS NUMERIC) AS op_beginning_of_period_inactive_cost_amt,
  CAST(SUM(a.op_beginning_of_period_inactive_qty) AS NUMERIC) AS op_beginning_of_period_inactive_qty,
  CAST(SUM(a.op_ending_of_period_active_cost_amt) AS NUMERIC) AS op_ending_of_period_active_cost_amt,
  CAST(SUM(a.op_ending_of_period_active_qty) AS NUMERIC) AS op_ending_of_period_active_qty,
  CAST(SUM(a.op_ending_of_period_inactive_cost_amt) AS NUMERIC) AS op_ending_of_period_inactive_cost_amt,
  CAST(SUM(a.op_ending_of_period_inactive_qty) AS NUMERIC) AS op_ending_of_period_inactive_qty,
  CAST(SUM(a.op_transfer_in_active_cost_amt) AS NUMERIC) AS op_transfer_in_active_cost_amt,
  CAST(SUM(a.op_transfer_in_active_qty) AS NUMERIC) AS op_transfer_in_active_qty,
  CAST(SUM(a.op_transfer_out_active_cost_amt) AS NUMERIC) AS op_transfer_out_active_cost_amt,
  CAST(SUM(a.op_transfer_out_active_qty) AS NUMERIC) AS op_transfer_out_active_qty,
  CAST(SUM(a.op_transfer_in_inactive_cost_amt) AS NUMERIC) AS op_transfer_in_inactive_cost_amt,
  CAST(SUM(a.op_transfer_in_inactive_qty) AS NUMERIC) AS op_transfer_in_inactive_qty,
  CAST(SUM(a.op_transfer_out_inactive_cost_amt) AS NUMERIC) AS op_transfer_out_inactive_cost_amt,
  CAST(SUM(a.op_transfer_out_inactive_qty) AS NUMERIC) AS op_transfer_out_inactive_qty,
  CAST(SUM(a.op_transfer_in_other_cost_amt) AS NUMERIC) AS op_transfer_in_other_cost_amt,
  CAST(SUM(a.op_transfer_in_other_qty) AS NUMERIC) AS op_transfer_in_other_qty,
  CAST(SUM(a.op_transfer_out_other_cost_amt) AS NUMERIC) AS op_transfer_out_other_cost_amt,
  CAST(SUM(a.op_transfer_out_other_qty) AS NUMERIC) AS op_transfer_out_other_qty,
  CAST(SUM(a.op_transfer_in_racking_cost_amt) AS NUMERIC) AS op_transfer_in_racking_cost_amt,
  CAST(SUM(a.op_transfer_in_racking_qty) AS NUMERIC) AS op_transfer_in_racking_qty,
  CAST(SUM(a.op_transfer_out_racking_cost_amt) AS NUMERIC) AS op_transfer_out_racking_cost_amt,
  CAST(SUM(a.op_transfer_out_racking_qty) AS NUMERIC) AS op_transfer_out_racking_qty,
  CAST(SUM(a.op_mark_out_of_stock_active_cost_amt) AS NUMERIC) AS op_mark_out_of_stock_active_cost_amt,
  CAST(SUM(a.op_mark_out_of_stock_active_qty) AS NUMERIC) AS op_mark_out_of_stock_active_qty,
  CAST(SUM(a.op_mark_out_of_stock_inactive_cost_amt) AS NUMERIC) AS op_mark_out_of_stock_inactive_cost_amt,
  CAST(SUM(a.op_mark_out_of_stock_inactive_qty) AS NUMERIC) AS op_mark_out_of_stock_inactive_qty,
  CAST(SUM(a.op_receipts_active_cost_amt) AS NUMERIC) AS op_receipts_active_cost_amt,
  CAST(SUM(a.op_receipts_active_qty) AS NUMERIC) AS op_receipts_active_qty,
  CAST(SUM(a.op_receipts_inactive_cost_amt) AS NUMERIC) AS op_receipts_inactive_cost_amt,
  CAST(SUM(a.op_receipts_inactive_qty) AS NUMERIC) AS op_receipts_inactive_qty,
  CAST(SUM(a.op_receipts_reserve_cost_amt) AS NUMERIC) AS op_receipts_reserve_cost_amt,
  CAST(SUM(a.op_receipts_reserve_qty) AS NUMERIC) AS op_receipts_reserve_qty,
  CAST(SUM(a.op_reclass_in_cost_amt) AS NUMERIC) AS op_reclass_in_cost_amt,
  CAST(SUM(a.op_reclass_in_qty) AS NUMERIC) AS op_reclass_in_qty,
  CAST(SUM(a.op_reclass_out_cost_amt) AS NUMERIC) AS op_reclass_out_cost_amt,
  CAST(SUM(a.op_reclass_out_qty) AS NUMERIC) AS op_reclass_out_qty,
  CAST(SUM(a.op_return_to_vendor_active_cost_amt) AS NUMERIC) AS op_return_to_vendor_active_cost_amt,
  CAST(SUM(a.op_return_to_vendor_active_qty) AS NUMERIC) AS op_return_to_vendor_active_qty,
  CAST(SUM(a.op_return_to_vendor_inactive_cost_amt) AS NUMERIC) AS op_return_to_vendor_inactive_cost_amt,
  CAST(SUM(a.op_return_to_vendor_inactive_qty) AS NUMERIC) AS op_return_to_vendor_inactive_qty,
  CAST(SUM(a.op_vendor_funds_cost_amt) AS NUMERIC) AS op_vendor_funds_cost_amt,
  CAST(SUM(a.op_discount_terms_cost_amt) AS NUMERIC) AS op_discount_terms_cost_amt,
  CAST(SUM(a.op_merch_margin_retail_amt) AS NUMERIC) AS op_merch_margin_retail_amt,
  CAST(SUM(a.op_inventory_movement_in_dropship_cost_amt) AS NUMERIC) AS op_inventory_movement_in_dropship_cost_amt,
  CAST(SUM(a.op_inventory_movement_in_dropship_qty) AS NUMERIC) AS op_inventory_movement_in_dropship_qty,
  CAST(SUM(a.op_inventory_movement_out_dropship_cost_amt) AS NUMERIC) AS op_inventory_movement_out_dropship_cost_amt,
  CAST(SUM(a.op_inventory_movement_out_dropship_qty) AS NUMERIC) AS op_inventory_movement_out_dropship_qty,
  CAST(SUM(a.op_other_inventory_adjustment_cost_amt) AS NUMERIC) AS op_other_inventory_adjustment_cost_amt,
  CAST(SUM(a.op_other_inventory_adjustment_qty) AS NUMERIC) AS op_other_inventory_adjustment_qty,
  CAST(SUM(a.sp_beginning_of_period_active_cost_amt) AS NUMERIC) AS sp_beginning_of_period_active_cost_amt,
  CAST(SUM(a.sp_beginning_of_period_active_inventory_target_qty) AS NUMERIC) AS
  sp_beginning_of_period_active_inventory_target_qty,
  CAST(SUM(a.sp_beginning_of_period_active_qty) AS NUMERIC) AS sp_beginning_of_period_active_qty,
  CAST(SUM(a.sp_beginning_of_period_inactive_cost_amt) AS NUMERIC) AS sp_beginning_of_period_inactive_cost_amt,
  CAST(SUM(a.sp_beginning_of_period_inactive_qty) AS NUMERIC) AS sp_beginning_of_period_inactive_qty,
  CAST(SUM(a.sp_ending_of_period_active_cost_amt) AS NUMERIC) AS sp_ending_of_period_active_cost_amt,
  CAST(SUM(a.sp_ending_of_period_active_qty) AS NUMERIC) AS sp_ending_of_period_active_qty,
  CAST(SUM(a.sp_ending_of_period_inactive_cost_amt) AS NUMERIC) AS sp_ending_of_period_inactive_cost_amt,
  CAST(SUM(a.sp_ending_of_period_inactive_qty) AS NUMERIC) AS sp_ending_of_period_inactive_qty,
  CAST(SUM(a.sp_transfer_in_active_cost_amt) AS NUMERIC) AS sp_transfer_in_active_cost_amt,
  CAST(SUM(a.sp_transfer_in_active_qty) AS NUMERIC) AS sp_transfer_in_active_qty,
  CAST(SUM(a.sp_transfer_out_active_cost_amt) AS NUMERIC) AS sp_transfer_out_active_cost_amt,
  CAST(SUM(a.sp_transfer_out_active_qty) AS NUMERIC) AS sp_transfer_out_active_qty,
  CAST(SUM(a.sp_transfer_in_inactive_cost_amt) AS NUMERIC) AS sp_transfer_in_inactive_cost_amt,
  CAST(SUM(a.sp_transfer_in_inactive_qty) AS NUMERIC) AS sp_transfer_in_inactive_qty,
  CAST(SUM(a.sp_transfer_out_inactive_cost_amt) AS NUMERIC) AS sp_transfer_out_inactive_cost_amt,
  CAST(SUM(a.sp_transfer_out_inactive_qty) AS NUMERIC) AS sp_transfer_out_inactive_qty,
  CAST(SUM(a.sp_transfer_in_other_cost_amt) AS NUMERIC) AS sp_transfer_in_other_cost_amt,
  CAST(SUM(a.sp_transfer_in_other_qty) AS NUMERIC) AS sp_transfer_in_other_qty,
  CAST(SUM(a.sp_transfer_out_other_cost_amt) AS NUMERIC) AS sp_transfer_out_other_cost_amt,
  CAST(SUM(a.sp_transfer_out_other_qty) AS NUMERIC) AS sp_transfer_out_other_qty,
  CAST(SUM(a.sp_transfer_in_racking_cost_amt) AS NUMERIC) AS sp_transfer_in_racking_cost_amt,
  CAST(SUM(a.sp_transfer_in_racking_qty) AS NUMERIC) AS sp_transfer_in_racking_qty,
  CAST(SUM(a.sp_transfer_out_racking_cost_amt) AS NUMERIC) AS sp_transfer_out_racking_cost_amt,
  CAST(SUM(a.sp_transfer_out_racking_qty) AS NUMERIC) AS sp_transfer_out_racking_qty,
  CAST(SUM(a.sp_mark_out_of_stock_active_cost_amt) AS NUMERIC) AS sp_mark_out_of_stock_active_cost_amt,
  CAST(SUM(a.sp_mark_out_of_stock_active_qty) AS NUMERIC) AS sp_mark_out_of_stock_active_qty,
  CAST(SUM(a.sp_mark_out_of_stock_inactive_cost_amt) AS NUMERIC) AS sp_mark_out_of_stock_inactive_cost_amt,
  CAST(SUM(a.sp_mark_out_of_stock_inactive_qty) AS NUMERIC) AS sp_mark_out_of_stock_inactive_qty,
  CAST(SUM(a.sp_receipts_active_cost_amt) AS NUMERIC) AS sp_receipts_active_cost_amt,
  CAST(SUM(a.sp_receipts_active_qty) AS NUMERIC) AS sp_receipts_active_qty,
  CAST(SUM(a.sp_receipts_inactive_cost_amt) AS NUMERIC) AS sp_receipts_inactive_cost_amt,
  CAST(SUM(a.sp_receipts_inactive_qty) AS NUMERIC) AS sp_receipts_inactive_qty,
  CAST(SUM(a.sp_receipts_reserve_cost_amt) AS NUMERIC) AS sp_receipts_reserve_cost_amt,
  CAST(SUM(a.sp_receipts_reserve_qty) AS NUMERIC) AS sp_receipts_reserve_qty,
  CAST(SUM(a.sp_reclass_in_cost_amt) AS NUMERIC) AS sp_reclass_in_cost_amt,
  CAST(SUM(a.sp_reclass_in_qty) AS NUMERIC) AS sp_reclass_in_qty,
  CAST(SUM(a.sp_reclass_out_cost_amt) AS NUMERIC) AS sp_reclass_out_cost_amt,
  CAST(SUM(a.sp_reclass_out_qty) AS NUMERIC) AS sp_reclass_out_qty,
  CAST(SUM(a.sp_return_to_vendor_active_cost_amt) AS NUMERIC) AS sp_return_to_vendor_active_cost_amt,
  CAST(SUM(a.sp_return_to_vendor_active_qty) AS NUMERIC) AS sp_return_to_vendor_active_qty,
  CAST(SUM(a.sp_return_to_vendor_inactive_cost_amt) AS NUMERIC) AS sp_return_to_vendor_inactive_cost_amt,
  CAST(SUM(a.sp_return_to_vendor_inactive_qty) AS NUMERIC) AS sp_return_to_vendor_inactive_qty,
  CAST(SUM(a.sp_vendor_funds_cost_amt) AS NUMERIC) AS sp_vendor_funds_cost_amt,
  CAST(SUM(a.sp_discount_terms_cost_amt) AS NUMERIC) AS sp_discount_terms_cost_amt,
  CAST(SUM(a.sp_merch_margin_retail_amt) AS NUMERIC) AS sp_merch_margin_retail_amt,
  CAST(SUM(a.sp_inventory_movement_in_dropship_cost_amt) AS NUMERIC) AS sp_inventory_movement_in_dropship_cost_amt,
  CAST(SUM(a.sp_inventory_movement_in_dropship_qty) AS NUMERIC) AS sp_inventory_movement_in_dropship_qty,
  CAST(SUM(a.sp_inventory_movement_out_dropship_cost_amt) AS NUMERIC) AS sp_inventory_movement_out_dropship_cost_amt,
  CAST(SUM(a.sp_inventory_movement_out_dropship_qty) AS NUMERIC) AS sp_inventory_movement_out_dropship_qty,
  CAST(SUM(a.sp_other_inventory_adjustment_cost_amt) AS NUMERIC) AS sp_other_inventory_adjustment_cost_amt,
  CAST(SUM(a.sp_other_inventory_adjustment_qty) AS NUMERIC) AS sp_other_inventory_adjustment_qty,
  CAST(SUM(a.sp_open_to_buy_receipts_active_cost_amt) AS NUMERIC) AS sp_open_to_buy_receipts_active_cost_amt,
  CAST(SUM(a.sp_open_to_buy_receipts_inactive_cost_amt) AS NUMERIC) AS sp_open_to_buy_receipts_inactive_cost_amt,
  SUM(a.ty_beginning_of_period_active_cost_amt) AS ty_beginning_of_period_active_cost_amt,
  SUM(a.ty_beginning_of_period_active_qty) AS ty_beginning_of_period_active_qty,
  SUM(a.ty_beginning_of_period_inactive_cost_amt) AS ty_beginning_of_period_inactive_cost_amt,
  SUM(a.ty_beginning_of_period_inactive_qty) AS ty_beginning_of_period_inactive_qty,
  SUM(a.ty_ending_of_period_active_cost_amt) AS ty_ending_of_period_active_cost_amt,
  SUM(a.ty_ending_of_period_active_qty) AS ty_ending_of_period_active_qty,
  SUM(a.ty_ending_of_period_inactive_cost_amt) AS ty_ending_of_period_inactive_cost_amt,
  SUM(a.ty_ending_of_period_inactive_qty) AS ty_ending_of_period_inactive_qty,
  SUM(a.ty_transfer_in_active_cost_amt) AS ty_transfer_in_active_cost_amt,
  SUM(a.ty_transfer_in_active_qty) AS ty_transfer_in_active_qty,
  SUM(a.ty_transfer_out_active_cost_amt) AS ty_transfer_out_active_cost_amt,
  SUM(a.ty_transfer_out_active_qty) AS ty_transfer_out_active_qty,
  SUM(a.ty_transfer_in_inactive_cost_amt) AS ty_transfer_in_inactive_cost_amt,
  SUM(a.ty_transfer_in_inactive_qty) AS ty_transfer_in_inactive_qty,
  SUM(a.ty_transfer_out_inactive_cost_amt) AS ty_transfer_out_inactive_cost_amt,
  SUM(a.ty_transfer_out_inactive_qty) AS ty_transfer_out_inactive_qty,
  SUM(a.ty_transfer_in_other_cost_amt) AS ty_transfer_in_other_cost_amt,
  SUM(a.ty_transfer_in_other_qty) AS ty_transfer_in_other_qty,
  SUM(a.ty_transfer_out_other_cost_amt) AS ty_transfer_out_other_cost_amt,
  SUM(a.ty_transfer_out_other_qty) AS ty_transfer_out_other_qty,
  SUM(a.ty_transfer_in_racking_cost_amt) AS ty_transfer_in_racking_cost_amt,
  SUM(a.ty_transfer_in_racking_qty) AS ty_transfer_in_racking_qty,
  SUM(a.ty_transfer_out_racking_cost_amt) AS ty_transfer_out_racking_cost_amt,
  SUM(a.ty_transfer_out_racking_qty) AS ty_transfer_out_racking_qty,
  SUM(a.ty_mark_out_of_stock_active_cost_amt) AS ty_mark_out_of_stock_active_cost_amt,
  SUM(a.ty_mark_out_of_stock_active_qty) AS ty_mark_out_of_stock_active_qty,
  SUM(a.ty_mark_out_of_stock_inactive_cost_amt) AS ty_mark_out_of_stock_inactive_cost_amt,
  SUM(a.ty_mark_out_of_stock_inactive_qty) AS ty_mark_out_of_stock_inactive_qty,
  SUM(a.ty_receipts_active_cost_amt) AS ty_receipts_active_cost_amt,
  SUM(a.ty_receipts_active_qty) AS ty_receipts_active_qty,
  SUM(a.ty_receipts_inactive_cost_amt) AS ty_receipts_inactive_cost_amt,
  SUM(a.ty_receipts_inactive_qty) AS ty_receipts_inactive_qty,
  SUM(a.ty_rp_receipts_active_cost_amt) AS ty_rp_receipts_active_cost_amt,
  SUM(a.ty_non_rp_receipts_active_cost_amt) AS ty_non_rp_receipts_active_cost_amt,
  SUM(a.ty_reclass_in_cost_amt) AS ty_reclass_in_cost_amt,
  SUM(a.ty_reclass_in_qty) AS ty_reclass_in_qty,
  SUM(a.ty_reclass_out_cost_amt) AS ty_reclass_out_cost_amt,
  SUM(a.ty_reclass_out_qty) AS ty_reclass_out_qty,
  SUM(a.ty_return_to_vendor_active_cost_amt) AS ty_return_to_vendor_active_cost_amt,
  SUM(a.ty_return_to_vendor_active_qty) AS ty_return_to_vendor_active_qty,
  SUM(a.ty_return_to_vendor_inactive_cost_amt) AS ty_return_to_vendor_inactive_cost_amt,
  SUM(a.ty_return_to_vendor_inactive_qty) AS ty_return_to_vendor_inactive_qty,
  SUM(a.ty_vendor_funds_cost_amt) AS ty_vendor_funds_cost_amt,
  SUM(a.ty_discount_terms_cost_amt) AS ty_discount_terms_cost_amt,
  SUM(a.ty_merch_margin_retail_amt) AS ty_merch_margin_retail_amt,
  SUM(a.ty_shrink_banner_cost_amt) AS ty_shrink_banner_cost_amt,
  CAST(SUM(a.ty_shrink_banner_qty) AS BIGINT) AS ty_shrink_banner_qty,
  SUM(a.ty_other_inventory_adj_cost) AS ty_other_inventory_adj_cost,
  SUM(a.ty_merch_margin_charges_cost) AS ty_merch_margin_charges_cost,
  CAST(SUM(a.ly_beginning_of_period_active_cost_amt) AS NUMERIC) AS ly_beginning_of_period_active_cost_amt,
  CAST(SUM(a.ly_beginning_of_period_active_qty) AS BIGINT) AS ly_beginning_of_period_active_qty,
  CAST(SUM(a.ly_beginning_of_period_inactive_cost_amt) AS NUMERIC) AS ly_beginning_of_period_inactive_cost_amt,
  CAST(SUM(a.ly_beginning_of_period_inactive_qty) AS BIGINT) AS ly_beginning_of_period_inactive_qty,
  CAST(SUM(a.ly_ending_of_period_active_cost_amt) AS NUMERIC) AS ly_ending_of_period_active_cost_amt,
  CAST(SUM(a.ly_ending_of_period_active_qty) AS BIGINT) AS ly_ending_of_period_active_qty,
  CAST(SUM(a.ly_ending_of_period_inactive_cost_amt) AS NUMERIC) AS ly_ending_of_period_inactive_cost_amt,
  CAST(SUM(a.ly_ending_of_period_inactive_qty) AS BIGINT) AS ly_ending_of_period_inactive_qty,
  CAST(SUM(a.ly_transfer_in_active_cost_amt) AS NUMERIC) AS ly_transfer_in_active_cost_amt,
  CAST(SUM(a.ly_transfer_in_active_qty) AS BIGINT) AS ly_transfer_in_active_qty,
  CAST(SUM(a.ly_transfer_out_active_cost_amt) AS NUMERIC) AS ly_transfer_out_active_cost_amt,
  CAST(SUM(a.ly_transfer_out_active_qty) AS BIGINT) AS ly_transfer_out_active_qty,
  CAST(SUM(a.ly_transfer_in_inactive_cost_amt) AS NUMERIC) AS ly_transfer_in_inactive_cost_amt,
  CAST(SUM(a.ly_transfer_in_inactive_qty) AS BIGINT) AS ly_transfer_in_inactive_qty,
  CAST(SUM(a.ly_transfer_out_inactive_cost_amt) AS NUMERIC) AS ly_transfer_out_inactive_cost_amt,
  CAST(SUM(a.ly_transfer_out_inactive_qty) AS BIGINT) AS ly_transfer_out_inactive_qty,
  CAST(SUM(a.ly_transfer_in_other_cost_amt) AS NUMERIC) AS ly_transfer_in_other_cost_amt,
  CAST(SUM(a.ly_transfer_in_other_qty) AS BIGINT) AS ly_transfer_in_other_qty,
  CAST(SUM(a.ly_transfer_out_other_cost_amt) AS NUMERIC) AS ly_transfer_out_other_cost_amt,
  CAST(SUM(a.ly_transfer_out_other_qty) AS BIGINT) AS ly_transfer_out_other_qty,
  CAST(SUM(a.ly_transfer_in_racking_cost_amt) AS NUMERIC) AS ly_transfer_in_racking_cost_amt,
  CAST(SUM(a.ly_transfer_in_racking_qty) AS BIGINT) AS ly_transfer_in_racking_qty,
  CAST(SUM(a.ly_transfer_out_racking_cost_amt) AS NUMERIC) AS ly_transfer_out_racking_cost_amt,
  CAST(SUM(a.ly_transfer_out_racking_qty) AS BIGINT) AS ly_transfer_out_racking_qty,
  CAST(SUM(a.ly_mark_out_of_stock_active_cost_amt) AS NUMERIC) AS ly_mark_out_of_stock_active_cost_amt,
  CAST(SUM(a.ly_mark_out_of_stock_active_qty) AS BIGINT) AS ly_mark_out_of_stock_active_qty,
  CAST(SUM(a.ly_mark_out_of_stock_inactive_cost_amt) AS NUMERIC) AS ly_mark_out_of_stock_inactive_cost_amt,
  CAST(SUM(a.ly_mark_out_of_stock_inactive_qty) AS BIGINT) AS ly_mark_out_of_stock_inactive_qty,
  CAST(SUM(a.ly_receipts_active_cost_amt) AS NUMERIC) AS ly_receipts_active_cost_amt,
  CAST(SUM(a.ly_receipts_active_qty) AS BIGINT) AS ly_receipts_active_qty,
  CAST(SUM(a.ly_receipts_inactive_cost_amt) AS NUMERIC) AS ly_receipts_inactive_cost_amt,
  CAST(SUM(a.ly_receipts_inactive_qty) AS BIGINT) AS ly_receipts_inactive_qty,
  SUM(a.ly_rp_receipts_active_cost_amt) AS ly_rp_receipts_active_cost_amt,
  SUM(a.ly_non_rp_receipts_active_cost_amt) AS ly_non_rp_receipts_active_cost_amt,
  CAST(SUM(a.ly_reclass_in_cost_amt) AS NUMERIC) AS ly_reclass_in_cost_amt,
  CAST(SUM(a.ly_reclass_in_qty) AS BIGINT) AS ly_reclass_in_qty,
  CAST(SUM(a.ly_reclass_out_cost_amt) AS NUMERIC) AS ly_reclass_out_cost_amt,
  CAST(SUM(a.ly_reclass_out_qty) AS BIGINT) AS ly_reclass_out_qty,
  CAST(SUM(a.ly_return_to_vendor_active_cost_amt) AS NUMERIC) AS ly_return_to_vendor_active_cost_amt,
  CAST(SUM(a.ly_return_to_vendor_active_qty) AS BIGINT) AS ly_return_to_vendor_active_qty,
  CAST(SUM(a.ly_return_to_vendor_inactive_cost_amt) AS NUMERIC) AS ly_return_to_vendor_inactive_cost_amt,
  CAST(SUM(a.ly_return_to_vendor_inactive_qty) AS BIGINT) AS ly_return_to_vendor_inactive_qty,
  CAST(SUM(a.ly_vendor_funds_cost_amt) AS NUMERIC) AS ly_vendor_funds_cost_amt,
  CAST(SUM(a.ly_discount_terms_cost_amt) AS NUMERIC) AS ly_discount_terms_cost_amt,
  CAST(SUM(a.ly_merch_margin_retail_amt) AS NUMERIC) AS ly_merch_margin_retail_amt,
  SUM(a.ly_shrink_banner_cost_amt) AS ly_shrink_banner_cost_amt,
  CAST(SUM(a.ly_shrink_banner_qty) AS BIGINT) AS ly_shrink_banner_qty,
  CAST(SUM(a.ly_other_inventory_adj_cost) AS NUMERIC) AS ly_other_inventory_adj_cost,
  SUM(a.ly_merch_margin_charges_cost) AS ly_merch_margin_charges_cost,
  SUM(a.lly_beginning_of_period_active_cost_amt) AS lly_beginning_of_period_active_cost_amt,
  SUM(a.lly_beginning_of_period_active_qty) AS lly_beginning_of_period_active_qty,
  SUM(a.lly_beginning_of_period_inactive_cost_amt) AS lly_beginning_of_period_inactive_cost_amt,
  SUM(a.lly_beginning_of_period_inactive_qty) AS lly_beginning_of_period_inactive_qty,
  SUM(a.lly_ending_of_period_active_cost_amt) AS lly_ending_of_period_active_cost_amt,
  SUM(a.lly_ending_of_period_active_qty) AS lly_ending_of_period_active_qty,
  SUM(a.lly_ending_of_period_inactive_cost_amt) AS lly_ending_of_period_inactive_cost_amt,
  SUM(a.lly_ending_of_period_inactive_qty) AS lly_ending_of_period_inactive_qty,
  SUM(a.lly_transfer_in_active_cost_amt) AS lly_transfer_in_active_cost_amt,
  SUM(a.lly_transfer_in_active_qty) AS lly_transfer_in_active_qty,
  SUM(a.lly_transfer_out_active_cost_amt) AS lly_transfer_out_active_cost_amt,
  SUM(a.lly_transfer_out_active_qty) AS lly_transfer_out_active_qty,
  SUM(a.lly_transfer_in_inactive_cost_amt) AS lly_transfer_in_inactive_cost_amt,
  SUM(a.lly_transfer_in_inactive_qty) AS lly_transfer_in_inactive_qty,
  SUM(a.lly_transfer_out_inactive_cost_amt) AS lly_transfer_out_inactive_cost_amt,
  SUM(a.lly_transfer_out_inactive_qty) AS lly_transfer_out_inactive_qty,
  SUM(a.lly_transfer_in_other_cost_amt) AS lly_transfer_in_other_cost_amt,
  SUM(a.lly_transfer_in_other_qty) AS lly_transfer_in_other_qty,
  SUM(a.lly_transfer_out_other_cost_amt) AS lly_transfer_out_other_cost_amt,
  SUM(a.lly_transfer_out_other_qty) AS lly_transfer_out_other_qty,
  SUM(a.lly_transfer_in_racking_cost_amt) AS lly_transfer_in_racking_cost_amt,
  SUM(a.lly_transfer_in_racking_qty) AS lly_transfer_in_racking_qty,
  SUM(a.lly_transfer_out_racking_cost_amt) AS lly_transfer_out_racking_cost_amt,
  SUM(a.lly_transfer_out_racking_qty) AS lly_transfer_out_racking_qty,
  SUM(a.lly_mark_out_of_stock_active_cost_amt) AS lly_mark_out_of_stock_active_cost_amt,
  SUM(a.lly_mark_out_of_stock_active_qty) AS lly_mark_out_of_stock_active_qty,
  SUM(a.lly_mark_out_of_stock_inactive_cost_amt) AS lly_mark_out_of_stock_inactive_cost_amt,
  SUM(a.lly_mark_out_of_stock_inactive_qty) AS lly_mark_out_of_stock_inactive_qty,
  SUM(a.lly_receipts_active_cost_amt) AS lly_receipts_active_cost_amt,
  SUM(a.lly_receipts_active_qty) AS lly_receipts_active_qty,
  SUM(a.lly_receipts_inactive_cost_amt) AS lly_receipts_inactive_cost_amt,
  SUM(a.lly_receipts_inactive_qty) AS lly_receipts_inactive_qty,
  SUM(a.lly_rp_receipts_active_cost_amt) AS lly_rp_receipts_active_cost_amt,
  SUM(a.lly_non_rp_receipts_active_cost_amt) AS lly_non_rp_receipts_active_cost_amt,
  SUM(a.lly_reclass_in_cost_amt) AS lly_reclass_in_cost_amt,
  SUM(a.lly_reclass_in_qty) AS lly_reclass_in_qty,
  SUM(a.lly_reclass_out_cost_amt) AS lly_reclass_out_cost_amt,
  SUM(a.lly_reclass_out_qty) AS lly_reclass_out_qty,
  SUM(a.lly_return_to_vendor_active_cost_amt) AS lly_return_to_vendor_active_cost_amt,
  SUM(a.lly_return_to_vendor_active_qty) AS lly_return_to_vendor_active_qty,
  SUM(a.lly_return_to_vendor_inactive_cost_amt) AS lly_return_to_vendor_inactive_cost_amt,
  SUM(a.lly_return_to_vendor_inactive_qty) AS lly_return_to_vendor_inactive_qty,
  SUM(a.lly_vendor_funds_cost_amt) AS lly_vendor_funds_cost_amt,
  SUM(a.lly_discount_terms_cost_amt) AS lly_discount_terms_cost_amt,
  SUM(a.lly_merch_margin_retail_amt) AS lly_merch_margin_retail_amt,
  SUM(a.lly_shrink_banner_cost_amt) AS lly_shrink_banner_cost_amt,
  CAST(SUM(a.lly_shrink_banner_qty) AS BIGINT) AS lly_shrink_banner_qty,
  SUM(a.lly_other_inventory_adj_cost) AS lly_other_inventory_adj_cost,
  SUM(a.lly_merch_margin_charges_cost) AS lly_merch_margin_charges_cost,
  MAX(a.dw_batch_dt) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT COALESCE(iifbv.department_num, iif.department_num, mtlif.department_num, pif.department_num, mrif.department_num
      , rtvif.department_num, vfmdt.department_num, mmif.department_num) AS dept_num,
     COALESCE(iifbv.current_week_num, iif.week_num, mtlif.week_num, pif.week_num, mrif.week_num, rtvif.week_num, vfmdt.week_num
      , mmif.week_num) AS week_num,
     COALESCE(iifbv.banner_num, iif.banner_num, mtlif.banner_num, pif.banner_num, mrif.banner_num, rtvif.banner_num,
      vfmdt.banner_num, mmif.banner_num) AS banner_country_num,
     COALESCE(iifbv.fulfilment_num, iif.fulfilment_num, mtlif.fulfilment_num, pif.fulfilment_num, mrif.fulfilment_num,
      rtvif.fulfilment_num, vfmdt.fulfilment_num, mmif.fulfilment_num) AS fulfill_type_num,
     0 AS cp_beginning_of_period_active_cost_amt,
     0 AS cp_beginning_of_period_active_inventory_target_qty,
     0 AS cp_beginning_of_period_active_qty,
     0 AS cp_beginning_of_period_inactive_cost_amt,
     0 AS cp_beginning_of_period_inactive_qty,
     0 AS cp_ending_of_period_active_cost_amt,
     0 AS cp_ending_of_period_active_qty,
     0 AS cp_ending_of_period_inactive_cost_amt,
     0 AS cp_ending_of_period_inactive_qty,
     0 AS cp_transfer_in_active_cost_amt,
     0 AS cp_transfer_in_active_qty,
     0 AS cp_transfer_out_active_cost_amt,
     0 AS cp_transfer_out_active_qty,
     0 AS cp_transfer_in_inactive_cost_amt,
     0 AS cp_transfer_in_inactive_qty,
     0 AS cp_transfer_out_inactive_cost_amt,
     0 AS cp_transfer_out_inactive_qty,
     0 AS cp_transfer_in_other_cost_amt,
     0 AS cp_transfer_in_other_qty,
     0 AS cp_transfer_out_other_cost_amt,
     0 AS cp_transfer_out_other_qty,
     0 AS cp_transfer_in_racking_cost_amt,
     0 AS cp_transfer_in_racking_qty,
     0 AS cp_transfer_out_racking_cost_amt,
     0 AS cp_transfer_out_racking_qty,
     0 AS cp_mark_out_of_stock_active_cost_amt,
     0 AS cp_mark_out_of_stock_active_qty,
     0 AS cp_mark_out_of_stock_inactive_cost_amt,
     0 AS cp_mark_out_of_stock_inactive_qty,
     0 AS cp_receipts_active_cost_amt,
     0 AS cp_receipts_active_qty,
     0 AS cp_receipts_inactive_cost_amt,
     0 AS cp_receipts_inactive_qty,
     0 AS cp_receipts_reserve_cost_amt,
     0 AS cp_receipts_reserve_qty,
     0 AS cp_reclass_in_cost_amt,
     0 AS cp_reclass_in_qty,
     0 AS cp_reclass_out_cost_amt,
     0 AS cp_reclass_out_qty,
     0 AS cp_return_to_vendor_active_cost_amt,
     0 AS cp_return_to_vendor_active_qty,
     0 AS cp_return_to_vendor_inactive_cost_amt,
     0 AS cp_return_to_vendor_inactive_qty,
     0 AS cp_vendor_funds_cost_amt,
     0 AS cp_discount_terms_cost_amt,
     0 AS cp_merch_margin_retail_amt,
     0 AS cp_inventory_movement_in_dropship_cost_amt,
     0 AS cp_inventory_movement_in_dropship_qty,
     0 AS cp_inventory_movement_out_dropship_cost_amt,
     0 AS cp_inventory_movement_out_dropship_qty,
     0 AS cp_other_inventory_adjustment_cost_amt,
     0 AS cp_other_inventory_adjustment_qty,
     0 AS cp_open_to_buy_receipts_active_cost_amt,
     0 AS cp_open_to_buy_receipts_inactive_cost_amt,
     0 AS op_beginning_of_period_active_cost_amt,
     0 AS op_beginning_of_period_active_inventory_target_qty,
     0 AS op_beginning_of_period_active_qty,
     0 AS op_beginning_of_period_inactive_cost_amt,
     0 AS op_beginning_of_period_inactive_qty,
     0 AS op_ending_of_period_active_cost_amt,
     0 AS op_ending_of_period_active_qty,
     0 AS op_ending_of_period_inactive_cost_amt,
     0 AS op_ending_of_period_inactive_qty,
     0 AS op_transfer_in_active_cost_amt,
     0 AS op_transfer_in_active_qty,
     0 AS op_transfer_out_active_cost_amt,
     0 AS op_transfer_out_active_qty,
     0 AS op_transfer_in_inactive_cost_amt,
     0 AS op_transfer_in_inactive_qty,
     0 AS op_transfer_out_inactive_cost_amt,
     0 AS op_transfer_out_inactive_qty,
     0 AS op_transfer_in_other_cost_amt,
     0 AS op_transfer_in_other_qty,
     0 AS op_transfer_out_other_cost_amt,
     0 AS op_transfer_out_other_qty,
     0 AS op_transfer_in_racking_cost_amt,
     0 AS op_transfer_in_racking_qty,
     0 AS op_transfer_out_racking_cost_amt,
     0 AS op_transfer_out_racking_qty,
     0 AS op_mark_out_of_stock_active_cost_amt,
     0 AS op_mark_out_of_stock_active_qty,
     0 AS op_mark_out_of_stock_inactive_cost_amt,
     0 AS op_mark_out_of_stock_inactive_qty,
     0 AS op_receipts_active_cost_amt,
     0 AS op_receipts_active_qty,
     0 AS op_receipts_inactive_cost_amt,
     0 AS op_receipts_inactive_qty,
     0 AS op_receipts_reserve_cost_amt,
     0 AS op_receipts_reserve_qty,
     0 AS op_reclass_in_cost_amt,
     0 AS op_reclass_in_qty,
     0 AS op_reclass_out_cost_amt,
     0 AS op_reclass_out_qty,
     0 AS op_return_to_vendor_active_cost_amt,
     0 AS op_return_to_vendor_active_qty,
     0 AS op_return_to_vendor_inactive_cost_amt,
     0 AS op_return_to_vendor_inactive_qty,
     0 AS op_vendor_funds_cost_amt,
     0 AS op_discount_terms_cost_amt,
     0 AS op_merch_margin_retail_amt,
     0 AS op_inventory_movement_in_dropship_cost_amt,
     0 AS op_inventory_movement_in_dropship_qty,
     0 AS op_inventory_movement_out_dropship_cost_amt,
     0 AS op_inventory_movement_out_dropship_qty,
     0 AS op_other_inventory_adjustment_cost_amt,
     0 AS op_other_inventory_adjustment_qty,
     0 AS sp_beginning_of_period_active_cost_amt,
     0 AS sp_beginning_of_period_active_inventory_target_qty,
     0 AS sp_beginning_of_period_active_qty,
     0 AS sp_beginning_of_period_inactive_cost_amt,
     0 AS sp_beginning_of_period_inactive_qty,
     0 AS sp_ending_of_period_active_cost_amt,
     0 AS sp_ending_of_period_active_qty,
     0 AS sp_ending_of_period_inactive_cost_amt,
     0 AS sp_ending_of_period_inactive_qty,
     0 AS sp_transfer_in_active_cost_amt,
     0 AS sp_transfer_in_active_qty,
     0 AS sp_transfer_out_active_cost_amt,
     0 AS sp_transfer_out_active_qty,
     0 AS sp_transfer_in_inactive_cost_amt,
     0 AS sp_transfer_in_inactive_qty,
     0 AS sp_transfer_out_inactive_cost_amt,
     0 AS sp_transfer_out_inactive_qty,
     0 AS sp_transfer_in_other_cost_amt,
     0 AS sp_transfer_in_other_qty,
     0 AS sp_transfer_out_other_cost_amt,
     0 AS sp_transfer_out_other_qty,
     0 AS sp_transfer_in_racking_cost_amt,
     0 AS sp_transfer_in_racking_qty,
     0 AS sp_transfer_out_racking_cost_amt,
     0 AS sp_transfer_out_racking_qty,
     0 AS sp_mark_out_of_stock_active_cost_amt,
     0 AS sp_mark_out_of_stock_active_qty,
     0 AS sp_mark_out_of_stock_inactive_cost_amt,
     0 AS sp_mark_out_of_stock_inactive_qty,
     0 AS sp_receipts_active_cost_amt,
     0 AS sp_receipts_active_qty,
     0 AS sp_receipts_inactive_cost_amt,
     0 AS sp_receipts_inactive_qty,
     0 AS sp_receipts_reserve_cost_amt,
     0 AS sp_receipts_reserve_qty,
     0 AS sp_reclass_in_cost_amt,
     0 AS sp_reclass_in_qty,
     0 AS sp_reclass_out_cost_amt,
     0 AS sp_reclass_out_qty,
     0 AS sp_return_to_vendor_active_cost_amt,
     0 AS sp_return_to_vendor_active_qty,
     0 AS sp_return_to_vendor_inactive_cost_amt,
     0 AS sp_return_to_vendor_inactive_qty,
     0 AS sp_vendor_funds_cost_amt,
     0 AS sp_discount_terms_cost_amt,
     0 AS sp_merch_margin_retail_amt,
     0 AS sp_inventory_movement_in_dropship_cost_amt,
     0 AS sp_inventory_movement_in_dropship_qty,
     0 AS sp_inventory_movement_out_dropship_cost_amt,
     0 AS sp_inventory_movement_out_dropship_qty,
     0 AS sp_other_inventory_adjustment_cost_amt,
     0 AS sp_other_inventory_adjustment_qty,
     0 AS sp_open_to_buy_receipts_active_cost_amt,
     0 AS sp_open_to_buy_receipts_inactive_cost_amt,
     COALESCE(iifbv.stock_on_hand_active_cost, 0) AS ty_beginning_of_period_active_cost_amt,
     COALESCE(iifbv.stock_on_hand_active_units, 0) AS ty_beginning_of_period_active_qty,
     COALESCE(iifbv.stock_on_hand_inactive_cost, 0) AS ty_beginning_of_period_inactive_cost_amt,
     COALESCE(iifbv.stock_on_hand_inactive_units, 0) AS ty_beginning_of_period_inactive_qty,
     COALESCE(iif.stock_on_hand_active_cost, 0) AS ty_ending_of_period_active_cost_amt,
     COALESCE(iif.stock_on_hand_active_units, 0) AS ty_ending_of_period_active_qty,
     COALESCE(iif.stock_on_hand_inactive_cost, 0) AS ty_ending_of_period_inactive_cost_amt,
     COALESCE(iif.stock_on_hand_inactive_units, 0) AS ty_ending_of_period_inactive_qty,
     COALESCE(mtlif.active_transfer_in_c, 0) AS ty_transfer_in_active_cost_amt,
     COALESCE(mtlif.active_transfer_in_u, 0) AS ty_transfer_in_active_qty,
     COALESCE(mtlif.active_transfer_out_c, 0) AS ty_transfer_out_active_cost_amt,
     COALESCE(mtlif.active_transfer_out_u, 0) AS ty_transfer_out_active_qty,
     COALESCE(mtlif.inactive_transfer_in_c, 0) AS ty_transfer_in_inactive_cost_amt,
     COALESCE(mtlif.inactive_transfer_in_u, 0) AS ty_transfer_in_inactive_qty,
     COALESCE(mtlif.inactive_transfer_out_c, 0) AS ty_transfer_out_inactive_cost_amt,
     COALESCE(mtlif.inactive_transfer_out_u, 0) AS ty_transfer_out_inactive_qty,
     COALESCE(mtlif.other_transfer_in_c, 0) AS ty_transfer_in_other_cost_amt,
     COALESCE(mtlif.other_transfer_in_u, 0) AS ty_transfer_in_other_qty,
     COALESCE(mtlif.other_transfer_out_c, 0) AS ty_transfer_out_other_cost_amt,
     COALESCE(mtlif.other_transfer_out_u, 0) AS ty_transfer_out_other_qty,
     COALESCE(mtlif.racking_transfer_in_c, 0) AS ty_transfer_in_racking_cost_amt,
     COALESCE(mtlif.racking_transfer_in_u, 0) AS ty_transfer_in_racking_qty,
     COALESCE(mtlif.racking_transfer_out_c, 0) AS ty_transfer_out_racking_cost_amt,
     COALESCE(mtlif.racking_transfer_out_u, 0) AS ty_transfer_out_racking_qty,
     COALESCE(mmif.mos_active_cost, 0) AS ty_mark_out_of_stock_active_cost_amt,
     COALESCE(mmif.mos_active_units, 0) AS ty_mark_out_of_stock_active_qty,
     COALESCE(mmif.mos_inactive_cost, 0) AS ty_mark_out_of_stock_inactive_cost_amt,
     COALESCE(mmif.mos_inactive_units, 0) AS ty_mark_out_of_stock_inactive_qty,
     COALESCE(pif.receipts_active_cost, 0) AS ty_receipts_active_cost_amt,
     COALESCE(pif.receipts_active_units, 0) AS ty_receipts_active_qty,
     COALESCE(pif.receipts_inactive_cost, 0) AS ty_receipts_inactive_cost_amt,
     COALESCE(pif.receipts_inactive_units, 0) AS ty_receipts_inactive_qty,
     COALESCE(pif.rp_receipts_active_cost, 0) AS ty_rp_receipts_active_cost_amt,
     COALESCE(pif.non_rp_receipts_active_cost, 0) AS ty_non_rp_receipts_active_cost_amt,
     COALESCE(mrif.reclass_in_cost, 0) AS ty_reclass_in_cost_amt,
     COALESCE(mrif.reclass_in_units, 0) AS ty_reclass_in_qty,
     COALESCE(mrif.reclass_out_cost, 0) AS ty_reclass_out_cost_amt,
     COALESCE(mrif.reclass_out_units, 0) AS ty_reclass_out_qty,
     COALESCE(rtvif.work_plan_rtv_active_cost, 0) AS ty_return_to_vendor_active_cost_amt,
     COALESCE(rtvif.work_plan_rtv_active_units, 0) AS ty_return_to_vendor_active_qty,
     COALESCE(rtvif.work_plan_rtv_inactive_cost, 0) AS ty_return_to_vendor_inactive_cost_amt,
     COALESCE(rtvif.work_plan_rtv_inactive_units, 0) AS ty_return_to_vendor_inactive_qty,
     COALESCE(vfmdt.vendor_funds_cost, 0) AS ty_vendor_funds_cost_amt,
     COALESCE(vfmdt.disc_terms_cost, 0) AS ty_discount_terms_cost_amt,
     COALESCE(mmif.merch_margin_retail, 0) AS ty_merch_margin_retail_amt,
     0 AS ly_beginning_of_period_active_cost_amt,
     0 AS ly_beginning_of_period_active_qty,
     0 AS ly_beginning_of_period_inactive_cost_amt,
     0 AS ly_beginning_of_period_inactive_qty,
     0 AS ly_ending_of_period_active_cost_amt,
     0 AS ly_ending_of_period_active_qty,
     0 AS ly_ending_of_period_inactive_cost_amt,
     0 AS ly_ending_of_period_inactive_qty,
     0 AS ly_transfer_in_active_cost_amt,
     0 AS ly_transfer_in_active_qty,
     0 AS ly_transfer_out_active_cost_amt,
     0 AS ly_transfer_out_active_qty,
     0 AS ly_transfer_in_inactive_cost_amt,
     0 AS ly_transfer_in_inactive_qty,
     0 AS ly_transfer_out_inactive_cost_amt,
     0 AS ly_transfer_out_inactive_qty,
     0 AS ly_transfer_in_other_cost_amt,
     0 AS ly_transfer_in_other_qty,
     0 AS ly_transfer_out_other_cost_amt,
     0 AS ly_transfer_out_other_qty,
     0 AS ly_transfer_in_racking_cost_amt,
     0 AS ly_transfer_in_racking_qty,
     0 AS ly_transfer_out_racking_cost_amt,
     0 AS ly_transfer_out_racking_qty,
     0 AS ly_mark_out_of_stock_active_cost_amt,
     0 AS ly_mark_out_of_stock_active_qty,
     0 AS ly_mark_out_of_stock_inactive_cost_amt,
     0 AS ly_mark_out_of_stock_inactive_qty,
     0 AS ly_receipts_active_cost_amt,
     0 AS ly_receipts_active_qty,
     0 AS ly_receipts_inactive_cost_amt,
     0 AS ly_receipts_inactive_qty,
     0 AS ly_rp_receipts_active_cost_amt,
     0 AS ly_non_rp_receipts_active_cost_amt,
     0 AS ly_reclass_in_cost_amt,
     0 AS ly_reclass_in_qty,
     0 AS ly_reclass_out_cost_amt,
     0 AS ly_reclass_out_qty,
     0 AS ly_return_to_vendor_active_cost_amt,
     0 AS ly_return_to_vendor_active_qty,
     0 AS ly_return_to_vendor_inactive_cost_amt,
     0 AS ly_return_to_vendor_inactive_qty,
     0 AS ly_vendor_funds_cost_amt,
     0 AS ly_discount_terms_cost_amt,
     0 AS ly_merch_margin_retail_amt,
     0 AS lly_beginning_of_period_active_cost_amt,
     0 AS lly_beginning_of_period_active_qty,
     0 AS lly_beginning_of_period_inactive_cost_amt,
     0 AS lly_beginning_of_period_inactive_qty,
     0 AS lly_ending_of_period_active_cost_amt,
     0 AS lly_ending_of_period_active_qty,
     0 AS lly_ending_of_period_inactive_cost_amt,
     0 AS lly_ending_of_period_inactive_qty,
     0 AS lly_transfer_in_active_cost_amt,
     0 AS lly_transfer_in_active_qty,
     0 AS lly_transfer_out_active_cost_amt,
     0 AS lly_transfer_out_active_qty,
     0 AS lly_transfer_in_inactive_cost_amt,
     0 AS lly_transfer_in_inactive_qty,
     0 AS lly_transfer_out_inactive_cost_amt,
     0 AS lly_transfer_out_inactive_qty,
     0 AS lly_transfer_in_other_cost_amt,
     0 AS lly_transfer_in_other_qty,
     0 AS lly_transfer_out_other_cost_amt,
     0 AS lly_transfer_out_other_qty,
     0 AS lly_transfer_in_racking_cost_amt,
     0 AS lly_transfer_in_racking_qty,
     0 AS lly_transfer_out_racking_cost_amt,
     0 AS lly_transfer_out_racking_qty,
     0 AS lly_mark_out_of_stock_active_cost_amt,
     0 AS lly_mark_out_of_stock_active_qty,
     0 AS lly_mark_out_of_stock_inactive_cost_amt,
     0 AS lly_mark_out_of_stock_inactive_qty,
     0 AS lly_receipts_active_cost_amt,
     0 AS lly_receipts_active_qty,
     0 AS lly_receipts_inactive_cost_amt,
     0 AS lly_receipts_inactive_qty,
     0 AS lly_rp_receipts_active_cost_amt,
     0 AS lly_non_rp_receipts_active_cost_amt,
     0 AS lly_reclass_in_cost_amt,
     0 AS lly_reclass_in_qty,
     0 AS lly_reclass_out_cost_amt,
     0 AS lly_reclass_out_qty,
     0 AS lly_return_to_vendor_active_cost_amt,
     0 AS lly_return_to_vendor_active_qty,
     0 AS lly_return_to_vendor_inactive_cost_amt,
     0 AS lly_return_to_vendor_inactive_qty,
     0 AS lly_vendor_funds_cost_amt,
     0 AS lly_discount_terms_cost_amt,
     0 AS lly_merch_margin_retail_amt,
     COALESCE(mmif.shrink_cost, 0) AS ty_shrink_banner_cost_amt,
     COALESCE(mmif.shrink_units, 0) AS ty_shrink_banner_qty,
     COALESCE(mmif.other_inventory_adj_cost, 0) AS ty_other_inventory_adj_cost,
     COALESCE(mmif.merch_margin_charges_cost, 0) AS ty_merch_margin_charges_cost,
     0 AS ly_shrink_banner_cost_amt,
     0 AS ly_shrink_banner_qty,
     0 AS ly_other_inventory_adj_cost,
     0 AS ly_merch_margin_charges_cost,
     0 AS lly_shrink_banner_cost_amt,
     0 AS lly_shrink_banner_qty,
     0 AS lly_other_inventory_adj_cost,
     0 AS lly_merch_margin_charges_cost,
     mtbv.dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_inventory_bop_insight_fact_vw AS iifbv
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.inventory_insight_fact AS iif ON iifbv.current_week_num = iif.week_num AND iifbv.department_num
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
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv ON LOWER(mtbv.interface_code) = LOWER('MFP_BANR_BLEND_WKLY'
       )
    UNION ALL
    SELECT COALESCE(iifbv0.department_num, iif0.department_num, mtlif0.department_num, pif0.department_num, mrif0.department_num
      , rtvif0.department_num, vfmdt0.department_num, mmif0.department_num) AS dept_num,
     wcd.week_num,
     COALESCE(iifbv0.banner_num, iif0.banner_num, mtlif0.banner_num, pif0.banner_num, mrif0.banner_num, rtvif0.banner_num
      , vfmdt0.banner_num, mmif0.banner_num) AS banner_country_num,
     COALESCE(iifbv0.fulfilment_num, iif0.fulfilment_num, mtlif0.fulfilment_num, pif0.fulfilment_num, mrif0.fulfilment_num
      , rtvif0.fulfilment_num, vfmdt0.fulfilment_num, mmif0.fulfilment_num) AS fulfill_type_num,
     0 AS cp_beginning_of_period_active_cost_amt,
     0 AS cp_beginning_of_period_active_inventory_target_qty,
     0 AS cp_beginning_of_period_active_qty,
     0 AS cp_beginning_of_period_inactive_cost_amt,
     0 AS cp_beginning_of_period_inactive_qty,
     0 AS cp_ending_of_period_active_cost_amt,
     0 AS cp_ending_of_period_active_qty,
     0 AS cp_ending_of_period_inactive_cost_amt,
     0 AS cp_ending_of_period_inactive_qty,
     0 AS cp_transfer_in_active_cost_amt,
     0 AS cp_transfer_in_active_qty,
     0 AS cp_transfer_out_active_cost_amt,
     0 AS cp_transfer_out_active_qty,
     0 AS cp_transfer_in_inactive_cost_amt,
     0 AS cp_transfer_in_inactive_qty,
     0 AS cp_transfer_out_inactive_cost_amt,
     0 AS cp_transfer_out_inactive_qty,
     0 AS cp_transfer_in_other_cost_amt,
     0 AS cp_transfer_in_other_qty,
     0 AS cp_transfer_out_other_cost_amt,
     0 AS cp_transfer_out_other_qty,
     0 AS cp_transfer_in_racking_cost_amt,
     0 AS cp_transfer_in_racking_qty,
     0 AS cp_transfer_out_racking_cost_amt,
     0 AS cp_transfer_out_racking_qty,
     0 AS cp_mark_out_of_stock_active_cost_amt,
     0 AS cp_mark_out_of_stock_active_qty,
     0 AS cp_mark_out_of_stock_inactive_cost_amt,
     0 AS cp_mark_out_of_stock_inactive_qty,
     0 AS cp_receipts_active_cost_amt,
     0 AS cp_receipts_active_qty,
     0 AS cp_receipts_inactive_cost_amt,
     0 AS cp_receipts_inactive_qty,
     0 AS cp_receipts_reserve_cost_amt,
     0 AS cp_receipts_reserve_qty,
     0 AS cp_reclass_in_cost_amt,
     0 AS cp_reclass_in_qty,
     0 AS cp_reclass_out_cost_amt,
     0 AS cp_reclass_out_qty,
     0 AS cp_return_to_vendor_active_cost_amt,
     0 AS cp_return_to_vendor_active_qty,
     0 AS cp_return_to_vendor_inactive_cost_amt,
     0 AS cp_return_to_vendor_inactive_qty,
     0 AS cp_vendor_funds_cost_amt,
     0 AS cp_discount_terms_cost_amt,
     0 AS cp_merch_margin_retail_amt,
     0 AS cp_inventory_movement_in_dropship_cost_amt,
     0 AS cp_inventory_movement_in_dropship_qty,
     0 AS cp_inventory_movement_out_dropship_cost_amt,
     0 AS cp_inventory_movement_out_dropship_qty,
     0 AS cp_other_inventory_adjustment_cost_amt,
     0 AS cp_other_inventory_adjustment_qty,
     0 AS cp_open_to_buy_receipts_active_cost_amt,
     0 AS cp_open_to_buy_receipts_inactive_cost_amt,
     0 AS op_beginning_of_period_active_cost_amt,
     0 AS op_beginning_of_period_active_inventory_target_qty,
     0 AS op_beginning_of_period_active_qty,
     0 AS op_beginning_of_period_inactive_cost_amt,
     0 AS op_beginning_of_period_inactive_qty,
     0 AS op_ending_of_period_active_cost_amt,
     0 AS op_ending_of_period_active_qty,
     0 AS op_ending_of_period_inactive_cost_amt,
     0 AS op_ending_of_period_inactive_qty,
     0 AS op_transfer_in_active_cost_amt,
     0 AS op_transfer_in_active_qty,
     0 AS op_transfer_out_active_cost_amt,
     0 AS op_transfer_out_active_qty,
     0 AS op_transfer_in_inactive_cost_amt,
     0 AS op_transfer_in_inactive_qty,
     0 AS op_transfer_out_inactive_cost_amt,
     0 AS op_transfer_out_inactive_qty,
     0 AS op_transfer_in_other_cost_amt,
     0 AS op_transfer_in_other_qty,
     0 AS op_transfer_out_other_cost_amt,
     0 AS op_transfer_out_other_qty,
     0 AS op_transfer_in_racking_cost_amt,
     0 AS op_transfer_in_racking_qty,
     0 AS op_transfer_out_racking_cost_amt,
     0 AS op_transfer_out_racking_qty,
     0 AS op_mark_out_of_stock_active_cost_amt,
     0 AS op_mark_out_of_stock_active_qty,
     0 AS op_mark_out_of_stock_inactive_cost_amt,
     0 AS op_mark_out_of_stock_inactive_qty,
     0 AS op_receipts_active_cost_amt,
     0 AS op_receipts_active_qty,
     0 AS op_receipts_inactive_cost_amt,
     0 AS op_receipts_inactive_qty,
     0 AS op_receipts_reserve_cost_amt,
     0 AS op_receipts_reserve_qty,
     0 AS op_reclass_in_cost_amt,
     0 AS op_reclass_in_qty,
     0 AS op_reclass_out_cost_amt,
     0 AS op_reclass_out_qty,
     0 AS op_return_to_vendor_active_cost_amt,
     0 AS op_return_to_vendor_active_qty,
     0 AS op_return_to_vendor_inactive_cost_amt,
     0 AS op_return_to_vendor_inactive_qty,
     0 AS op_vendor_funds_cost_amt,
     0 AS op_discount_terms_cost_amt,
     0 AS op_merch_margin_retail_amt,
     0 AS op_inventory_movement_in_dropship_cost_amt,
     0 AS op_inventory_movement_in_dropship_qty,
     0 AS op_inventory_movement_out_dropship_cost_amt,
     0 AS op_inventory_movement_out_dropship_qty,
     0 AS op_other_inventory_adjustment_cost_amt,
     0 AS op_other_inventory_adjustment_qty,
     0 AS sp_beginning_of_period_active_cost_amt,
     0 AS sp_beginning_of_period_active_inventory_target_qty,
     0 AS sp_beginning_of_period_active_qty,
     0 AS sp_beginning_of_period_inactive_cost_amt,
     0 AS sp_beginning_of_period_inactive_qty,
     0 AS sp_ending_of_period_active_cost_amt,
     0 AS sp_ending_of_period_active_qty,
     0 AS sp_ending_of_period_inactive_cost_amt,
     0 AS sp_ending_of_period_inactive_qty,
     0 AS sp_transfer_in_active_cost_amt,
     0 AS sp_transfer_in_active_qty,
     0 AS sp_transfer_out_active_cost_amt,
     0 AS sp_transfer_out_active_qty,
     0 AS sp_transfer_in_inactive_cost_amt,
     0 AS sp_transfer_in_inactive_qty,
     0 AS sp_transfer_out_inactive_cost_amt,
     0 AS sp_transfer_out_inactive_qty,
     0 AS sp_transfer_in_other_cost_amt,
     0 AS sp_transfer_in_other_qty,
     0 AS sp_transfer_out_other_cost_amt,
     0 AS sp_transfer_out_other_qty,
     0 AS sp_transfer_in_racking_cost_amt,
     0 AS sp_transfer_in_racking_qty,
     0 AS sp_transfer_out_racking_cost_amt,
     0 AS sp_transfer_out_racking_qty,
     0 AS sp_mark_out_of_stock_active_cost_amt,
     0 AS sp_mark_out_of_stock_active_qty,
     0 AS sp_mark_out_of_stock_inactive_cost_amt,
     0 AS sp_mark_out_of_stock_inactive_qty,
     0 AS sp_receipts_active_cost_amt,
     0 AS sp_receipts_active_qty,
     0 AS sp_receipts_inactive_cost_amt,
     0 AS sp_receipts_inactive_qty,
     0 AS sp_receipts_reserve_cost_amt,
     0 AS sp_receipts_reserve_qty,
     0 AS sp_reclass_in_cost_amt,
     0 AS sp_reclass_in_qty,
     0 AS sp_reclass_out_cost_amt,
     0 AS sp_reclass_out_qty,
     0 AS sp_return_to_vendor_active_cost_amt,
     0 AS sp_return_to_vendor_active_qty,
     0 AS sp_return_to_vendor_inactive_cost_amt,
     0 AS sp_return_to_vendor_inactive_qty,
     0 AS sp_vendor_funds_cost_amt,
     0 AS sp_discount_terms_cost_amt,
     0 AS sp_merch_margin_retail_amt,
     0 AS sp_inventory_movement_in_dropship_cost_amt,
     0 AS sp_inventory_movement_in_dropship_qty,
     0 AS sp_inventory_movement_out_dropship_cost_amt,
     0 AS sp_inventory_movement_out_dropship_qty,
     0 AS sp_other_inventory_adjustment_cost_amt,
     0 AS sp_other_inventory_adjustment_qty,
     0 AS sp_open_to_buy_receipts_active_cost_amt,
     0 AS sp_open_to_buy_receipts_inactive_cost_amt,
     0 AS ty_beginning_of_period_active_cost_amt,
     0 AS ty_beginning_of_period_active_qty,
     0 AS ty_beginning_of_period_inactive_cost_amt,
     0 AS ty_beginning_of_period_inactive_qty,
     0 AS ty_ending_of_period_active_cost_amt,
     0 AS ty_ending_of_period_active_qty,
     0 AS ty_ending_of_period_inactive_cost_amt,
     0 AS ty_ending_of_period_inactive_qty,
     0 AS ty_transfer_in_active_cost_amt,
     0 AS ty_transfer_in_active_qty,
     0 AS ty_transfer_out_active_cost_amt,
     0 AS ty_transfer_out_active_qty,
     0 AS ty_transfer_in_inactive_cost_amt,
     0 AS ty_transfer_in_inactive_qty,
     0 AS ty_transfer_out_inactive_cost_amt,
     0 AS ty_transfer_out_inactive_qty,
     0 AS ty_transfer_in_other_cost_amt,
     0 AS ty_transfer_in_other_qty,
     0 AS ty_transfer_out_other_cost_amt,
     0 AS ty_transfer_out_other_qty,
     0 AS ty_transfer_in_racking_cost_amt,
     0 AS ty_transfer_in_racking_qty,
     0 AS ty_transfer_out_racking_cost_amt,
     0 AS ty_transfer_out_racking_qty,
     0 AS ty_mark_out_of_stock_active_cost_amt,
     0 AS ty_mark_out_of_stock_active_qty,
     0 AS ty_mark_out_of_stock_inactive_cost_amt,
     0 AS ty_mark_out_of_stock_inactive_qty,
     0 AS ty_receipts_active_cost_amt,
     0 AS ty_receipts_active_qty,
     0 AS ty_receipts_inactive_cost_amt,
     0 AS ty_receipts_inactive_qty,
     0 AS ty_rp_receipts_active_cost_amt,
     0 AS ty_non_rp_receipts_active_cost_amt,
     0 AS ty_reclass_in_cost_amt,
     0 AS ty_reclass_in_qty,
     0 AS ty_reclass_out_cost_amt,
     0 AS ty_reclass_out_qty,
     0 AS ty_return_to_vendor_active_cost_amt,
     0 AS ty_return_to_vendor_active_qty,
     0 AS ty_return_to_vendor_inactive_cost_amt,
     0 AS ty_return_to_vendor_inactive_qty,
     0 AS ty_vendor_funds_cost_amt,
     0 AS ty_discount_terms_cost_amt,
     0 AS ty_merch_margin_retail_amt,
     COALESCE(iifbv0.stock_on_hand_active_cost, 0) AS ly_beginning_of_period_active_cost_amt,
     COALESCE(iifbv0.stock_on_hand_active_units, 0) AS ly_beginning_of_period_active_qty,
     COALESCE(iifbv0.stock_on_hand_inactive_cost, 0) AS ly_beginning_of_period_inactive_cost_amt,
     COALESCE(iifbv0.stock_on_hand_inactive_units, 0) AS ly_beginning_of_period_inactive_qty,
     COALESCE(iif0.stock_on_hand_active_cost, 0) AS ly_ending_of_period_active_cost_amt,
     COALESCE(iif0.stock_on_hand_active_units, 0) AS ly_ending_of_period_active_qty,
     COALESCE(iif0.stock_on_hand_inactive_cost, 0) AS ly_ending_of_period_inactive_cost_amt,
     COALESCE(iif0.stock_on_hand_inactive_units, 0) AS ly_ending_of_period_inactive_qty,
     COALESCE(mtlif0.active_transfer_in_c, 0) AS ly_transfer_in_active_cost_amt,
     COALESCE(mtlif0.active_transfer_in_u, 0) AS ly_transfer_in_active_qty,
     COALESCE(mtlif0.active_transfer_out_c, 0) AS ly_transfer_out_active_cost_amt,
     COALESCE(mtlif0.active_transfer_out_u, 0) AS ly_transfer_out_active_qty,
     COALESCE(mtlif0.inactive_transfer_in_c, 0) AS ly_transfer_in_inactive_cost_amt,
     COALESCE(mtlif0.inactive_transfer_in_u, 0) AS ly_transfer_in_inactive_qty,
     COALESCE(mtlif0.inactive_transfer_out_c, 0) AS ly_transfer_out_inactive_cost_amt,
     COALESCE(mtlif0.inactive_transfer_out_u, 0) AS ly_transfer_out_inactive_qty,
     COALESCE(mtlif0.other_transfer_in_c, 0) AS ly_transfer_in_other_cost_amt,
     COALESCE(mtlif0.other_transfer_in_u, 0) AS ly_transfer_in_other_qty,
     COALESCE(mtlif0.other_transfer_out_c, 0) AS ly_transfer_out_other_cost_amt,
     COALESCE(mtlif0.other_transfer_out_u, 0) AS ly_transfer_out_other_qty,
     COALESCE(mtlif0.racking_transfer_in_c, 0) AS ly_transfer_in_racking_cost_amt,
     COALESCE(mtlif0.racking_transfer_in_u, 0) AS ly_transfer_in_racking_qty,
     COALESCE(mtlif0.racking_transfer_out_c, 0) AS ly_transfer_out_racking_cost_amt,
     COALESCE(mtlif0.racking_transfer_out_u, 0) AS ly_transfer_out_racking_qty,
     COALESCE(mmif0.mos_active_cost, 0) AS ly_mark_out_of_stock_active_cost_amt,
     COALESCE(mmif0.mos_active_units, 0) AS ly_mark_out_of_stock_active_qty,
     COALESCE(mmif0.mos_inactive_cost, 0) AS ly_mark_out_of_stock_inactive_cost_amt,
     COALESCE(mmif0.mos_inactive_units, 0) AS ly_mark_out_of_stock_inactive_qty,
     COALESCE(pif0.receipts_active_cost, 0) AS ly_receipts_active_cost_amt,
     COALESCE(pif0.receipts_active_units, 0) AS ly_receipts_active_qty,
     COALESCE(pif0.receipts_inactive_cost, 0) AS ly_receipts_inactive_cost_amt,
     COALESCE(pif0.receipts_inactive_units, 0) AS ly_receipts_inactive_qty,
     COALESCE(pif0.rp_receipts_active_cost, 0) AS ly_rp_receipts_active_cost_amt,
     COALESCE(pif0.non_rp_receipts_active_cost, 0) AS ly_non_rp_receipts_active_cost_amt,
     COALESCE(mrif0.reclass_in_cost, 0) AS ly_reclass_in_cost_amt,
     COALESCE(mrif0.reclass_in_units, 0) AS ly_reclass_in_qty,
     COALESCE(mrif0.reclass_out_cost, 0) AS ly_reclass_out_cost_amt,
     COALESCE(mrif0.reclass_out_units, 0) AS ly_reclass_out_qty,
     COALESCE(rtvif0.work_plan_rtv_active_cost, 0) AS ly_return_to_vendor_active_cost_amt,
     COALESCE(rtvif0.work_plan_rtv_active_units, 0) AS ly_return_to_vendor_active_qty,
     COALESCE(rtvif0.work_plan_rtv_inactive_cost, 0) AS ly_return_to_vendor_inactive_cost_amt,
     COALESCE(rtvif0.work_plan_rtv_inactive_units, 0) AS ly_return_to_vendor_inactive_qty,
     COALESCE(vfmdt0.vendor_funds_cost, 0) AS ly_vendor_funds_cost_amt,
     COALESCE(vfmdt0.disc_terms_cost, 0) AS ly_discount_terms_cost_amt,
     COALESCE(mmif0.merch_margin_retail, 0) AS ly_merch_margin_retail_amt,
     0 AS lly_beginning_of_period_active_cost_amt,
     0 AS lly_beginning_of_period_active_qty,
     0 AS lly_beginning_of_period_inactive_cost_amt,
     0 AS lly_beginning_of_period_inactive_qty,
     0 AS lly_ending_of_period_active_cost_amt,
     0 AS lly_ending_of_period_active_qty,
     0 AS lly_ending_of_period_inactive_cost_amt,
     0 AS lly_ending_of_period_inactive_qty,
     0 AS lly_transfer_in_active_cost_amt,
     0 AS lly_transfer_in_active_qty,
     0 AS lly_transfer_out_active_cost_amt,
     0 AS lly_transfer_out_active_qty,
     0 AS lly_transfer_in_inactive_cost_amt,
     0 AS lly_transfer_in_inactive_qty,
     0 AS lly_transfer_out_inactive_cost_amt,
     0 AS lly_transfer_out_inactive_qty,
     0 AS lly_transfer_in_other_cost_amt,
     0 AS lly_transfer_in_other_qty,
     0 AS lly_transfer_out_other_cost_amt,
     0 AS lly_transfer_out_other_qty,
     0 AS lly_transfer_in_racking_cost_amt,
     0 AS lly_transfer_in_racking_qty,
     0 AS lly_transfer_out_racking_cost_amt,
     0 AS lly_transfer_out_racking_qty,
     0 AS lly_mark_out_of_stock_active_cost_amt,
     0 AS lly_mark_out_of_stock_active_qty,
     0 AS lly_mark_out_of_stock_inactive_cost_amt,
     0 AS lly_mark_out_of_stock_inactive_qty,
     0 AS lly_receipts_active_cost_amt,
     0 AS lly_receipts_active_qty,
     0 AS lly_receipts_inactive_cost_amt,
     0 AS lly_receipts_inactive_qty,
     0 AS lly_rp_receipts_active_cost_amt,
     0 AS lly_non_rp_receipts_active_cost_amt,
     0 AS lly_reclass_in_cost_amt,
     0 AS lly_reclass_in_qty,
     0 AS lly_reclass_out_cost_amt,
     0 AS lly_reclass_out_qty,
     0 AS lly_return_to_vendor_active_cost_amt,
     0 AS lly_return_to_vendor_active_qty,
     0 AS lly_return_to_vendor_inactive_cost_amt,
     0 AS lly_return_to_vendor_inactive_qty,
     0 AS lly_vendor_funds_cost_amt,
     0 AS lly_discount_terms_cost_amt,
     0 AS lly_merch_margin_retail_amt,
     0 AS ty_shrink_banner_cost_amt,
     0 AS ty_shrink_banner_qty,
     0 AS ty_other_inventory_adj_cost,
     0 AS ty_merch_margin_charges_cost,
     COALESCE(mmif0.shrink_cost, 0) AS ly_shrink_banner_cost_amt,
     COALESCE(mmif0.shrink_units, 0) AS ly_shrink_banner_qty,
     COALESCE(mmif0.other_inventory_adj_cost, 0) AS ly_other_inventory_adj_cost,
     COALESCE(mmif0.merch_margin_charges_cost, 0) AS ly_merch_margin_charges_cost,
     0 AS lly_shrink_banner_cost_amt,
     0 AS lly_shrink_banner_qty,
     0 AS lly_other_inventory_adj_cost,
     0 AS lly_merch_margin_charges_cost,
     mtbv0.dw_batch_dt
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
       
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd ON wcd.ly_week_num <> 444444 AND (iifbv0.current_week_num = wcd.ly_week_num
                OR mmif0.week_num = wcd.ly_week_num OR vfmdt0.week_num = wcd.ly_week_num OR rtvif0.week_num = wcd.ly_week_num
              OR mrif0.week_num = wcd.ly_week_num OR pif0.week_num = wcd.ly_week_num OR mtlif0.week_num = wcd.ly_week_num
           OR iif0.week_num = wcd.ly_week_num)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv0 ON LOWER(mtbv0.interface_code) = LOWER('MFP_BANR_BLEND_WKLY'
       )
    UNION ALL
    SELECT COALESCE(iifbv1.department_num, iif1.department_num, mtlif1.department_num, pif1.department_num, mrif1.department_num
      , rtvif1.department_num, vfmdt1.department_num, mmif1.department_num) AS dept_num,
     wcd0.week_num,
     COALESCE(iifbv1.banner_num, iif1.banner_num, mtlif1.banner_num, pif1.banner_num, mrif1.banner_num, rtvif1.banner_num
      , vfmdt1.banner_num, mmif1.banner_num) AS banner_country_num,
     COALESCE(iifbv1.fulfilment_num, iif1.fulfilment_num, mtlif1.fulfilment_num, pif1.fulfilment_num, mrif1.fulfilment_num
      , rtvif1.fulfilment_num, vfmdt1.fulfilment_num, mmif1.fulfilment_num) AS fulfill_type_num,
     0 AS cp_beginning_of_period_active_cost_amt,
     0 AS cp_beginning_of_period_active_inventory_target_qty,
     0 AS cp_beginning_of_period_active_qty,
     0 AS cp_beginning_of_period_inactive_cost_amt,
     0 AS cp_beginning_of_period_inactive_qty,
     0 AS cp_ending_of_period_active_cost_amt,
     0 AS cp_ending_of_period_active_qty,
     0 AS cp_ending_of_period_inactive_cost_amt,
     0 AS cp_ending_of_period_inactive_qty,
     0 AS cp_transfer_in_active_cost_amt,
     0 AS cp_transfer_in_active_qty,
     0 AS cp_transfer_out_active_cost_amt,
     0 AS cp_transfer_out_active_qty,
     0 AS cp_transfer_in_inactive_cost_amt,
     0 AS cp_transfer_in_inactive_qty,
     0 AS cp_transfer_out_inactive_cost_amt,
     0 AS cp_transfer_out_inactive_qty,
     0 AS cp_transfer_in_other_cost_amt,
     0 AS cp_transfer_in_other_qty,
     0 AS cp_transfer_out_other_cost_amt,
     0 AS cp_transfer_out_other_qty,
     0 AS cp_transfer_in_racking_cost_amt,
     0 AS cp_transfer_in_racking_qty,
     0 AS cp_transfer_out_racking_cost_amt,
     0 AS cp_transfer_out_racking_qty,
     0 AS cp_mark_out_of_stock_active_cost_amt,
     0 AS cp_mark_out_of_stock_active_qty,
     0 AS cp_mark_out_of_stock_inactive_cost_amt,
     0 AS cp_mark_out_of_stock_inactive_qty,
     0 AS cp_receipts_active_cost_amt,
     0 AS cp_receipts_active_qty,
     0 AS cp_receipts_inactive_cost_amt,
     0 AS cp_receipts_inactive_qty,
     0 AS cp_receipts_reserve_cost_amt,
     0 AS cp_receipts_reserve_qty,
     0 AS cp_reclass_in_cost_amt,
     0 AS cp_reclass_in_qty,
     0 AS cp_reclass_out_cost_amt,
     0 AS cp_reclass_out_qty,
     0 AS cp_return_to_vendor_active_cost_amt,
     0 AS cp_return_to_vendor_active_qty,
     0 AS cp_return_to_vendor_inactive_cost_amt,
     0 AS cp_return_to_vendor_inactive_qty,
     0 AS cp_vendor_funds_cost_amt,
     0 AS cp_discount_terms_cost_amt,
     0 AS cp_merch_margin_retail_amt,
     0 AS cp_inventory_movement_in_dropship_cost_amt,
     0 AS cp_inventory_movement_in_dropship_qty,
     0 AS cp_inventory_movement_out_dropship_cost_amt,
     0 AS cp_inventory_movement_out_dropship_qty,
     0 AS cp_other_inventory_adjustment_cost_amt,
     0 AS cp_other_inventory_adjustment_qty,
     0 AS cp_open_to_buy_receipts_active_cost_amt,
     0 AS cp_open_to_buy_receipts_inactive_cost_amt,
     0 AS op_beginning_of_period_active_cost_amt,
     0 AS op_beginning_of_period_active_inventory_target_qty,
     0 AS op_beginning_of_period_active_qty,
     0 AS op_beginning_of_period_inactive_cost_amt,
     0 AS op_beginning_of_period_inactive_qty,
     0 AS op_ending_of_period_active_cost_amt,
     0 AS op_ending_of_period_active_qty,
     0 AS op_ending_of_period_inactive_cost_amt,
     0 AS op_ending_of_period_inactive_qty,
     0 AS op_transfer_in_active_cost_amt,
     0 AS op_transfer_in_active_qty,
     0 AS op_transfer_out_active_cost_amt,
     0 AS op_transfer_out_active_qty,
     0 AS op_transfer_in_inactive_cost_amt,
     0 AS op_transfer_in_inactive_qty,
     0 AS op_transfer_out_inactive_cost_amt,
     0 AS op_transfer_out_inactive_qty,
     0 AS op_transfer_in_other_cost_amt,
     0 AS op_transfer_in_other_qty,
     0 AS op_transfer_out_other_cost_amt,
     0 AS op_transfer_out_other_qty,
     0 AS op_transfer_in_racking_cost_amt,
     0 AS op_transfer_in_racking_qty,
     0 AS op_transfer_out_racking_cost_amt,
     0 AS op_transfer_out_racking_qty,
     0 AS op_mark_out_of_stock_active_cost_amt,
     0 AS op_mark_out_of_stock_active_qty,
     0 AS op_mark_out_of_stock_inactive_cost_amt,
     0 AS op_mark_out_of_stock_inactive_qty,
     0 AS op_receipts_active_cost_amt,
     0 AS op_receipts_active_qty,
     0 AS op_receipts_inactive_cost_amt,
     0 AS op_receipts_inactive_qty,
     0 AS op_receipts_reserve_cost_amt,
     0 AS op_receipts_reserve_qty,
     0 AS op_reclass_in_cost_amt,
     0 AS op_reclass_in_qty,
     0 AS op_reclass_out_cost_amt,
     0 AS op_reclass_out_qty,
     0 AS op_return_to_vendor_active_cost_amt,
     0 AS op_return_to_vendor_active_qty,
     0 AS op_return_to_vendor_inactive_cost_amt,
     0 AS op_return_to_vendor_inactive_qty,
     0 AS op_vendor_funds_cost_amt,
     0 AS op_discount_terms_cost_amt,
     0 AS op_merch_margin_retail_amt,
     0 AS op_inventory_movement_in_dropship_cost_amt,
     0 AS op_inventory_movement_in_dropship_qty,
     0 AS op_inventory_movement_out_dropship_cost_amt,
     0 AS op_inventory_movement_out_dropship_qty,
     0 AS op_other_inventory_adjustment_cost_amt,
     0 AS op_other_inventory_adjustment_qty,
     0 AS sp_beginning_of_period_active_cost_amt,
     0 AS sp_beginning_of_period_active_inventory_target_qty,
     0 AS sp_beginning_of_period_active_qty,
     0 AS sp_beginning_of_period_inactive_cost_amt,
     0 AS sp_beginning_of_period_inactive_qty,
     0 AS sp_ending_of_period_active_cost_amt,
     0 AS sp_ending_of_period_active_qty,
     0 AS sp_ending_of_period_inactive_cost_amt,
     0 AS sp_ending_of_period_inactive_qty,
     0 AS sp_transfer_in_active_cost_amt,
     0 AS sp_transfer_in_active_qty,
     0 AS sp_transfer_out_active_cost_amt,
     0 AS sp_transfer_out_active_qty,
     0 AS sp_transfer_in_inactive_cost_amt,
     0 AS sp_transfer_in_inactive_qty,
     0 AS sp_transfer_out_inactive_cost_amt,
     0 AS sp_transfer_out_inactive_qty,
     0 AS sp_transfer_in_other_cost_amt,
     0 AS sp_transfer_in_other_qty,
     0 AS sp_transfer_out_other_cost_amt,
     0 AS sp_transfer_out_other_qty,
     0 AS sp_transfer_in_racking_cost_amt,
     0 AS sp_transfer_in_racking_qty,
     0 AS sp_transfer_out_racking_cost_amt,
     0 AS sp_transfer_out_racking_qty,
     0 AS sp_mark_out_of_stock_active_cost_amt,
     0 AS sp_mark_out_of_stock_active_qty,
     0 AS sp_mark_out_of_stock_inactive_cost_amt,
     0 AS sp_mark_out_of_stock_inactive_qty,
     0 AS sp_receipts_active_cost_amt,
     0 AS sp_receipts_active_qty,
     0 AS sp_receipts_inactive_cost_amt,
     0 AS sp_receipts_inactive_qty,
     0 AS sp_receipts_reserve_cost_amt,
     0 AS sp_receipts_reserve_qty,
     0 AS sp_reclass_in_cost_amt,
     0 AS sp_reclass_in_qty,
     0 AS sp_reclass_out_cost_amt,
     0 AS sp_reclass_out_qty,
     0 AS sp_return_to_vendor_active_cost_amt,
     0 AS sp_return_to_vendor_active_qty,
     0 AS sp_return_to_vendor_inactive_cost_amt,
     0 AS sp_return_to_vendor_inactive_qty,
     0 AS sp_vendor_funds_cost_amt,
     0 AS sp_discount_terms_cost_amt,
     0 AS sp_merch_margin_retail_amt,
     0 AS sp_inventory_movement_in_dropship_cost_amt,
     0 AS sp_inventory_movement_in_dropship_qty,
     0 AS sp_inventory_movement_out_dropship_cost_amt,
     0 AS sp_inventory_movement_out_dropship_qty,
     0 AS sp_other_inventory_adjustment_cost_amt,
     0 AS sp_other_inventory_adjustment_qty,
     0 AS sp_open_to_buy_receipts_active_cost_amt,
     0 AS sp_open_to_buy_receipts_inactive_cost_amt,
     0 AS ty_beginning_of_period_active_cost_amt,
     0 AS ty_beginning_of_period_active_qty,
     0 AS ty_beginning_of_period_inactive_cost_amt,
     0 AS ty_beginning_of_period_inactive_qty,
     0 AS ty_ending_of_period_active_cost_amt,
     0 AS ty_ending_of_period_active_qty,
     0 AS ty_ending_of_period_inactive_cost_amt,
     0 AS ty_ending_of_period_inactive_qty,
     0 AS ty_transfer_in_active_cost_amt,
     0 AS ty_transfer_in_active_qty,
     0 AS ty_transfer_out_active_cost_amt,
     0 AS ty_transfer_out_active_qty,
     0 AS ty_transfer_in_inactive_cost_amt,
     0 AS ty_transfer_in_inactive_qty,
     0 AS ty_transfer_out_inactive_cost_amt,
     0 AS ty_transfer_out_inactive_qty,
     0 AS ty_transfer_in_other_cost_amt,
     0 AS ty_transfer_in_other_qty,
     0 AS ty_transfer_out_other_cost_amt,
     0 AS ty_transfer_out_other_qty,
     0 AS ty_transfer_in_racking_cost_amt,
     0 AS ty_transfer_in_racking_qty,
     0 AS ty_transfer_out_racking_cost_amt,
     0 AS ty_transfer_out_racking_qty,
     0 AS ty_mark_out_of_stock_active_cost_amt,
     0 AS ty_mark_out_of_stock_active_qty,
     0 AS ty_mark_out_of_stock_inactive_cost_amt,
     0 AS ty_mark_out_of_stock_inactive_qty,
     0 AS ty_receipts_active_cost_amt,
     0 AS ty_receipts_active_qty,
     0 AS ty_receipts_inactive_cost_amt,
     0 AS ty_receipts_inactive_qty,
     0 AS ty_rp_receipts_active_cost_amt,
     0 AS ty_non_rp_receipts_active_cost_amt,
     0 AS ty_reclass_in_cost_amt,
     0 AS ty_reclass_in_qty,
     0 AS ty_reclass_out_cost_amt,
     0 AS ty_reclass_out_qty,
     0 AS ty_return_to_vendor_active_cost_amt,
     0 AS ty_return_to_vendor_active_qty,
     0 AS ty_return_to_vendor_inactive_cost_amt,
     0 AS ty_return_to_vendor_inactive_qty,
     0 AS ty_vendor_funds_cost_amt,
     0 AS ty_discount_terms_cost_amt,
     0 AS ty_merch_margin_retail_amt,
     0 AS ly_beginning_of_period_active_cost_amt,
     0 AS ly_beginning_of_period_active_qty,
     0 AS ly_beginning_of_period_inactive_cost_amt,
     0 AS ly_beginning_of_period_inactive_qty,
     0 AS ly_ending_of_period_active_cost_amt,
     0 AS ly_ending_of_period_active_qty,
     0 AS ly_ending_of_period_inactive_cost_amt,
     0 AS ly_ending_of_period_inactive_qty,
     0 AS ly_transfer_in_active_cost_amt,
     0 AS ly_transfer_in_active_qty,
     0 AS ly_transfer_out_active_cost_amt,
     0 AS ly_transfer_out_active_qty,
     0 AS ly_transfer_in_inactive_cost_amt,
     0 AS ly_transfer_in_inactive_qty,
     0 AS ly_transfer_out_inactive_cost_amt,
     0 AS ly_transfer_out_inactive_qty,
     0 AS ly_transfer_in_other_cost_amt,
     0 AS ly_transfer_in_other_qty,
     0 AS ly_transfer_out_other_cost_amt,
     0 AS ly_transfer_out_other_qty,
     0 AS ly_transfer_in_racking_cost_amt,
     0 AS ly_transfer_in_racking_qty,
     0 AS ly_transfer_out_racking_cost_amt,
     0 AS ly_transfer_out_racking_qty,
     0 AS ly_mark_out_of_stock_active_cost_amt,
     0 AS ly_mark_out_of_stock_active_qty,
     0 AS ly_mark_out_of_stock_inactive_cost_amt,
     0 AS ly_mark_out_of_stock_inactive_qty,
     0 AS ly_receipts_active_cost_amt,
     0 AS ly_receipts_active_qty,
     0 AS ly_receipts_inactive_cost_amt,
     0 AS ly_receipts_inactive_qty,
     0 AS ly_rp_receipts_active_cost_amt,
     0 AS ly_non_rp_receipts_active_cost_amt,
     0 AS ly_reclass_in_cost_amt,
     0 AS ly_reclass_in_qty,
     0 AS ly_reclass_out_cost_amt,
     0 AS ly_reclass_out_qty,
     0 AS ly_return_to_vendor_active_cost_amt,
     0 AS ly_return_to_vendor_active_qty,
     0 AS ly_return_to_vendor_inactive_cost_amt,
     0 AS ly_return_to_vendor_inactive_qty,
     0 AS ly_vendor_funds_cost_amt,
     0 AS ly_discount_terms_cost_amt,
     0 AS ly_merch_margin_retail_amt,
     COALESCE(iifbv1.stock_on_hand_active_cost, 0) AS lly_beginning_of_period_active_cost_amt,
     COALESCE(iifbv1.stock_on_hand_active_units, 0) AS lly_beginning_of_period_active_qty,
     COALESCE(iifbv1.stock_on_hand_inactive_cost, 0) AS lly_beginning_of_period_inactive_cost_amt,
     COALESCE(iifbv1.stock_on_hand_inactive_units, 0) AS lly_beginning_of_period_inactive_qty,
     COALESCE(iif1.stock_on_hand_active_cost, 0) AS lly_ending_of_period_active_cost_amt,
     COALESCE(iif1.stock_on_hand_active_units, 0) AS lly_ending_of_period_active_qty,
     COALESCE(iif1.stock_on_hand_inactive_cost, 0) AS lly_ending_of_period_inactive_cost_amt,
     COALESCE(iif1.stock_on_hand_inactive_units, 0) AS lly_ending_of_period_inactive_qty,
     COALESCE(mtlif1.active_transfer_in_c, 0) AS lly_transfer_in_active_cost_amt,
     COALESCE(mtlif1.active_transfer_in_u, 0) AS lly_transfer_in_active_qty,
     COALESCE(mtlif1.active_transfer_out_c, 0) AS lly_transfer_out_active_cost_amt,
     COALESCE(mtlif1.active_transfer_out_u, 0) AS lly_transfer_out_active_qty,
     COALESCE(mtlif1.inactive_transfer_in_c, 0) AS lly_transfer_in_inactive_cost_amt,
     COALESCE(mtlif1.inactive_transfer_in_u, 0) AS lly_transfer_in_inactive_qty,
     COALESCE(mtlif1.inactive_transfer_out_c, 0) AS lly_transfer_out_inactive_cost_amt,
     COALESCE(mtlif1.inactive_transfer_out_u, 0) AS lly_transfer_out_inactive_qty,
     COALESCE(mtlif1.other_transfer_in_c, 0) AS lly_transfer_in_other_cost_amt,
     COALESCE(mtlif1.other_transfer_in_u, 0) AS lly_transfer_in_other_qty,
     COALESCE(mtlif1.other_transfer_out_c, 0) AS lly_transfer_out_other_cost_amt,
     COALESCE(mtlif1.other_transfer_out_u, 0) AS lly_transfer_out_other_qty,
     COALESCE(mtlif1.racking_transfer_in_c, 0) AS lly_transfer_in_racking_cost_amt,
     COALESCE(mtlif1.racking_transfer_in_u, 0) AS lly_transfer_in_racking_qty,
     COALESCE(mtlif1.racking_transfer_out_c, 0) AS lly_transfer_out_racking_cost_amt,
     COALESCE(mtlif1.racking_transfer_out_u, 0) AS lly_transfer_out_racking_qty,
     COALESCE(mmif1.mos_active_cost, 0) AS lly_mark_out_of_stock_active_cost_amt,
     COALESCE(mmif1.mos_active_units, 0) AS lly_mark_out_of_stock_active_qty,
     COALESCE(mmif1.mos_inactive_cost, 0) AS lly_mark_out_of_stock_inactive_cost_amt,
     COALESCE(mmif1.mos_inactive_units, 0) AS lly_mark_out_of_stock_inactive_qty,
     COALESCE(pif1.receipts_active_cost, 0) AS lly_receipts_active_cost_amt,
     COALESCE(pif1.receipts_active_units, 0) AS lly_receipts_active_qty,
     COALESCE(pif1.receipts_inactive_cost, 0) AS lly_receipts_inactive_cost_amt,
     COALESCE(pif1.receipts_inactive_units, 0) AS lly_receipts_inactive_qty,
     COALESCE(pif1.rp_receipts_active_cost, 0) AS lly_rp_receipts_active_cost_amt,
     COALESCE(pif1.non_rp_receipts_active_cost, 0) AS lly_non_rp_receipts_active_cost_amt,
     COALESCE(mrif1.reclass_in_cost, 0) AS lly_reclass_in_cost_amt,
     COALESCE(mrif1.reclass_in_units, 0) AS lly_reclass_in_qty,
     COALESCE(mrif1.reclass_out_cost, 0) AS lly_reclass_out_cost_amt,
     COALESCE(mrif1.reclass_out_units, 0) AS lly_reclass_out_qty,
     COALESCE(rtvif1.work_plan_rtv_active_cost, 0) AS lly_return_to_vendor_active_cost_amt,
     COALESCE(rtvif1.work_plan_rtv_active_units, 0) AS lly_return_to_vendor_active_qty,
     COALESCE(rtvif1.work_plan_rtv_inactive_cost, 0) AS lly_return_to_vendor_inactive_cost_amt,
     COALESCE(rtvif1.work_plan_rtv_inactive_units, 0) AS lly_return_to_vendor_inactive_qty,
     COALESCE(vfmdt1.vendor_funds_cost, 0) AS lly_vendor_funds_cost_amt,
     COALESCE(vfmdt1.disc_terms_cost, 0) AS lly_discount_terms_cost_amt,
     COALESCE(mmif1.merch_margin_retail, 0) AS lly_merch_margin_retail_amt,
     0 AS ty_shrink_banner_cost_amt,
     0 AS ty_shrink_banner_qty,
     0 AS ty_other_inventory_adj_cost,
     0 AS ty_merch_margin_charges_cost,
     0 AS ly_shrink_banner_cost_amt,
     0 AS ly_shrink_banner_qty,
     0 AS ly_other_inventory_adj_cost,
     0 AS ly_merch_margin_charges_cost,
     COALESCE(mmif1.shrink_cost, 0) AS lly_shrink_banner_cost_amt,
     COALESCE(mmif1.shrink_units, 0) AS lly_shrink_banner_qty,
     COALESCE(mmif1.other_inventory_adj_cost, 0) AS lly_other_inventory_adj_cost,
     COALESCE(mmif1.merch_margin_charges_cost, 0) AS lly_merch_margin_charges_cost,
     mtbv1.dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_inventory_bop_insight_fact_vw AS iifbv1
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.inventory_insight_fact AS iif1 ON iifbv1.current_week_num = iif1.week_num AND iifbv1.department_num
          = iif1.department_num AND iifbv1.banner_num = iif1.banner_num AND iifbv1.fulfilment_num = iif1.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_transfer_ledger_insight_fact AS mtlif1 ON iifbv1.current_week_num = mtlif1.week_num AND
         iifbv1.department_num = mtlif1.department_num AND iifbv1.banner_num = mtlif1.banner_num AND iifbv1.fulfilment_num
        = mtlif1.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.poreceipts_insight_fact AS pif1 ON iifbv1.current_week_num = pif1.week_num AND iifbv1.department_num
          = pif1.department_num AND iifbv1.banner_num = pif1.banner_num AND iifbv1.fulfilment_num = pif1.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_reclass_insight_fact AS mrif1 ON iifbv1.current_week_num = mrif1.week_num AND iifbv1.department_num
          = mrif1.department_num AND iifbv1.banner_num = mrif1.banner_num AND iifbv1.fulfilment_num = mrif1.fulfilment_num
       
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.return_to_vendor_insight_fact AS rtvif1 ON iifbv1.current_week_num = rtvif1.week_num AND
         iifbv1.department_num = rtvif1.department_num AND iifbv1.banner_num = rtvif1.banner_num AND iifbv1.fulfilment_num
        = rtvif1.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.vfm_discterms_ledger_insight_fact AS vfmdt1 ON iifbv1.current_week_num = vfmdt1.week_num AND
         iifbv1.department_num = vfmdt1.department_num AND iifbv1.banner_num = vfmdt1.banner_num AND iifbv1.fulfilment_num
        = vfmdt1.fulfilment_num
     FULL JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_margin_insight_fact AS mmif1 ON iifbv1.current_week_num = mmif1.week_num AND iifbv1.department_num
          = mmif1.department_num AND iifbv1.banner_num = mmif1.banner_num AND iifbv1.fulfilment_num = mmif1.fulfilment_num
       
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd0 ON wcd0.lly_week_num <> 444444 AND (iifbv1.current_week_num = wcd0
               .lly_week_num OR mmif1.week_num = wcd0.lly_week_num OR vfmdt1.week_num = wcd0.lly_week_num OR rtvif1.week_num
              = wcd0.lly_week_num OR mrif1.week_num = wcd0.lly_week_num OR pif1.week_num = wcd0.lly_week_num OR mtlif1.week_num
           = wcd0.lly_week_num OR iif1.week_num = wcd0.lly_week_num)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv1 ON LOWER(mtbv1.interface_code) = LOWER('MFP_BANR_BLEND_WKLY'
       )
    UNION ALL
    SELECT iifbv.dept_num,
     wcd1.week_num,
     iifbv.banner_country_num,
     iifbv.fulfill_type_num,
     0 AS cp_beginning_of_period_active_cost_amt,
     0 AS cp_beginning_of_period_active_inventory_target_qty,
     0 AS cp_beginning_of_period_active_qty,
     0 AS cp_beginning_of_period_inactive_cost_amt,
     0 AS cp_beginning_of_period_inactive_qty,
     0 AS cp_ending_of_period_active_cost_amt,
     0 AS cp_ending_of_period_active_qty,
     0 AS cp_ending_of_period_inactive_cost_amt,
     0 AS cp_ending_of_period_inactive_qty,
     0 AS cp_transfer_in_active_cost_amt,
     0 AS cp_transfer_in_active_qty,
     0 AS cp_transfer_out_active_cost_amt,
     0 AS cp_transfer_out_active_qty,
     0 AS cp_transfer_in_inactive_cost_amt,
     0 AS cp_transfer_in_inactive_qty,
     0 AS cp_transfer_out_inactive_cost_amt,
     0 AS cp_transfer_out_inactive_qty,
     0 AS cp_transfer_in_other_cost_amt,
     0 AS cp_transfer_in_other_qty,
     0 AS cp_transfer_out_other_cost_amt,
     0 AS cp_transfer_out_other_qty,
     0 AS cp_transfer_in_racking_cost_amt,
     0 AS cp_transfer_in_racking_qty,
     0 AS cp_transfer_out_racking_cost_amt,
     0 AS cp_transfer_out_racking_qty,
     0 AS cp_mark_out_of_stock_active_cost_amt,
     0 AS cp_mark_out_of_stock_active_qty,
     0 AS cp_mark_out_of_stock_inactive_cost_amt,
     0 AS cp_mark_out_of_stock_inactive_qty,
     0 AS cp_receipts_active_cost_amt,
     0 AS cp_receipts_active_qty,
     0 AS cp_receipts_inactive_cost_amt,
     0 AS cp_receipts_inactive_qty,
     0 AS cp_receipts_reserve_cost_amt,
     0 AS cp_receipts_reserve_qty,
     0 AS cp_reclass_in_cost_amt,
     0 AS cp_reclass_in_qty,
     0 AS cp_reclass_out_cost_amt,
     0 AS cp_reclass_out_qty,
     0 AS cp_return_to_vendor_active_cost_amt,
     0 AS cp_return_to_vendor_active_qty,
     0 AS cp_return_to_vendor_inactive_cost_amt,
     0 AS cp_return_to_vendor_inactive_qty,
     0 AS cp_vendor_funds_cost_amt,
     0 AS cp_discount_terms_cost_amt,
     0 AS cp_merch_margin_retail_amt,
     0 AS cp_inventory_movement_in_dropship_cost_amt,
     0 AS cp_inventory_movement_in_dropship_qty,
     0 AS cp_inventory_movement_out_dropship_cost_amt,
     0 AS cp_inventory_movement_out_dropship_qty,
     0 AS cp_other_inventory_adjustment_cost_amt,
     0 AS cp_other_inventory_adjustment_qty,
     0 AS cp_open_to_buy_receipts_active_cost_amt,
     0 AS cp_open_to_buy_receipts_inactive_cost_amt,
     0 AS op_beginning_of_period_active_cost_amt,
     0 AS op_beginning_of_period_active_inventory_target_qty,
     0 AS op_beginning_of_period_active_qty,
     0 AS op_beginning_of_period_inactive_cost_amt,
     0 AS op_beginning_of_period_inactive_qty,
     0 AS op_ending_of_period_active_cost_amt,
     0 AS op_ending_of_period_active_qty,
     0 AS op_ending_of_period_inactive_cost_amt,
     0 AS op_ending_of_period_inactive_qty,
     0 AS op_transfer_in_active_cost_amt,
     0 AS op_transfer_in_active_qty,
     0 AS op_transfer_out_active_cost_amt,
     0 AS op_transfer_out_active_qty,
     0 AS op_transfer_in_inactive_cost_amt,
     0 AS op_transfer_in_inactive_qty,
     0 AS op_transfer_out_inactive_cost_amt,
     0 AS op_transfer_out_inactive_qty,
     0 AS op_transfer_in_other_cost_amt,
     0 AS op_transfer_in_other_qty,
     0 AS op_transfer_out_other_cost_amt,
     0 AS op_transfer_out_other_qty,
     0 AS op_transfer_in_racking_cost_amt,
     0 AS op_transfer_in_racking_qty,
     0 AS op_transfer_out_racking_cost_amt,
     0 AS op_transfer_out_racking_qty,
     0 AS op_mark_out_of_stock_active_cost_amt,
     0 AS op_mark_out_of_stock_active_qty,
     0 AS op_mark_out_of_stock_inactive_cost_amt,
     0 AS op_mark_out_of_stock_inactive_qty,
     0 AS op_receipts_active_cost_amt,
     0 AS op_receipts_active_qty,
     0 AS op_receipts_inactive_cost_amt,
     0 AS op_receipts_inactive_qty,
     0 AS op_receipts_reserve_cost_amt,
     0 AS op_receipts_reserve_qty,
     0 AS op_reclass_in_cost_amt,
     0 AS op_reclass_in_qty,
     0 AS op_reclass_out_cost_amt,
     0 AS op_reclass_out_qty,
     0 AS op_return_to_vendor_active_cost_amt,
     0 AS op_return_to_vendor_active_qty,
     0 AS op_return_to_vendor_inactive_cost_amt,
     0 AS op_return_to_vendor_inactive_qty,
     0 AS op_vendor_funds_cost_amt,
     0 AS op_discount_terms_cost_amt,
     0 AS op_merch_margin_retail_amt,
     0 AS op_inventory_movement_in_dropship_cost_amt,
     0 AS op_inventory_movement_in_dropship_qty,
     0 AS op_inventory_movement_out_dropship_cost_amt,
     0 AS op_inventory_movement_out_dropship_qty,
     0 AS op_other_inventory_adjustment_cost_amt,
     0 AS op_other_inventory_adjustment_qty,
     0 AS sp_beginning_of_period_active_cost_amt,
     0 AS sp_beginning_of_period_active_inventory_target_qty,
     0 AS sp_beginning_of_period_active_qty,
     0 AS sp_beginning_of_period_inactive_cost_amt,
     0 AS sp_beginning_of_period_inactive_qty,
     0 AS sp_ending_of_period_active_cost_amt,
     0 AS sp_ending_of_period_active_qty,
     0 AS sp_ending_of_period_inactive_cost_amt,
     0 AS sp_ending_of_period_inactive_qty,
     0 AS sp_transfer_in_active_cost_amt,
     0 AS sp_transfer_in_active_qty,
     0 AS sp_transfer_out_active_cost_amt,
     0 AS sp_transfer_out_active_qty,
     0 AS sp_transfer_in_inactive_cost_amt,
     0 AS sp_transfer_in_inactive_qty,
     0 AS sp_transfer_out_inactive_cost_amt,
     0 AS sp_transfer_out_inactive_qty,
     0 AS sp_transfer_in_other_cost_amt,
     0 AS sp_transfer_in_other_qty,
     0 AS sp_transfer_out_other_cost_amt,
     0 AS sp_transfer_out_other_qty,
     0 AS sp_transfer_in_racking_cost_amt,
     0 AS sp_transfer_in_racking_qty,
     0 AS sp_transfer_out_racking_cost_amt,
     0 AS sp_transfer_out_racking_qty,
     0 AS sp_mark_out_of_stock_active_cost_amt,
     0 AS sp_mark_out_of_stock_active_qty,
     0 AS sp_mark_out_of_stock_inactive_cost_amt,
     0 AS sp_mark_out_of_stock_inactive_qty,
     0 AS sp_receipts_active_cost_amt,
     0 AS sp_receipts_active_qty,
     0 AS sp_receipts_inactive_cost_amt,
     0 AS sp_receipts_inactive_qty,
     0 AS sp_receipts_reserve_cost_amt,
     0 AS sp_receipts_reserve_qty,
     0 AS sp_reclass_in_cost_amt,
     0 AS sp_reclass_in_qty,
     0 AS sp_reclass_out_cost_amt,
     0 AS sp_reclass_out_qty,
     0 AS sp_return_to_vendor_active_cost_amt,
     0 AS sp_return_to_vendor_active_qty,
     0 AS sp_return_to_vendor_inactive_cost_amt,
     0 AS sp_return_to_vendor_inactive_qty,
     0 AS sp_vendor_funds_cost_amt,
     0 AS sp_discount_terms_cost_amt,
     0 AS sp_merch_margin_retail_amt,
     0 AS sp_inventory_movement_in_dropship_cost_amt,
     0 AS sp_inventory_movement_in_dropship_qty,
     0 AS sp_inventory_movement_out_dropship_cost_amt,
     0 AS sp_inventory_movement_out_dropship_qty,
     0 AS sp_other_inventory_adjustment_cost_amt,
     0 AS sp_other_inventory_adjustment_qty,
     0 AS sp_open_to_buy_receipts_active_cost_amt,
     0 AS sp_open_to_buy_receipts_inactive_cost_amt,
     0 AS ty_beginning_of_period_active_cost_amt,
     0 AS ty_beginning_of_period_active_qty,
     0 AS ty_beginning_of_period_inactive_cost_amt,
     0 AS ty_beginning_of_period_inactive_qty,
     0 AS ty_ending_of_period_active_cost_amt,
     0 AS ty_ending_of_period_active_qty,
     0 AS ty_ending_of_period_inactive_cost_amt,
     0 AS ty_ending_of_period_inactive_qty,
     0 AS ty_transfer_in_active_cost_amt,
     0 AS ty_transfer_in_active_qty,
     0 AS ty_transfer_out_active_cost_amt,
     0 AS ty_transfer_out_active_qty,
     0 AS ty_transfer_in_inactive_cost_amt,
     0 AS ty_transfer_in_inactive_qty,
     0 AS ty_transfer_out_inactive_cost_amt,
     0 AS ty_transfer_out_inactive_qty,
     0 AS ty_transfer_in_other_cost_amt,
     0 AS ty_transfer_in_other_qty,
     0 AS ty_transfer_out_other_cost_amt,
     0 AS ty_transfer_out_other_qty,
     0 AS ty_transfer_in_racking_cost_amt,
     0 AS ty_transfer_in_racking_qty,
     0 AS ty_transfer_out_racking_cost_amt,
     0 AS ty_transfer_out_racking_qty,
     0 AS ty_mark_out_of_stock_active_cost_amt,
     0 AS ty_mark_out_of_stock_active_qty,
     0 AS ty_mark_out_of_stock_inactive_cost_amt,
     0 AS ty_mark_out_of_stock_inactive_qty,
     0 AS ty_receipts_active_cost_amt,
     0 AS ty_receipts_active_qty,
     0 AS ty_receipts_inactive_cost_amt,
     0 AS ty_receipts_inactive_qty,
     0 AS ty_rp_receipts_active_cost_amt,
     0 AS ty_non_rp_receipts_active_cost_amt,
     0 AS ty_reclass_in_cost_amt,
     0 AS ty_reclass_in_qty,
     0 AS ty_reclass_out_cost_amt,
     0 AS ty_reclass_out_qty,
     0 AS ty_return_to_vendor_active_cost_amt,
     0 AS ty_return_to_vendor_active_qty,
     0 AS ty_return_to_vendor_inactive_cost_amt,
     0 AS ty_return_to_vendor_inactive_qty,
     0 AS ty_vendor_funds_cost_amt,
     0 AS ty_discount_terms_cost_amt,
     0 AS ty_merch_margin_retail_amt,
     COALESCE(iifbv.cp_beginning_of_period_active_cost_amt, 0) AS ly_beginning_of_period_active_cost_amt,
     COALESCE(iifbv.cp_beginning_of_period_active_qty, 0) AS ly_beginning_of_period_active_qty,
     COALESCE(iifbv.cp_beginning_of_period_inactive_cost_amt, 0) AS ly_beginning_of_period_inactive_cost_amt,
     COALESCE(iifbv.cp_beginning_of_period_inactive_qty, 0) AS ly_beginning_of_period_inactive_qty,
     COALESCE(iifbv.cp_ending_of_period_active_cost_amt, 0) AS ly_ending_of_period_active_cost_amt,
     COALESCE(iifbv.cp_ending_of_period_active_qty, 0) AS ly_ending_of_period_active_qty,
     COALESCE(iifbv.cp_ending_of_period_inactive_cost_amt, 0) AS ly_ending_of_period_inactive_cost_amt,
     COALESCE(iifbv.cp_ending_of_period_inactive_qty, 0) AS ly_ending_of_period_inactive_qty,
     COALESCE(iifbv.cp_transfer_in_active_cost_amt, 0) AS ly_transfer_in_active_cost_amt,
     COALESCE(iifbv.cp_transfer_in_active_qty, 0) AS ly_transfer_in_active_qty,
     COALESCE(iifbv.cp_transfer_out_active_cost_amt, 0) AS ly_transfer_out_active_cost_amt,
     COALESCE(iifbv.cp_transfer_out_active_qty, 0) AS ly_transfer_out_active_qty,
     COALESCE(iifbv.cp_transfer_in_inactive_cost_amt, 0) AS ly_transfer_in_inactive_cost_amt,
     COALESCE(iifbv.cp_transfer_in_inactive_qty, 0) AS ly_transfer_in_inactive_qty,
     COALESCE(iifbv.cp_transfer_out_inactive_cost_amt, 0) AS ly_transfer_out_inactive_cost_amt,
     COALESCE(iifbv.cp_transfer_out_inactive_qty, 0) AS ly_transfer_out_inactive_qty,
     COALESCE(iifbv.cp_transfer_in_other_cost_amt, 0) AS ly_transfer_in_other_cost_amt,
     COALESCE(iifbv.cp_transfer_in_other_qty, 0) AS ly_transfer_in_other_qty,
     COALESCE(iifbv.cp_transfer_out_other_cost_amt, 0) AS ly_transfer_out_other_cost_amt,
     COALESCE(iifbv.cp_transfer_out_other_qty, 0) AS ly_transfer_out_other_qty,
     COALESCE(iifbv.cp_transfer_in_racking_cost_amt, 0) AS ly_transfer_in_racking_cost_amt,
     COALESCE(iifbv.cp_transfer_in_racking_qty, 0) AS ly_transfer_in_racking_qty,
     COALESCE(iifbv.cp_transfer_out_racking_cost_amt, 0) AS ly_transfer_out_racking_cost_amt,
     COALESCE(iifbv.cp_transfer_out_racking_qty, 0) AS ly_transfer_out_racking_qty,
     COALESCE(iifbv.cp_mark_out_of_stock_active_cost_amt, 0) AS ly_mark_out_of_stock_active_cost_amt,
     COALESCE(iifbv.cp_mark_out_of_stock_active_qty, 0) AS ly_mark_out_of_stock_active_qty,
     COALESCE(iifbv.cp_mark_out_of_stock_inactive_cost_amt, 0) AS ly_mark_out_of_stock_inactive_cost_amt,
     COALESCE(iifbv.cp_mark_out_of_stock_inactive_qty, 0) AS ly_mark_out_of_stock_inactive_qty,
     COALESCE(iifbv.cp_receipts_active_cost_amt, 0) AS ly_receipts_active_cost_amt,
     COALESCE(iifbv.cp_receipts_active_qty, 0) AS ly_receipts_active_qty,
     COALESCE(iifbv.cp_receipts_inactive_cost_amt, 0) AS ly_receipts_inactive_cost_amt,
     COALESCE(iifbv.cp_receipts_inactive_qty, 0) AS ly_receipts_inactive_qty,
     0 AS ly_rp_receipts_active_cost_amt,
     0 AS ly_non_rp_receipts_active_cost_amt,
     COALESCE(iifbv.cp_reclass_in_cost_amt, 0) AS ly_reclass_in_cost_amt,
     COALESCE(iifbv.cp_reclass_in_qty, 0) AS ly_reclass_in_qty,
     COALESCE(iifbv.cp_reclass_out_cost_amt, 0) AS ly_reclass_out_cost_amt,
     COALESCE(iifbv.cp_reclass_out_qty, 0) AS ly_reclass_out_qty,
     COALESCE(iifbv.cp_return_to_vendor_active_cost_amt, 0) AS ly_return_to_vendor_active_cost_amt,
     COALESCE(iifbv.cp_return_to_vendor_active_qty, 0) AS ly_return_to_vendor_active_qty,
     COALESCE(iifbv.cp_return_to_vendor_inactive_cost_amt, 0) AS ly_return_to_vendor_inactive_cost_amt,
     COALESCE(iifbv.cp_return_to_vendor_inactive_qty, 0) AS ly_return_to_vendor_inactive_qty,
     COALESCE(iifbv.cp_vendor_funds_cost_amt, 0) AS ly_vendor_funds_cost_amt,
     COALESCE(iifbv.cp_discount_terms_cost_amt, 0) AS ly_discount_terms_cost_amt,
     COALESCE(iifbv.cp_merch_margin_retail_amt, 0) AS ly_merch_margin_retail_amt,
     0 AS lly_beginning_of_period_active_cost_amt,
     0 AS lly_beginning_of_period_active_qty,
     0 AS lly_beginning_of_period_inactive_cost_amt,
     0 AS lly_beginning_of_period_inactive_qty,
     0 AS lly_ending_of_period_active_cost_amt,
     0 AS lly_ending_of_period_active_qty,
     0 AS lly_ending_of_period_inactive_cost_amt,
     0 AS lly_ending_of_period_inactive_qty,
     0 AS lly_transfer_in_active_cost_amt,
     0 AS lly_transfer_in_active_qty,
     0 AS lly_transfer_out_active_cost_amt,
     0 AS lly_transfer_out_active_qty,
     0 AS lly_transfer_in_inactive_cost_amt,
     0 AS lly_transfer_in_inactive_qty,
     0 AS lly_transfer_out_inactive_cost_amt,
     0 AS lly_transfer_out_inactive_qty,
     0 AS lly_transfer_in_other_cost_amt,
     0 AS lly_transfer_in_other_qty,
     0 AS lly_transfer_out_other_cost_amt,
     0 AS lly_transfer_out_other_qty,
     0 AS lly_transfer_in_racking_cost_amt,
     0 AS lly_transfer_in_racking_qty,
     0 AS lly_transfer_out_racking_cost_amt,
     0 AS lly_transfer_out_racking_qty,
     0 AS lly_mark_out_of_stock_active_cost_amt,
     0 AS lly_mark_out_of_stock_active_qty,
     0 AS lly_mark_out_of_stock_inactive_cost_amt,
     0 AS lly_mark_out_of_stock_inactive_qty,
     0 AS lly_receipts_active_cost_amt,
     0 AS lly_receipts_active_qty,
     0 AS lly_receipts_inactive_cost_amt,
     0 AS lly_receipts_inactive_qty,
     0 AS lly_rp_receipts_active_cost_amt,
     0 AS lly_non_rp_receipts_active_cost_amt,
     0 AS lly_reclass_in_cost_amt,
     0 AS lly_reclass_in_qty,
     0 AS lly_reclass_out_cost_amt,
     0 AS lly_reclass_out_qty,
     0 AS lly_return_to_vendor_active_cost_amt,
     0 AS lly_return_to_vendor_active_qty,
     0 AS lly_return_to_vendor_inactive_cost_amt,
     0 AS lly_return_to_vendor_inactive_qty,
     0 AS lly_vendor_funds_cost_amt,
     0 AS lly_discount_terms_cost_amt,
     0 AS lly_merch_margin_retail_amt,
     0 AS ty_shrink_banner_cost_amt,
     0 AS ty_shrink_banner_qty,
     0 AS ty_other_inventory_adj_cost,
     0 AS ty_merch_margin_charges_cost,
     0 AS ly_shrink_banner_cost_amt,
     0 AS ly_shrink_banner_qty,
     COALESCE(iifbv.cp_other_inventory_adjustment_cost_amt, 0) AS ly_other_inventory_adj_cost,
     0 AS ly_merch_margin_charges_cost,
     0 AS lly_shrink_banner_cost_amt,
     0 AS lly_shrink_banner_qty,
     0 AS lly_other_inventory_adj_cost,
     0 AS lly_merch_margin_charges_cost,
     iifbv.dw_batch_dt
    FROM (SELECT mfcbpf.department_number AS dept_num,
       mfcbpf.week_id AS week_num,
       obcd.banner_country_num,
       aimd.fulfill_type_num,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN') AND mfcbpf.week_id <> mtbv2.current_fiscal_week_num
          
        THEN mfcbpf.active_beginning_inventory_dollar_amount
        ELSE 0
        END AS cp_beginning_of_period_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN') AND mfcbpf.week_id <> mtbv2.current_fiscal_week_num
          
        THEN mfcbpf.active_beginning_inventory_units
        ELSE 0
        END AS cp_beginning_of_period_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN') AND mfcbpf.week_id <> mtbv2.current_fiscal_week_num
          
        THEN mfcbpf.inactive_beginning_period_inventory_dollar_amount
        ELSE 0
        END AS cp_beginning_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN') AND mfcbpf.week_id <> mtbv2.current_fiscal_week_num
          
        THEN mfcbpf.inactive_beginning_period_inventory_units
        ELSE 0
        END AS cp_beginning_of_period_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_beginning_period_inventory_target_units
        ELSE 0
        END AS cp_beginning_of_period_active_inventory_target_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_ending_inventory_dollar_amount
        ELSE 0
        END AS cp_ending_of_period_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_ending_inventory_units
        ELSE 0
        END AS cp_ending_of_period_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_ending_inventory_dollar_amount
        ELSE 0
        END AS cp_ending_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_ending_inventory_units
        ELSE 0
        END AS cp_ending_of_period_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_inventory_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_inventory_in_units
        ELSE 0
        END AS cp_transfer_in_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_inventory_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_inventory_out_units
        ELSE 0
        END AS cp_transfer_out_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_inventory_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_inventory_in_units
        ELSE 0
        END AS cp_transfer_in_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_inventory_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_inventory_out_units
        ELSE 0
        END AS cp_transfer_out_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.other_inventory_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_other_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.other_inventory_in_units
        ELSE 0
        END AS cp_transfer_in_other_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.other_inventory_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_other_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.other_inventory_out_units
        ELSE 0
        END AS cp_transfer_out_other_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.racking_in_dollar_amount
        ELSE 0
        END AS cp_transfer_in_racking_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.racking_in_units
        ELSE 0
        END AS cp_transfer_in_racking_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.racking_out_dollar_amount
        ELSE 0
        END AS cp_transfer_out_racking_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.racking_out_units
        ELSE 0
        END AS cp_transfer_out_racking_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_mos_dollar_amount
        ELSE 0
        END AS cp_mark_out_of_stock_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_mos_units
        ELSE 0
        END AS cp_mark_out_of_stock_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_mos_dollar_amount
        ELSE 0
        END AS cp_mark_out_of_stock_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_mos_units
        ELSE 0
        END AS cp_mark_out_of_stock_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_receipts_dollar_amount
        ELSE 0
        END AS cp_receipts_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_receipts_units
        ELSE 0
        END AS cp_receipts_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_receipts_dollar_amount
        ELSE 0
        END AS cp_receipts_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_receipts_units
        ELSE 0
        END AS cp_receipts_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.reserve_receipts_dollar_amount
        ELSE 0
        END AS cp_receipts_reserve_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.reserve_receipts_units
        ELSE 0
        END AS cp_receipts_reserve_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.reclass_in_dollar_amount
        ELSE 0
        END AS cp_reclass_in_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.reclass_in_units
        ELSE 0
        END AS cp_reclass_in_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.reclass_out_dollar_amount
        ELSE 0
        END AS cp_reclass_out_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.reclass_out_units
        ELSE 0
        END AS cp_reclass_out_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_rtv_dollar_amount
        ELSE 0
        END AS cp_return_to_vendor_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_rtv_units
        ELSE 0
        END AS cp_return_to_vendor_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_rtv_dollar_amount
        ELSE 0
        END AS cp_return_to_vendor_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_rtv_units
        ELSE 0
        END AS cp_return_to_vendor_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.vendor_funds_cost_amount
        ELSE 0
        END AS cp_vendor_funds_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.discount_terms_cost_amount
        ELSE 0
        END AS cp_discount_terms_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.merch_margin_retail_amount
        ELSE 0
        END AS cp_merch_margin_retail_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_in_dollar_amount
        ELSE 0
        END AS cp_inventory_movement_in_dropship_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_in_units
        ELSE 0
        END AS cp_inventory_movement_in_dropship_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_out_dollar_amount
        ELSE 0
        END AS cp_inventory_movement_out_dropship_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_out_units
        ELSE 0
        END AS cp_inventory_movement_out_dropship_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.other_inventory_adjustments_dollar_amount
        ELSE 0
        END AS cp_other_inventory_adjustment_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.other_inventory_adjustments_units
        ELSE 0
        END AS cp_other_inventory_adjustment_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.active_opentobuy_cost_amount
        ELSE 0
        END AS cp_open_to_buy_receipts_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('CURRENT_PLAN')
        THEN mfcbpf.inactive_opentobuy_cost_amount
        ELSE 0
        END AS cp_open_to_buy_receipts_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_beginning_inventory_dollar_amount
        ELSE 0
        END AS op_beginning_of_period_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_beginning_period_inventory_target_units
        ELSE 0
        END AS op_beginning_of_period_active_inventory_target_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_beginning_inventory_units
        ELSE 0
        END AS op_beginning_of_period_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_beginning_period_inventory_dollar_amount
        ELSE 0
        END AS op_beginning_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_beginning_period_inventory_units
        ELSE 0
        END AS op_beginning_of_period_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_ending_inventory_dollar_amount
        ELSE 0
        END AS op_ending_of_period_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_ending_inventory_units
        ELSE 0
        END AS op_ending_of_period_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_ending_inventory_dollar_amount
        ELSE 0
        END AS op_ending_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_ending_inventory_units
        ELSE 0
        END AS op_ending_of_period_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_inventory_in_dollar_amount
        ELSE 0
        END AS op_transfer_in_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_inventory_in_units
        ELSE 0
        END AS op_transfer_in_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_inventory_out_dollar_amount
        ELSE 0
        END AS op_transfer_out_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_inventory_out_units
        ELSE 0
        END AS op_transfer_out_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_inventory_in_dollar_amount
        ELSE 0
        END AS op_transfer_in_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_inventory_in_units
        ELSE 0
        END AS op_transfer_in_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_inventory_out_dollar_amount
        ELSE 0
        END AS op_transfer_out_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_inventory_out_units
        ELSE 0
        END AS op_transfer_out_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.other_inventory_in_dollar_amount
        ELSE 0
        END AS op_transfer_in_other_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.other_inventory_in_units
        ELSE 0
        END AS op_transfer_in_other_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.other_inventory_out_dollar_amount
        ELSE 0
        END AS op_transfer_out_other_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.other_inventory_out_units
        ELSE 0
        END AS op_transfer_out_other_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.racking_in_dollar_amount
        ELSE 0
        END AS op_transfer_in_racking_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.racking_in_units
        ELSE 0
        END AS op_transfer_in_racking_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.racking_out_dollar_amount
        ELSE 0
        END AS op_transfer_out_racking_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.racking_out_units
        ELSE 0
        END AS op_transfer_out_racking_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_mos_dollar_amount
        ELSE 0
        END AS op_mark_out_of_stock_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_mos_units
        ELSE 0
        END AS op_mark_out_of_stock_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_mos_dollar_amount
        ELSE 0
        END AS op_mark_out_of_stock_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_mos_units
        ELSE 0
        END AS op_mark_out_of_stock_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_receipts_dollar_amount
        ELSE 0
        END AS op_receipts_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_receipts_units
        ELSE 0
        END AS op_receipts_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_receipts_dollar_amount
        ELSE 0
        END AS op_receipts_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_receipts_units
        ELSE 0
        END AS op_receipts_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.reserve_receipts_dollar_amount
        ELSE 0
        END AS op_receipts_reserve_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.reserve_receipts_units
        ELSE 0
        END AS op_receipts_reserve_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.reclass_in_dollar_amount
        ELSE 0
        END AS op_reclass_in_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.reclass_in_units
        ELSE 0
        END AS op_reclass_in_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.reclass_out_dollar_amount
        ELSE 0
        END AS op_reclass_out_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.reclass_out_units
        ELSE 0
        END AS op_reclass_out_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_rtv_dollar_amount
        ELSE 0
        END AS op_return_to_vendor_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.active_rtv_units
        ELSE 0
        END AS op_return_to_vendor_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_rtv_dollar_amount
        ELSE 0
        END AS op_return_to_vendor_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inactive_rtv_units
        ELSE 0
        END AS op_return_to_vendor_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.vendor_funds_cost_amount
        ELSE 0
        END AS op_vendor_funds_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.discount_terms_cost_amount
        ELSE 0
        END AS op_discount_terms_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.merch_margin_retail_amount
        ELSE 0
        END AS op_merch_margin_retail_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_in_dollar_amount
        ELSE 0
        END AS op_inventory_movement_in_dropship_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_in_units
        ELSE 0
        END AS op_inventory_movement_in_dropship_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_out_dollar_amount
        ELSE 0
        END AS op_inventory_movement_out_dropship_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_out_units
        ELSE 0
        END AS op_inventory_movement_out_dropship_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.other_inventory_adjustments_dollar_amount
        ELSE 0
        END AS op_other_inventory_adjustment_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('ORIGINAL_PLAN')
        THEN mfcbpf.other_inventory_adjustments_units
        ELSE 0
        END AS op_other_inventory_adjustment_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_beginning_inventory_dollar_amount
        ELSE 0
        END AS sp_beginning_of_period_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_beginning_period_inventory_target_units
        ELSE 0
        END AS sp_beginning_of_period_active_inventory_target_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_beginning_inventory_units
        ELSE 0
        END AS sp_beginning_of_period_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_beginning_period_inventory_dollar_amount
        ELSE 0
        END AS sp_beginning_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_beginning_period_inventory_units
        ELSE 0
        END AS sp_beginning_of_period_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_ending_inventory_dollar_amount
        ELSE 0
        END AS sp_ending_of_period_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_ending_inventory_units
        ELSE 0
        END AS sp_ending_of_period_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_ending_inventory_dollar_amount
        ELSE 0
        END AS sp_ending_of_period_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_ending_inventory_units
        ELSE 0
        END AS sp_ending_of_period_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_inventory_in_dollar_amount
        ELSE 0
        END AS sp_transfer_in_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_inventory_in_units
        ELSE 0
        END AS sp_transfer_in_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_inventory_out_dollar_amount
        ELSE 0
        END AS sp_transfer_out_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_inventory_out_units
        ELSE 0
        END AS sp_transfer_out_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_inventory_in_dollar_amount
        ELSE 0
        END AS sp_transfer_in_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_inventory_in_units
        ELSE 0
        END AS sp_transfer_in_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_inventory_out_dollar_amount
        ELSE 0
        END AS sp_transfer_out_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_inventory_out_units
        ELSE 0
        END AS sp_transfer_out_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.other_inventory_in_dollar_amount
        ELSE 0
        END AS sp_transfer_in_other_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.other_inventory_in_units
        ELSE 0
        END AS sp_transfer_in_other_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.other_inventory_out_dollar_amount
        ELSE 0
        END AS sp_transfer_out_other_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.other_inventory_out_units
        ELSE 0
        END AS sp_transfer_out_other_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.racking_in_dollar_amount
        ELSE 0
        END AS sp_transfer_in_racking_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.racking_in_units
        ELSE 0
        END AS sp_transfer_in_racking_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.racking_out_dollar_amount
        ELSE 0
        END AS sp_transfer_out_racking_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.racking_out_units
        ELSE 0
        END AS sp_transfer_out_racking_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_mos_dollar_amount
        ELSE 0
        END AS sp_mark_out_of_stock_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_mos_units
        ELSE 0
        END AS sp_mark_out_of_stock_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_mos_dollar_amount
        ELSE 0
        END AS sp_mark_out_of_stock_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_mos_units
        ELSE 0
        END AS sp_mark_out_of_stock_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_receipts_dollar_amount
        ELSE 0
        END AS sp_receipts_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_receipts_units
        ELSE 0
        END AS sp_receipts_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_receipts_dollar_amount
        ELSE 0
        END AS sp_receipts_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_receipts_units
        ELSE 0
        END AS sp_receipts_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.reserve_receipts_dollar_amount
        ELSE 0
        END AS sp_receipts_reserve_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.reserve_receipts_units
        ELSE 0
        END AS sp_receipts_reserve_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.reclass_in_dollar_amount
        ELSE 0
        END AS sp_reclass_in_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.reclass_in_units
        ELSE 0
        END AS sp_reclass_in_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.reclass_out_dollar_amount
        ELSE 0
        END AS sp_reclass_out_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.reclass_out_units
        ELSE 0
        END AS sp_reclass_out_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_rtv_dollar_amount
        ELSE 0
        END AS sp_return_to_vendor_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_rtv_units
        ELSE 0
        END AS sp_return_to_vendor_active_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_rtv_dollar_amount
        ELSE 0
        END AS sp_return_to_vendor_inactive_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_rtv_units
        ELSE 0
        END AS sp_return_to_vendor_inactive_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.vendor_funds_cost_amount
        ELSE 0
        END AS sp_vendor_funds_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.discount_terms_cost_amount
        ELSE 0
        END AS sp_discount_terms_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.merch_margin_retail_amount
        ELSE 0
        END AS sp_merch_margin_retail_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_in_dollar_amount
        ELSE 0
        END AS sp_inventory_movement_in_dropship_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_in_units
        ELSE 0
        END AS sp_inventory_movement_in_dropship_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_out_dollar_amount
        ELSE 0
        END AS sp_inventory_movement_out_dropship_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inventory_movement_drop_ship_out_units
        ELSE 0
        END AS sp_inventory_movement_out_dropship_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.other_inventory_adjustments_dollar_amount
        ELSE 0
        END AS sp_other_inventory_adjustment_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.other_inventory_adjustments_units
        ELSE 0
        END AS sp_other_inventory_adjustment_qty,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.active_opentobuy_cost_amount
        ELSE 0
        END AS sp_open_to_buy_receipts_active_cost_amt,
        CASE
        WHEN LOWER(mfcbpf.financial_plan_version) = LOWER('SNAP_PLAN')
        THEN mfcbpf.inactive_opentobuy_cost_amount
        ELSE 0
        END AS sp_open_to_buy_receipts_inactive_cost_amt,
       0 AS ty_beginning_of_period_active_cost_amt,
       0 AS ty_beginning_of_period_active_qty,
       0 AS ty_beginning_of_period_inactive_cost_amt,
       0 AS ty_beginning_of_period_inactive_qty,
       0 AS ty_ending_of_period_active_cost_amt,
       0 AS ty_ending_of_period_active_qty,
       0 AS ty_ending_of_period_inactive_cost_amt,
       0 AS ty_ending_of_period_inactive_qty,
       0 AS ty_transfer_in_active_cost_amt,
       0 AS ty_transfer_in_active_qty,
       0 AS ty_transfer_out_active_cost_amt,
       0 AS ty_transfer_out_active_qty,
       0 AS ty_transfer_in_inactive_cost_amt,
       0 AS ty_transfer_in_inactive_qty,
       0 AS ty_transfer_out_inactive_cost_amt,
       0 AS ty_transfer_out_inactive_qty,
       0 AS ty_transfer_in_other_cost_amt,
       0 AS ty_transfer_in_other_qty,
       0 AS ty_transfer_out_other_cost_amt,
       0 AS ty_transfer_out_other_qty,
       0 AS ty_transfer_in_racking_cost_amt,
       0 AS ty_transfer_in_racking_qty,
       0 AS ty_transfer_out_racking_cost_amt,
       0 AS ty_transfer_out_racking_qty,
       0 AS ty_mark_out_of_stock_active_cost_amt,
       0 AS ty_mark_out_of_stock_active_qty,
       0 AS ty_mark_out_of_stock_inactive_cost_amt,
       0 AS ty_mark_out_of_stock_inactive_qty,
       0 AS ty_receipts_active_cost_amt,
       0 AS ty_receipts_active_qty,
       0 AS ty_receipts_inactive_cost_amt,
       0 AS ty_receipts_inactive_qty,
       0 AS ty_rp_receipts_active_cost_amt,
       0 AS ty_non_rp_receipts_active_cost_amt,
       0 AS ty_reclass_in_cost_amt,
       0 AS ty_reclass_in_qty,
       0 AS ty_reclass_out_cost_amt,
       0 AS ty_reclass_out_qty,
       0 AS ty_return_to_vendor_active_cost_amt,
       0 AS ty_return_to_vendor_active_qty,
       0 AS ty_return_to_vendor_inactive_cost_amt,
       0 AS ty_return_to_vendor_inactive_qty,
       0 AS ty_vendor_funds_cost_amt,
       0 AS ty_discount_terms_cost_amt,
       0 AS ty_merch_margin_retail_amt,
       0 AS ly_beginning_of_period_active_cost_amt,
       0 AS ly_beginning_of_period_active_qty,
       0 AS ly_beginning_of_period_inactive_cost_amt,
       0 AS ly_beginning_of_period_inactive_qty,
       0 AS ly_ending_of_period_active_cost_amt,
       0 AS ly_ending_of_period_active_qty,
       0 AS ly_ending_of_period_inactive_cost_amt,
       0 AS ly_ending_of_period_inactive_qty,
       0 AS ly_transfer_in_active_cost_amt,
       0 AS ly_transfer_in_active_qty,
       0 AS ly_transfer_out_active_cost_amt,
       0 AS ly_transfer_out_active_qty,
       0 AS ly_transfer_in_inactive_cost_amt,
       0 AS ly_transfer_in_inactive_qty,
       0 AS ly_transfer_out_inactive_cost_amt,
       0 AS ly_transfer_out_inactive_qty,
       0 AS ly_transfer_in_other_cost_amt,
       0 AS ly_transfer_in_other_qty,
       0 AS ly_transfer_out_other_cost_amt,
       0 AS ly_transfer_out_other_qty,
       0 AS ly_transfer_in_racking_cost_amt,
       0 AS ly_transfer_in_racking_qty,
       0 AS ly_transfer_out_racking_cost_amt,
       0 AS ly_transfer_out_racking_qty,
       0 AS ly_mark_out_of_stock_active_cost_amt,
       0 AS ly_mark_out_of_stock_active_qty,
       0 AS ly_mark_out_of_stock_inactive_cost_amt,
       0 AS ly_mark_out_of_stock_inactive_qty,
       0 AS ly_receipts_active_cost_amt,
       0 AS ly_receipts_active_qty,
       0 AS ly_receipts_inactive_cost_amt,
       0 AS ly_receipts_inactive_qty,
       0 AS ly_rp_receipts_active_cost_amt,
       0 AS ly_non_rp_receipts_active_cost_amt,
       0 AS ly_reclass_in_cost_amt,
       0 AS ly_reclass_in_qty,
       0 AS ly_reclass_out_cost_amt,
       0 AS ly_reclass_out_qty,
       0 AS ly_return_to_vendor_active_cost_amt,
       0 AS ly_return_to_vendor_active_qty,
       0 AS ly_return_to_vendor_inactive_cost_amt,
       0 AS ly_return_to_vendor_inactive_qty,
       0 AS ly_vendor_funds_cost_amt,
       0 AS ly_discount_terms_cost_amt,
       0 AS ly_merch_margin_retail_amt,
       0 AS lly_beginning_of_period_active_cost_amt,
       0 AS lly_beginning_of_period_active_qty,
       0 AS lly_beginning_of_period_inactive_cost_amt,
       0 AS lly_beginning_of_period_inactive_qty,
       0 AS lly_ending_of_period_active_cost_amt,
       0 AS lly_ending_of_period_active_qty,
       0 AS lly_ending_of_period_inactive_cost_amt,
       0 AS lly_ending_of_period_inactive_qty,
       0 AS lly_transfer_in_active_cost_amt,
       0 AS lly_transfer_in_active_qty,
       0 AS lly_transfer_out_active_cost_amt,
       0 AS lly_transfer_out_active_qty,
       0 AS lly_transfer_in_inactive_cost_amt,
       0 AS lly_transfer_in_inactive_qty,
       0 AS lly_transfer_out_inactive_cost_amt,
       0 AS lly_transfer_out_inactive_qty,
       0 AS lly_transfer_in_other_cost_amt,
       0 AS lly_transfer_in_other_qty,
       0 AS lly_transfer_out_other_cost_amt,
       0 AS lly_transfer_out_other_qty,
       0 AS lly_transfer_in_racking_cost_amt,
       0 AS lly_transfer_in_racking_qty,
       0 AS lly_transfer_out_racking_cost_amt,
       0 AS lly_transfer_out_racking_qty,
       0 AS lly_mark_out_of_stock_active_cost_amt,
       0 AS lly_mark_out_of_stock_active_qty,
       0 AS lly_mark_out_of_stock_inactive_cost_amt,
       0 AS lly_mark_out_of_stock_inactive_qty,
       0 AS lly_receipts_active_cost_amt,
       0 AS lly_receipts_active_qty,
       0 AS lly_receipts_inactive_cost_amt,
       0 AS lly_receipts_inactive_qty,
       0 AS lly_rp_receipts_active_cost_amt,
       0 AS lly_non_rp_receipts_active_cost_amt,
       0 AS lly_reclass_in_cost_amt,
       0 AS lly_reclass_in_qty,
       0 AS lly_reclass_out_cost_amt,
       0 AS lly_reclass_out_qty,
       0 AS lly_return_to_vendor_active_cost_amt,
       0 AS lly_return_to_vendor_active_qty,
       0 AS lly_return_to_vendor_inactive_cost_amt,
       0 AS lly_return_to_vendor_inactive_qty,
       0 AS lly_vendor_funds_cost_amt,
       0 AS lly_discount_terms_cost_amt,
       0 AS lly_merch_margin_retail_amt,
       0 AS ty_shrink_banner_cost_amt,
       0 AS ty_shrink_banner_qty,
       0 AS ty_other_inventory_adj_cost,
       0 AS ty_merch_margin_charges_cost,
       0 AS ly_shrink_banner_cost_amt,
       0 AS ly_shrink_banner_qty,
       0 AS ly_other_inventory_adj_cost,
       0 AS ly_merch_margin_charges_cost,
       0 AS lly_shrink_banner_cost_amt,
       0 AS lly_shrink_banner_qty,
       0 AS lly_other_inventory_adj_cost,
       0 AS lly_merch_margin_charges_cost,
       mtbv2.dw_batch_dt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_country_banner_plan_fct AS mfcbpf
       LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.org_banner_country_dim AS obcd ON LOWER(mfcbpf.channel_country) = LOWER(obcd.country_code)
        AND LOWER(mfcbpf.pricing_channel) = LOWER(obcd.banner_desc)
       LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.alternate_inventory_model_dim AS aimd ON LOWER(mfcbpf.alternate_inventory_model) =
        LOWER(aimd.fulfill_type_desc)
       INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv2 ON LOWER(mtbv2.interface_code) = LOWER('MFP_BANR_BLEND_WKLY'
         )) AS iifbv
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.week_cal_vw AS wcd1 ON wcd1.ly_week_num <> 444444 AND iifbv.week_num = wcd1.ly_week_num
     WHERE wcd1.LY_WEEK_NUM  > (SELECT CASE WHEN DAY_NUM_OF_FISCAL_WEEK = 7
                        THEN l01.CURRENT_FISCAL_WEEK_NUM
                        ELSE l01.LAST_COMPLETED_FISCAL_WEEK_NUM
                        END
                    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw l01 
                    JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim d01
                    ON l01.DW_BATCH_DT = d01.DAY_DATE
                    WHERE LOWER(l01.INTERFACE_CODE) = LOWER('MFP_BANR_BLEND_WKLY'))
       
    UNION ALL
    SELECT mfcbpf0.department_number AS dept_num,
     mfcbpf0.week_id AS week_num,
     obcd0.banner_country_num,
     aimd0.fulfill_type_num,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_beginning_inventory_dollar_amount
      ELSE 0
      END AS cp_beginning_of_period_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_beginning_period_inventory_target_units
      ELSE 0
      END AS cp_beginning_of_period_active_inventory_target_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_beginning_inventory_units
      ELSE 0
      END AS cp_beginning_of_period_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_beginning_period_inventory_dollar_amount
      ELSE 0
      END AS cp_beginning_of_period_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_beginning_period_inventory_units
      ELSE 0
      END AS cp_beginning_of_period_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_ending_inventory_dollar_amount
      ELSE 0
      END AS cp_ending_of_period_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_ending_inventory_units
      ELSE 0
      END AS cp_ending_of_period_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_ending_inventory_dollar_amount
      ELSE 0
      END AS cp_ending_of_period_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_ending_inventory_units
      ELSE 0
      END AS cp_ending_of_period_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_inventory_in_dollar_amount
      ELSE 0
      END AS cp_transfer_in_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_inventory_in_units
      ELSE 0
      END AS cp_transfer_in_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_inventory_out_dollar_amount
      ELSE 0
      END AS cp_transfer_out_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_inventory_out_units
      ELSE 0
      END AS cp_transfer_out_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_inventory_in_dollar_amount
      ELSE 0
      END AS cp_transfer_in_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_inventory_in_units
      ELSE 0
      END AS cp_transfer_in_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_inventory_out_dollar_amount
      ELSE 0
      END AS cp_transfer_out_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_inventory_out_units
      ELSE 0
      END AS cp_transfer_out_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.other_inventory_in_dollar_amount
      ELSE 0
      END AS cp_transfer_in_other_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.other_inventory_in_units
      ELSE 0
      END AS cp_transfer_in_other_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.other_inventory_out_dollar_amount
      ELSE 0
      END AS cp_transfer_out_other_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.other_inventory_out_units
      ELSE 0
      END AS cp_transfer_out_other_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.racking_in_dollar_amount
      ELSE 0
      END AS cp_transfer_in_racking_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.racking_in_units
      ELSE 0
      END AS cp_transfer_in_racking_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.racking_out_dollar_amount
      ELSE 0
      END AS cp_transfer_out_racking_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.racking_out_units
      ELSE 0
      END AS cp_transfer_out_racking_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_mos_dollar_amount
      ELSE 0
      END AS cp_mark_out_of_stock_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_mos_units
      ELSE 0
      END AS cp_mark_out_of_stock_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_mos_dollar_amount
      ELSE 0
      END AS cp_mark_out_of_stock_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_mos_units
      ELSE 0
      END AS cp_mark_out_of_stock_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_receipts_dollar_amount
      ELSE 0
      END AS cp_receipts_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_receipts_units
      ELSE 0
      END AS cp_receipts_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_receipts_dollar_amount
      ELSE 0
      END AS cp_receipts_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_receipts_units
      ELSE 0
      END AS cp_receipts_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.reserve_receipts_dollar_amount
      ELSE 0
      END AS cp_receipts_reserve_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.reserve_receipts_units
      ELSE 0
      END AS cp_receipts_reserve_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.reclass_in_dollar_amount
      ELSE 0
      END AS cp_reclass_in_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.reclass_in_units
      ELSE 0
      END AS cp_reclass_in_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.reclass_out_dollar_amount
      ELSE 0
      END AS cp_reclass_out_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.reclass_out_units
      ELSE 0
      END AS cp_reclass_out_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_rtv_dollar_amount
      ELSE 0
      END AS cp_return_to_vendor_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_rtv_units
      ELSE 0
      END AS cp_return_to_vendor_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_rtv_dollar_amount
      ELSE 0
      END AS cp_return_to_vendor_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_rtv_units
      ELSE 0
      END AS cp_return_to_vendor_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.vendor_funds_cost_amount
      ELSE 0
      END AS cp_vendor_funds_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.discount_terms_cost_amount
      ELSE 0
      END AS cp_discount_terms_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.merch_margin_retail_amount
      ELSE 0
      END AS cp_merch_margin_retail_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_in_dollar_amount
      ELSE 0
      END AS cp_inventory_movement_in_dropship_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_in_units
      ELSE 0
      END AS cp_inventory_movement_in_dropship_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_out_dollar_amount
      ELSE 0
      END AS cp_inventory_movement_out_dropship_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_out_units
      ELSE 0
      END AS cp_inventory_movement_out_dropship_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.other_inventory_adjustments_dollar_amount
      ELSE 0
      END AS cp_other_inventory_adjustment_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.other_inventory_adjustments_units
      ELSE 0
      END AS cp_other_inventory_adjustment_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.active_opentobuy_cost_amount
      ELSE 0
      END AS cp_open_to_buy_receipts_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('CURRENT_PLAN')
      THEN mfcbpf0.inactive_opentobuy_cost_amount
      ELSE 0
      END AS cp_open_to_buy_receipts_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_beginning_inventory_dollar_amount
      ELSE 0
      END AS op_beginning_of_period_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_beginning_period_inventory_target_units
      ELSE 0
      END AS op_beginning_of_period_active_inventory_target_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_beginning_inventory_units
      ELSE 0
      END AS op_beginning_of_period_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_beginning_period_inventory_dollar_amount
      ELSE 0
      END AS op_beginning_of_period_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_beginning_period_inventory_units
      ELSE 0
      END AS op_beginning_of_period_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_ending_inventory_dollar_amount
      ELSE 0
      END AS op_ending_of_period_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_ending_inventory_units
      ELSE 0
      END AS op_ending_of_period_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_ending_inventory_dollar_amount
      ELSE 0
      END AS op_ending_of_period_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_ending_inventory_units
      ELSE 0
      END AS op_ending_of_period_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_inventory_in_dollar_amount
      ELSE 0
      END AS op_transfer_in_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_inventory_in_units
      ELSE 0
      END AS op_transfer_in_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_inventory_out_dollar_amount
      ELSE 0
      END AS op_transfer_out_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_inventory_out_units
      ELSE 0
      END AS op_transfer_out_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_inventory_in_dollar_amount
      ELSE 0
      END AS op_transfer_in_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_inventory_in_units
      ELSE 0
      END AS op_transfer_in_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_inventory_out_dollar_amount
      ELSE 0
      END AS op_transfer_out_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_inventory_out_units
      ELSE 0
      END AS op_transfer_out_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.other_inventory_in_dollar_amount
      ELSE 0
      END AS op_transfer_in_other_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.other_inventory_in_units
      ELSE 0
      END AS op_transfer_in_other_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.other_inventory_out_dollar_amount
      ELSE 0
      END AS op_transfer_out_other_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.other_inventory_out_units
      ELSE 0
      END AS op_transfer_out_other_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.racking_in_dollar_amount
      ELSE 0
      END AS op_transfer_in_racking_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.racking_in_units
      ELSE 0
      END AS op_transfer_in_racking_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.racking_out_dollar_amount
      ELSE 0
      END AS op_transfer_out_racking_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.racking_out_units
      ELSE 0
      END AS op_transfer_out_racking_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_mos_dollar_amount
      ELSE 0
      END AS op_mark_out_of_stock_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_mos_units
      ELSE 0
      END AS op_mark_out_of_stock_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_mos_dollar_amount
      ELSE 0
      END AS op_mark_out_of_stock_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_mos_units
      ELSE 0
      END AS op_mark_out_of_stock_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_receipts_dollar_amount
      ELSE 0
      END AS op_receipts_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_receipts_units
      ELSE 0
      END AS op_receipts_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_receipts_dollar_amount
      ELSE 0
      END AS op_receipts_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_receipts_units
      ELSE 0
      END AS op_receipts_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.reserve_receipts_dollar_amount
      ELSE 0
      END AS op_receipts_reserve_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.reserve_receipts_units
      ELSE 0
      END AS op_receipts_reserve_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.reclass_in_dollar_amount
      ELSE 0
      END AS op_reclass_in_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.reclass_in_units
      ELSE 0
      END AS op_reclass_in_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.reclass_out_dollar_amount
      ELSE 0
      END AS op_reclass_out_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.reclass_out_units
      ELSE 0
      END AS op_reclass_out_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_rtv_dollar_amount
      ELSE 0
      END AS op_return_to_vendor_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.active_rtv_units
      ELSE 0
      END AS op_return_to_vendor_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_rtv_dollar_amount
      ELSE 0
      END AS op_return_to_vendor_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inactive_rtv_units
      ELSE 0
      END AS op_return_to_vendor_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.vendor_funds_cost_amount
      ELSE 0
      END AS op_vendor_funds_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.discount_terms_cost_amount
      ELSE 0
      END AS op_discount_terms_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.merch_margin_retail_amount
      ELSE 0
      END AS op_merch_margin_retail_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_in_dollar_amount
      ELSE 0
      END AS op_inventory_movement_in_dropship_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_in_units
      ELSE 0
      END AS op_inventory_movement_in_dropship_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_out_dollar_amount
      ELSE 0
      END AS op_inventory_movement_out_dropship_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_out_units
      ELSE 0
      END AS op_inventory_movement_out_dropship_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.other_inventory_adjustments_dollar_amount
      ELSE 0
      END AS op_other_inventory_adjustment_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('ORIGINAL_PLAN')
      THEN mfcbpf0.other_inventory_adjustments_units
      ELSE 0
      END AS op_other_inventory_adjustment_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_beginning_inventory_dollar_amount
      ELSE 0
      END AS sp_beginning_of_period_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_beginning_period_inventory_target_units
      ELSE 0
      END AS sp_beginning_of_period_active_inventory_target_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_beginning_inventory_units
      ELSE 0
      END AS sp_beginning_of_period_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_beginning_period_inventory_dollar_amount
      ELSE 0
      END AS sp_beginning_of_period_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_beginning_period_inventory_units
      ELSE 0
      END AS sp_beginning_of_period_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_ending_inventory_dollar_amount
      ELSE 0
      END AS sp_ending_of_period_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_ending_inventory_units
      ELSE 0
      END AS sp_ending_of_period_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_ending_inventory_dollar_amount
      ELSE 0
      END AS sp_ending_of_period_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_ending_inventory_units
      ELSE 0
      END AS sp_ending_of_period_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_inventory_in_dollar_amount
      ELSE 0
      END AS sp_transfer_in_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_inventory_in_units
      ELSE 0
      END AS sp_transfer_in_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_inventory_out_dollar_amount
      ELSE 0
      END AS sp_transfer_out_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_inventory_out_units
      ELSE 0
      END AS sp_transfer_out_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_inventory_in_dollar_amount
      ELSE 0
      END AS sp_transfer_in_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_inventory_in_units
      ELSE 0
      END AS sp_transfer_in_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_inventory_out_dollar_amount
      ELSE 0
      END AS sp_transfer_out_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_inventory_out_units
      ELSE 0
      END AS sp_transfer_out_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.other_inventory_in_dollar_amount
      ELSE 0
      END AS sp_transfer_in_other_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.other_inventory_in_units
      ELSE 0
      END AS sp_transfer_in_other_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.other_inventory_out_dollar_amount
      ELSE 0
      END AS sp_transfer_out_other_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.other_inventory_out_units
      ELSE 0
      END AS sp_transfer_out_other_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.racking_in_dollar_amount
      ELSE 0
      END AS sp_transfer_in_racking_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.racking_in_units
      ELSE 0
      END AS sp_transfer_in_racking_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.racking_out_dollar_amount
      ELSE 0
      END AS sp_transfer_out_racking_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.racking_out_units
      ELSE 0
      END AS sp_transfer_out_racking_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_mos_dollar_amount
      ELSE 0
      END AS sp_mark_out_of_stock_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_mos_units
      ELSE 0
      END AS sp_mark_out_of_stock_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_mos_dollar_amount
      ELSE 0
      END AS sp_mark_out_of_stock_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_mos_units
      ELSE 0
      END AS sp_mark_out_of_stock_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_receipts_dollar_amount
      ELSE 0
      END AS sp_receipts_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_receipts_units
      ELSE 0
      END AS sp_receipts_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_receipts_dollar_amount
      ELSE 0
      END AS sp_receipts_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_receipts_units
      ELSE 0
      END AS sp_receipts_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.reserve_receipts_dollar_amount
      ELSE 0
      END AS sp_receipts_reserve_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.reserve_receipts_units
      ELSE 0
      END AS sp_receipts_reserve_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.reclass_in_dollar_amount
      ELSE 0
      END AS sp_reclass_in_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.reclass_in_units
      ELSE 0
      END AS sp_reclass_in_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.reclass_out_dollar_amount
      ELSE 0
      END AS sp_reclass_out_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.reclass_out_units
      ELSE 0
      END AS sp_reclass_out_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_rtv_dollar_amount
      ELSE 0
      END AS sp_return_to_vendor_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_rtv_units
      ELSE 0
      END AS sp_return_to_vendor_active_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_rtv_dollar_amount
      ELSE 0
      END AS sp_return_to_vendor_inactive_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_rtv_units
      ELSE 0
      END AS sp_return_to_vendor_inactive_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.vendor_funds_cost_amount
      ELSE 0
      END AS sp_vendor_funds_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.discount_terms_cost_amount
      ELSE 0
      END AS sp_discount_terms_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.merch_margin_retail_amount
      ELSE 0
      END AS sp_merch_margin_retail_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_in_dollar_amount
      ELSE 0
      END AS sp_inventory_movement_in_dropship_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_in_units
      ELSE 0
      END AS sp_inventory_movement_in_dropship_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_out_dollar_amount
      ELSE 0
      END AS sp_inventory_movement_out_dropship_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inventory_movement_drop_ship_out_units
      ELSE 0
      END AS sp_inventory_movement_out_dropship_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.other_inventory_adjustments_dollar_amount
      ELSE 0
      END AS sp_other_inventory_adjustment_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.other_inventory_adjustments_units
      ELSE 0
      END AS sp_other_inventory_adjustment_qty,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.active_opentobuy_cost_amount
      ELSE 0
      END AS sp_open_to_buy_receipts_active_cost_amt,
      CASE
      WHEN LOWER(mfcbpf0.financial_plan_version) = LOWER('SNAP_PLAN')
      THEN mfcbpf0.inactive_opentobuy_cost_amount
      ELSE 0
      END AS sp_open_to_buy_receipts_inactive_cost_amt,
     0 AS ty_beginning_of_period_active_cost_amt,
     0 AS ty_beginning_of_period_active_qty,
     0 AS ty_beginning_of_period_inactive_cost_amt,
     0 AS ty_beginning_of_period_inactive_qty,
     0 AS ty_ending_of_period_active_cost_amt,
     0 AS ty_ending_of_period_active_qty,
     0 AS ty_ending_of_period_inactive_cost_amt,
     0 AS ty_ending_of_period_inactive_qty,
     0 AS ty_transfer_in_active_cost_amt,
     0 AS ty_transfer_in_active_qty,
     0 AS ty_transfer_out_active_cost_amt,
     0 AS ty_transfer_out_active_qty,
     0 AS ty_transfer_in_inactive_cost_amt,
     0 AS ty_transfer_in_inactive_qty,
     0 AS ty_transfer_out_inactive_cost_amt,
     0 AS ty_transfer_out_inactive_qty,
     0 AS ty_transfer_in_other_cost_amt,
     0 AS ty_transfer_in_other_qty,
     0 AS ty_transfer_out_other_cost_amt,
     0 AS ty_transfer_out_other_qty,
     0 AS ty_transfer_in_racking_cost_amt,
     0 AS ty_transfer_in_racking_qty,
     0 AS ty_transfer_out_racking_cost_amt,
     0 AS ty_transfer_out_racking_qty,
     0 AS ty_mark_out_of_stock_active_cost_amt,
     0 AS ty_mark_out_of_stock_active_qty,
     0 AS ty_mark_out_of_stock_inactive_cost_amt,
     0 AS ty_mark_out_of_stock_inactive_qty,
     0 AS ty_receipts_active_cost_amt,
     0 AS ty_receipts_active_qty,
     0 AS ty_receipts_inactive_cost_amt,
     0 AS ty_receipts_inactive_qty,
     0 AS ty_rp_receipts_active_cost_amt,
     0 AS ty_non_rp_receipts_active_cost_amt,
     0 AS ty_reclass_in_cost_amt,
     0 AS ty_reclass_in_qty,
     0 AS ty_reclass_out_cost_amt,
     0 AS ty_reclass_out_qty,
     0 AS ty_return_to_vendor_active_cost_amt,
     0 AS ty_return_to_vendor_active_qty,
     0 AS ty_return_to_vendor_inactive_cost_amt,
     0 AS ty_return_to_vendor_inactive_qty,
     0 AS ty_vendor_funds_cost_amt,
     0 AS ty_discount_terms_cost_amt,
     0 AS ty_merch_margin_retail_amt,
     0 AS ly_beginning_of_period_active_cost_amt,
     0 AS ly_beginning_of_period_active_qty,
     0 AS ly_beginning_of_period_inactive_cost_amt,
     0 AS ly_beginning_of_period_inactive_qty,
     0 AS ly_ending_of_period_active_cost_amt,
     0 AS ly_ending_of_period_active_qty,
     0 AS ly_ending_of_period_inactive_cost_amt,
     0 AS ly_ending_of_period_inactive_qty,
     0 AS ly_transfer_in_active_cost_amt,
     0 AS ly_transfer_in_active_qty,
     0 AS ly_transfer_out_active_cost_amt,
     0 AS ly_transfer_out_active_qty,
     0 AS ly_transfer_in_inactive_cost_amt,
     0 AS ly_transfer_in_inactive_qty,
     0 AS ly_transfer_out_inactive_cost_amt,
     0 AS ly_transfer_out_inactive_qty,
     0 AS ly_transfer_in_other_cost_amt,
     0 AS ly_transfer_in_other_qty,
     0 AS ly_transfer_out_other_cost_amt,
     0 AS ly_transfer_out_other_qty,
     0 AS ly_transfer_in_racking_cost_amt,
     0 AS ly_transfer_in_racking_qty,
     0 AS ly_transfer_out_racking_cost_amt,
     0 AS ly_transfer_out_racking_qty,
     0 AS ly_mark_out_of_stock_active_cost_amt,
     0 AS ly_mark_out_of_stock_active_qty,
     0 AS ly_mark_out_of_stock_inactive_cost_amt,
     0 AS ly_mark_out_of_stock_inactive_qty,
     0 AS ly_receipts_active_cost_amt,
     0 AS ly_receipts_active_qty,
     0 AS ly_receipts_inactive_cost_amt,
     0 AS ly_receipts_inactive_qty,
     0 AS ly_rp_receipts_active_cost_amt,
     0 AS ly_non_rp_receipts_active_cost_amt,
     0 AS ly_reclass_in_cost_amt,
     0 AS ly_reclass_in_qty,
     0 AS ly_reclass_out_cost_amt,
     0 AS ly_reclass_out_qty,
     0 AS ly_return_to_vendor_active_cost_amt,
     0 AS ly_return_to_vendor_active_qty,
     0 AS ly_return_to_vendor_inactive_cost_amt,
     0 AS ly_return_to_vendor_inactive_qty,
     0 AS ly_vendor_funds_cost_amt,
     0 AS ly_discount_terms_cost_amt,
     0 AS ly_merch_margin_retail_amt,
     0 AS lly_beginning_of_period_active_cost_amt,
     0 AS lly_beginning_of_period_active_qty,
     0 AS lly_beginning_of_period_inactive_cost_amt,
     0 AS lly_beginning_of_period_inactive_qty,
     0 AS lly_ending_of_period_active_cost_amt,
     0 AS lly_ending_of_period_active_qty,
     0 AS lly_ending_of_period_inactive_cost_amt,
     0 AS lly_ending_of_period_inactive_qty,
     0 AS lly_transfer_in_active_cost_amt,
     0 AS lly_transfer_in_active_qty,
     0 AS lly_transfer_out_active_cost_amt,
     0 AS lly_transfer_out_active_qty,
     0 AS lly_transfer_in_inactive_cost_amt,
     0 AS lly_transfer_in_inactive_qty,
     0 AS lly_transfer_out_inactive_cost_amt,
     0 AS lly_transfer_out_inactive_qty,
     0 AS lly_transfer_in_other_cost_amt,
     0 AS lly_transfer_in_other_qty,
     0 AS lly_transfer_out_other_cost_amt,
     0 AS lly_transfer_out_other_qty,
     0 AS lly_transfer_in_racking_cost_amt,
     0 AS lly_transfer_in_racking_qty,
     0 AS lly_transfer_out_racking_cost_amt,
     0 AS lly_transfer_out_racking_qty,
     0 AS lly_mark_out_of_stock_active_cost_amt,
     0 AS lly_mark_out_of_stock_active_qty,
     0 AS lly_mark_out_of_stock_inactive_cost_amt,
     0 AS lly_mark_out_of_stock_inactive_qty,
     0 AS lly_receipts_active_cost_amt,
     0 AS lly_receipts_active_qty,
     0 AS lly_receipts_inactive_cost_amt,
     0 AS lly_receipts_inactive_qty,
     0 AS lly_rp_receipts_active_cost_amt,
     0 AS lly_non_rp_receipts_active_cost_amt,
     0 AS lly_reclass_in_cost_amt,
     0 AS lly_reclass_in_qty,
     0 AS lly_reclass_out_cost_amt,
     0 AS lly_reclass_out_qty,
     0 AS lly_return_to_vendor_active_cost_amt,
     0 AS lly_return_to_vendor_active_qty,
     0 AS lly_return_to_vendor_inactive_cost_amt,
     0 AS lly_return_to_vendor_inactive_qty,
     0 AS lly_vendor_funds_cost_amt,
     0 AS lly_discount_terms_cost_amt,
     0 AS lly_merch_margin_retail_amt,
     0 AS ty_shrink_banner_cost_amt,
     0 AS ty_shrink_banner_qty,
     0 AS ty_other_inventory_adj_cost,
     0 AS ty_merch_margin_charges_cost,
     0 AS ly_shrink_banner_cost_amt,
     0 AS ly_shrink_banner_qty,
     0 AS ly_other_inventory_adj_cost,
     0 AS ly_merch_margin_charges_cost,
     0 AS lly_shrink_banner_cost_amt,
     0 AS lly_shrink_banner_qty,
     0 AS lly_other_inventory_adj_cost,
     0 AS lly_merch_margin_charges_cost,
     mtbv3.dw_batch_dt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_financial_country_banner_plan_fct AS mfcbpf0
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.org_banner_country_dim AS obcd0 ON LOWER(mfcbpf0.channel_country) = LOWER(obcd0.country_code
        ) AND LOWER(mfcbpf0.pricing_channel) = LOWER(obcd0.banner_desc)
     LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.alternate_inventory_model_dim AS aimd0 ON LOWER(mfcbpf0.alternate_inventory_model) =
      LOWER(aimd0.fulfill_type_desc)
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_tran_batch_vw AS mtbv3 ON LOWER(mtbv3.interface_code) = LOWER('MFP_BANR_BLEND_WKLY'
       )) AS a
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS c ON a.week_num = c.week_idnt
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l01 ON LOWER(l01.interface_code) = LOWER('MFP_BANR_BLEND_WKLY'
    )
 GROUP BY a.dept_num,
  a.week_num,
  a.banner_country_num,
  a.fulfill_type_num);