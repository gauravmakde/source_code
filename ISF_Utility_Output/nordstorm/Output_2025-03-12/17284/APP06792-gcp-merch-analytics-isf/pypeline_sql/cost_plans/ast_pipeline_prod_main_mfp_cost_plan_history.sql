/*
APT Cost Plan History DDL
Author: Sara Riker
Date Created: 11/29/22

Datalab: t2dl_das_open_to_buy
Updates Tables:
    - mfp_cost_plan_history
*/

-- Category/Cluster Plans

-- DROP TABLE cat_cluster_plans;
CREATE MULTISET VOLATILE TABLE mfp_banner_country_plans AS (
SELECT
     cal.month_idnt AS snapshot_plan_month_idnt
    ,cal.month_start_day_date AS snapshot_start_day_date
    ,dept_num 
    ,week_num 
    ,banner_country_num 
    ,fulfill_type_num 
    ,cp_beginning_of_period_active_cost_amt 
    ,cp_beginning_of_period_active_inventory_target_qty 
    ,cp_beginning_of_period_active_qty 
    ,cp_beginning_of_period_inactive_cost_amt 
    ,cp_beginning_of_period_inactive_qty 
    ,cp_ending_of_period_active_cost_amt 
    ,cp_ending_of_period_active_qty 
    ,cp_ending_of_period_inactive_cost_amt 
    ,cp_ending_of_period_inactive_qty 
    ,cp_transfer_in_active_cost_amt 
    ,cp_transfer_in_active_qty 
    ,cp_transfer_out_active_cost_amt 
    ,cp_transfer_out_active_qty 
    ,cp_transfer_in_inactive_cost_amt 
    ,cp_transfer_in_inactive_qty 
    ,cp_transfer_out_inactive_cost_amt 
    ,cp_transfer_out_inactive_qty 
    ,cp_transfer_in_other_cost_amt 
    ,cp_transfer_in_other_qty 
    ,cp_transfer_out_other_cost_amt 
    ,cp_transfer_out_other_qty 
    ,cp_transfer_in_racking_cost_amt 
    ,cp_transfer_in_racking_qty 
    ,cp_transfer_out_racking_cost_amt 
    ,cp_transfer_out_racking_qty 
    ,cp_mark_out_of_stock_active_cost_amt 
    ,cp_mark_out_of_stock_active_qty 
    ,cp_mark_out_of_stock_inactive_cost_amt 
    ,cp_mark_out_of_stock_inactive_qty 
    ,cp_receipts_active_cost_amt 
    ,cp_receipts_active_qty 
    ,cp_receipts_inactive_cost_amt 
    ,cp_receipts_inactive_qty 
    ,cp_receipts_reserve_cost_amt 
    ,cp_receipts_reserve_qty 
    ,cp_reclass_in_cost_amt 
    ,cp_reclass_in_qty 
    ,cp_reclass_out_cost_amt 
    ,cp_reclass_out_qty 
    ,cp_return_to_vendor_active_cost_amt 
    ,cp_return_to_vendor_active_qty 
    ,cp_return_to_vendor_inactive_cost_amt 
    ,cp_return_to_vendor_inactive_qty 
    ,cp_vendor_funds_cost_amt 
    ,cp_discount_terms_cost_amt 
    ,cp_merch_margin_retail_amt 
    ,cp_inventory_movement_in_dropship_cost_amt 
    ,cp_inventory_movement_in_dropship_qty 
    ,cp_inventory_movement_out_dropship_cost_amt 
    ,cp_inventory_movement_out_dropship_qty 
    ,cp_other_inventory_adjustment_cost_amt 
    ,cp_other_inventory_adjustment_qty 
    ,cp_open_to_buy_receipts_active_cost_amt 
    ,cp_open_to_buy_receipts_inactive_cost_amt 
    ,op_beginning_of_period_active_cost_amt 
    ,op_beginning_of_period_active_inventory_target_qty 
    ,op_beginning_of_period_active_qty 
    ,op_beginning_of_period_inactive_cost_amt 
    ,op_beginning_of_period_inactive_qty 
    ,op_ending_of_period_active_cost_amt 
    ,op_ending_of_period_active_qty 
    ,op_ending_of_period_inactive_cost_amt 
    ,op_ending_of_period_inactive_qty 
    ,op_transfer_in_active_cost_amt 
    ,op_transfer_in_active_qty 
    ,op_transfer_out_active_cost_amt 
    ,op_transfer_out_active_qty 
    ,op_transfer_in_inactive_cost_amt 
    ,op_transfer_in_inactive_qty 
    ,op_transfer_out_inactive_cost_amt 
    ,op_transfer_out_inactive_qty 
    ,op_transfer_in_other_cost_amt 
    ,op_transfer_in_other_qty 
    ,op_transfer_out_other_cost_amt 
    ,op_transfer_out_other_qty 
    ,op_transfer_in_racking_cost_amt 
    ,op_transfer_in_racking_qty 
    ,op_transfer_out_racking_cost_amt 
    ,op_transfer_out_racking_qty 
    ,op_mark_out_of_stock_active_cost_amt 
    ,op_mark_out_of_stock_active_qty 
    ,op_mark_out_of_stock_inactive_cost_amt 
    ,op_mark_out_of_stock_inactive_qty 
    ,op_receipts_active_cost_amt 
    ,op_receipts_active_qty 
    ,op_receipts_inactive_cost_amt 
    ,op_receipts_inactive_qty 
    ,op_receipts_reserve_cost_amt 
    ,op_receipts_reserve_qty 
    ,op_reclass_in_cost_amt 
    ,op_reclass_in_qty 
    ,op_reclass_out_cost_amt 
    ,op_reclass_out_qty 
    ,op_return_to_vendor_active_cost_amt 
    ,op_return_to_vendor_active_qty 
    ,op_return_to_vendor_inactive_cost_amt 
    ,op_return_to_vendor_inactive_qty 
    ,op_vendor_funds_cost_amt 
    ,op_discount_terms_cost_amt 
    ,op_merch_margin_retail_amt 
    ,op_inventory_movement_in_dropship_cost_amt 
    ,op_inventory_movement_in_dropship_qty 
    ,op_inventory_movement_out_dropship_cost_amt 
    ,op_inventory_movement_out_dropship_qty 
    ,op_other_inventory_adjustment_cost_amt 
    ,op_other_inventory_adjustment_qty 
    ,sp_beginning_of_period_active_cost_amt 
    ,sp_beginning_of_period_active_inventory_target_qty 
    ,sp_beginning_of_period_active_qty 
    ,sp_beginning_of_period_inactive_cost_amt 
    ,sp_beginning_of_period_inactive_qty 
    ,sp_ending_of_period_active_cost_amt 
    ,sp_ending_of_period_active_qty 
    ,sp_ending_of_period_inactive_cost_amt 
    ,sp_ending_of_period_inactive_qty 
    ,sp_transfer_in_active_cost_amt 
    ,sp_transfer_in_active_qty 
    ,sp_transfer_out_active_cost_amt 
    ,sp_transfer_out_active_qty 
    ,sp_transfer_in_inactive_cost_amt 
    ,sp_transfer_in_inactive_qty 
    ,sp_transfer_out_inactive_cost_amt 
    ,sp_transfer_out_inactive_qty 
    ,sp_transfer_in_other_cost_amt 
    ,sp_transfer_in_other_qty 
    ,sp_transfer_out_other_cost_amt 
    ,sp_transfer_out_other_qty 
    ,sp_transfer_in_racking_cost_amt 
    ,sp_transfer_in_racking_qty 
    ,sp_transfer_out_racking_cost_amt 
    ,sp_transfer_out_racking_qty 
    ,sp_mark_out_of_stock_active_cost_amt 
    ,sp_mark_out_of_stock_active_qty 
    ,sp_mark_out_of_stock_inactive_cost_amt 
    ,sp_mark_out_of_stock_inactive_qty 
    ,sp_receipts_active_cost_amt 
    ,sp_receipts_active_qty 
    ,sp_receipts_inactive_cost_amt 
    ,sp_receipts_inactive_qty 
    ,sp_receipts_reserve_cost_amt 
    ,sp_receipts_reserve_qty 
    ,sp_reclass_in_cost_amt 
    ,sp_reclass_in_qty 
    ,sp_reclass_out_cost_amt 
    ,sp_reclass_out_qty 
    ,sp_return_to_vendor_active_cost_amt 
    ,sp_return_to_vendor_active_qty 
    ,sp_return_to_vendor_inactive_cost_amt 
    ,sp_return_to_vendor_inactive_qty 
    ,sp_vendor_funds_cost_amt 
    ,sp_discount_terms_cost_amt 
    ,sp_merch_margin_retail_amt 
    ,sp_inventory_movement_in_dropship_cost_amt 
    ,sp_inventory_movement_in_dropship_qty 
    ,sp_inventory_movement_out_dropship_cost_amt 
    ,sp_inventory_movement_out_dropship_qty 
    ,sp_other_inventory_adjustment_cost_amt 
    ,sp_other_inventory_adjustment_qty 
    ,sp_open_to_buy_receipts_active_cost_amt 
    ,sp_open_to_buy_receipts_inactive_cost_amt 
    ,dw_batch_date
    ,dw_sys_load_tmstp 
    ,CURRENT_TIMESTAMP AS rcd_update_timestamp
FROM prd_nap_vws.mfp_cost_plan_actual_banner_country_fact p
JOIN prd_nap_usr_vws.day_cal_454_dim cal
  ON current_date = cal.day_date
WHERE week_num >= (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
)
WITH DATA
PRIMARY INDEX (snapshot_plan_month_idnt, week_num, dept_num, banner_country_num)
ON COMMIT PRESERVE ROWS
;


DELETE FROM {environment_schema}.mfp_cost_plan_actual_banner_country_history{env_suffix} WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date);

INSERT INTO {environment_schema}.mfp_cost_plan_actual_banner_country_history{env_suffix}
SELECT * 
FROM mfp_banner_country_plans
WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
;


/*
APT Cost Plan History DDL
Author: Sara Riker
Date Created: 11/29/22

Datalab: t2dl_das_open_to_buy
Updates Tables:
    - mfp_cost_plan_history
*/

-- Category/Cluster Plans

-- DROP TABLE cat_cluster_plans;
CREATE MULTISET VOLATILE TABLE mfp_channel_plans AS (
SELECT
     cal.month_idnt AS snapshot_plan_month_idnt
    ,cal.month_start_day_date AS snapshot_start_day_date
    ,dept_num
    ,week_num
    ,channel_num
    ,fulfill_type_num
    ,cp_demand_flash_retail_amt
    ,cp_demand_persistent_retail_amt
    ,cp_demand_total_qty
    ,cp_demand_total_retail_amt
    ,cp_gross_margin_retail_amt
    ,cp_gross_sales_retail_amt
    ,cp_gross_sales_qty
    ,cp_net_sales_retail_amt
    ,cp_net_sales_qty
    ,cp_net_sales_cost_amt
    ,cp_net_sales_trunk_club_retail_amt
    ,cp_net_sales_trunk_club_qty
    ,cp_returns_retail_amt
    ,cp_returns_qty
    ,cp_shrink_cost_amt
    ,cp_shrink_qty
    ,op_demand_flash_retail_amt
    ,op_demand_persistent_retail_amt
    ,op_demand_total_qty
    ,op_demand_total_retail_amt
    ,op_gross_margin_retail_amt
    ,op_gross_sales_retail_amt
    ,op_gross_sales_qty
    ,op_net_sales_retail_amt
    ,op_net_sales_qty
    ,op_net_sales_cost_amt
    ,op_net_sales_trunk_club_retail_amt
    ,op_net_sales_trunk_club_qty
    ,op_returns_retail_amt
    ,op_returns_qty
    ,op_shrink_cost_amt
    ,op_shrink_qty
    ,sp_demand_flash_retail_amt
    ,sp_demand_persistent_retail_amt
    ,sp_demand_total_qty
    ,sp_demand_total_retail_amt
    ,sp_gross_margin_retail_amt
    ,sp_gross_sales_retail_amt
    ,sp_gross_sales_qty
    ,sp_net_sales_retail_amt
    ,sp_net_sales_qty
    ,sp_net_sales_cost_amt
    ,sp_net_sales_trunk_club_retail_amt
    ,sp_net_sales_trunk_club_qty
    ,sp_returns_retail_amt
    ,sp_returns_qty
    ,sp_shrink_cost_amt
    ,sp_shrink_qty
    ,ty_demand_flash_retail_amt
    ,ty_demand_persistent_retail_amt
    ,ty_demand_total_qty
    ,ty_demand_total_retail_amt
    ,ty_gross_margin_retail_amt
    ,ty_gross_sales_retail_amt
    ,ty_gross_sales_qty
    ,ty_net_sales_retail_amt
    ,ty_net_sales_qty
    ,ty_net_sales_cost_amt
    ,ty_net_sales_trunk_club_retail_amt
    ,ty_net_sales_trunk_club_qty
    ,ty_returns_retail_amt
    ,ty_returns_qty
    ,ty_shrink_cost_amt
    ,ty_shrink_qty
    ,ly_demand_flash_retail_amt
    ,ly_demand_persistent_retail_amt
    ,ly_demand_total_qty
    ,ly_demand_total_retail_amt
    ,ly_gross_margin_retail_amt
    ,ly_gross_sales_retail_amt
    ,ly_gross_sales_qty
    ,ly_net_sales_retail_amt
    ,ly_net_sales_qty
    ,ly_net_sales_cost_amt
    ,ly_net_sales_trunk_club_retail_amt
    ,ly_net_sales_trunk_club_qty
    ,ly_returns_retail_amt
    ,ly_returns_qty
    ,ly_shrink_cost_amt
    ,ly_shrink_qty
    ,lly_demand_flash_retail_amt
    ,lly_demand_persistent_retail_amt
    ,lly_demand_total_qty
    ,lly_demand_total_retail_amt
    ,lly_gross_margin_retail_amt
    ,lly_gross_sales_retail_amt
    ,lly_gross_sales_qty
    ,lly_net_sales_retail_amt
    ,lly_net_sales_qty
    ,lly_net_sales_cost_amt
    ,lly_net_sales_trunk_club_retail_amt
    ,lly_net_sales_trunk_club_qty
    ,lly_returns_retail_amt
    ,lly_returns_qty
    ,lly_shrink_cost_amt
    ,lly_shrink_qty
    ,dw_batch_date
    ,dw_sys_load_tmstp
    ,CURRENT_TIMESTAMP AS rcd_update_timestamp
FROM prd_nap_vws.mfp_cost_plan_actual_channel_fact p
JOIN prd_nap_usr_vws.day_cal_454_dim cal
  ON current_date = cal.day_date
WHERE week_num >= (SELECT DISTINCT week_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
)
WITH DATA
PRIMARY INDEX (snapshot_plan_month_idnt, week_num, dept_num, channel_num)
ON COMMIT PRESERVE ROWS
;


DELETE FROM {environment_schema}.mfp_cost_plan_actual_channel_history{env_suffix} WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date);

INSERT INTO {environment_schema}.mfp_cost_plan_actual_channel_history{env_suffix}
SELECT * 
FROM mfp_channel_plans
WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt FROM prd_nap_usr_vws.day_cal_454_dim WHERE day_date = current_date)
;
