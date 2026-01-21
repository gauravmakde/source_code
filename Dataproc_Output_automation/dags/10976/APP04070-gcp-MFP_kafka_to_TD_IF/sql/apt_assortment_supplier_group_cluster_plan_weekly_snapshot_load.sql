BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=apt_assortment_supplier_group_cluster_plan_weekly_snapshot_load;
---Task_Name=load_weekly_snapshot;'*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
BEGIN
SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_suppgrp_cluster_plan_snapshot_fact AS tgt
WHERE snapshot_week_id = (SELECT week_idnt AS snapshot_week_id
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
        WHERE day_date < CURRENT_DATE('PST8PDT') AND day_num_of_fiscal_week = 7
        QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
BEGIN
SET _ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_suppgrp_cluster_plan_snapshot_fact (snapshot_week_id, event_time,event_time_tz,
 plan_published_date, supplier_group_id, supplier_group, department_num, country, cluster_name, category, month_num,
 month_label, last_updated_time_in_millis, last_updated_time, last_updated_time_tz,alternate_inventory_model, replenishment_receipt_units,
 replenishment_receipt_retail_currcycd, replenishment_receipt_retail_amt, replenishment_receipt_cost_currcycd,
 replenishment_receipt_cost_amt, replenishment_receipt_less_reserve_units,
 replenishment_receipt_less_reserve_retail_currcycd, replenishment_receipt_less_reserve_retail_amt,
 replenishment_receipt_less_reserve_cost_currcycd, replenishment_receipt_less_reserve_cost_amt,
 non_replenishment_receipt_units, non_replenishment_receipt_retail_currcycd, non_replenishment_receipt_retail_amt,
 non_replenishment_receipt_cost_currcycd, non_replenishment_receipt_cost_amt,
 non_replenishment_receipt_less_reserve_units, non_replenishment_receipt_less_reserve_retail_currcycd,
 non_replenishment_receipt_less_reserve_retail_amt, non_replenishment_receipt_less_reserve_cost_currcycd,
 non_replenishment_receipt_less_reserve_cost_amt, drop_ship_receipt_units, drop_ship_receipt_retail_currcycd,
 drop_ship_receipt_retail_amt, drop_ship_receipt_cost_currcycd, drop_ship_receipt_cost_amt, average_inventory_units,
 average_inventory_retail_currcycd, average_inventory_retail_amt, average_inventory_cost_currcycd,
 average_inventory_cost_amt, beginning_of_period_inventory_units, beginning_of_period_inventory_retail_currcycd,
 beginning_of_period_inventory_retail_amt, beginning_of_period_inventory_cost_currcycd,
 beginning_of_period_inventory_cost_amt, beginning_of_period_inventory_target_units,
 beginning_of_period_inventory_target_retail_currcycd, beginning_of_period_inventory_target_retail_amt,
 beginning_of_period_inventory_target_cost_currcycd, beginning_of_period_inventory_target_cost_amt,
 return_to_vendor_units, return_to_vendor_retail_currcycd, return_to_vendor_retail_amt, return_to_vendor_cost_currcycd,
 return_to_vendor_cost_amt, rack_transfer_units, rack_transfer_retail_currcycd, rack_transfer_retail_amt,
 rack_transfer_cost_currcycd, rack_transfer_cost_amt, active_inventory_in_units, active_inventory_in_retail_currcycd,
 active_inventory_in_retail_amt, active_inventory_in_cost_currcycd, active_inventory_in_cost_amt,
 plannable_inventory_units, plannable_inventory_retail_currcycd, plannable_inventory_retail_amt,
 plannable_inventory_cost_currcycd, plannable_inventory_cost_amt, plannable_inventory_receipt_less_reserve_units,
 plannable_inventory_receipt_less_reserve_retail_currcycd, plannable_inventory_receipt_less_reserve_retail_amt,
 plannable_inventory_receipt_less_reserve_cost_currcycd, plannable_inventory_receipt_less_reserve_cost_amt, shrink_units
 , shrink_retail_currcycd, shrink_retail_amt, shrink_cost_currcycd, shrink_cost_amt, net_sales_units,
 net_sales_retail_currcycd, net_sales_retail_amt, net_sales_cost_currcycd, net_sales_cost_amt, demand_units,
 demand_dollar_currcycd, demand_dollar_amt, gross_sales_units, gross_sales_dollar_currcycd, gross_sales_dollar_amt,
 returns_units, returns_dollar_currcycd, returns_dollar_amt, product_margin_retail_currcycd, product_margin_retail_amt,
 demand_next_two_month_run_rate, sales_next_two_month_run_rate, dw_sys_load_tmstp)
(SELECT dcd.snapshot_week_id,
  cast(fct.event_time as timestamp),
  fct.event_time_tz,
  fct.plan_published_date,
  fct.supp_group_id,
  COALESCE(dm.supplier_group, 'UNKNOWN') AS supplier_group,
  COALESCE(dm.department_number, - 1) AS department_num,
  fct.country,
  fct.cluster_name,
  fct.category,
  fct.month_num,
  fct.month_label,
  fct.last_updated_time_in_millis,
  cast(fct.last_updated_time as timestamp),
  fct.last_updated_time_tz,
  fct.alternate_inventory_model,
  fct.replenishment_receipt_units,
  fct.replenishment_receipt_retail_currcycd,
  fct.replenishment_receipt_retail_amt,
  fct.replenishment_receipt_cost_currcycd,
  fct.replenishment_receipt_cost_amt,
  fct.replenishment_receipt_less_reserve_units,
  fct.replenishment_receipt_less_reserve_retail_currcycd,
  fct.replenishment_receipt_less_reserve_retail_amt,
  fct.replenishment_receipt_less_reserve_cost_currcycd,
  fct.replenishment_receipt_less_reserve_cost_amt,
  fct.non_replenishment_receipt_units,
  fct.non_replenishment_receipt_retail_currcycd,
  fct.non_replenishment_receipt_retail_amt,
  fct.non_replenishment_receipt_cost_currcycd,
  fct.non_replenishment_receipt_cost_amt,
  fct.non_replenishment_receipt_less_reserve_units,
  fct.non_replenishment_receipt_less_reserve_retail_currcycd,
  fct.non_replenishment_receipt_less_reserve_retail_amt,
  fct.non_replenishment_receipt_less_reserve_cost_currcycd,
  fct.non_replenishment_receipt_less_reserve_cost_amt,
  fct.drop_ship_receipt_units,
  fct.drop_ship_receipt_retail_currcycd,
  fct.drop_ship_receipt_retail_amt,
  fct.drop_ship_receipt_cost_currcycd,
  fct.drop_ship_receipt_cost_amt,
  fct.average_inventory_units,
  fct.average_inventory_retail_currcycd,
  fct.average_inventory_retail_amt,
  fct.average_inventory_cost_currcycd,
  fct.average_inventory_cost_amt,
  fct.beginning_of_period_inventory_units,
  fct.beginning_of_period_inventory_retail_currcycd,
  fct.beginning_of_period_inventory_retail_amt,
  fct.beginning_of_period_inventory_cost_currcycd,
  fct.beginning_of_period_inventory_cost_amt,
  fct.beginning_of_period_inventory_target_units,
  fct.beginning_of_period_inventory_target_retail_currcycd,
  fct.beginning_of_period_inventory_target_retail_amt,
  fct.beginning_of_period_inventory_target_cost_currcycd,
  fct.beginning_of_period_inventory_target_cost_amt,
  fct.return_to_vendor_units,
  fct.return_to_vendor_retail_currcycd,
  fct.return_to_vendor_retail_amt,
  fct.return_to_vendor_cost_currcycd,
  fct.return_to_vendor_cost_amt,
  fct.rack_transfer_units,
  fct.rack_transfer_retail_currcycd,
  fct.rack_transfer_retail_amt,
  fct.rack_transfer_cost_currcycd,
  fct.rack_transfer_cost_amt,
  fct.active_inventory_in_units,
  fct.active_inventory_in_retail_currcycd,
  fct.active_inventory_in_retail_amt,
  fct.active_inventory_in_cost_currcycd,
  fct.active_inventory_in_cost_amt,
  fct.plannable_inventory_units,
  fct.plannable_inventory_retail_currcycd,
  fct.plannable_inventory_retail_amt,
  fct.plannable_inventory_cost_currcycd,
  fct.plannable_inventory_cost_amt,
  fct.plannable_inventory_receipt_less_reserve_units,
  fct.plannable_inventory_receipt_less_reserve_retail_currcycd,
  fct.plannable_inventory_receipt_less_reserve_retail_amt,
  fct.plannable_inventory_receipt_less_reserve_cost_currcycd,
  fct.plannable_inventory_receipt_less_reserve_cost_amt,
  fct.shrink_units,
  fct.shrink_retail_currcycd,
  fct.shrink_retail_amt,
  fct.shrink_cost_currcycd,
  fct.shrink_cost_amt,
  fct.net_sales_units,
  fct.net_sales_retail_currcycd,
  fct.net_sales_retail_amt,
  fct.net_sales_cost_currcycd,
  fct.net_sales_cost_amt,
  fct.demand_units,
  fct.demand_dollar_currcycd,
  fct.demand_dollar_amt,
  fct.gross_sales_units,
  fct.gross_sales_dollar_currcycd,
  fct.gross_sales_dollar_amt,
  fct.returns_units,
  fct.returns_dollar_currcycd,
  fct.returns_dollar_amt,
  fct.product_margin_retail_currcycd,
  fct.product_margin_retail_amt,
  fct.demand_next_two_month_run_rate,
  fct.sales_next_two_month_run_rate,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_assortment_suppgrp_cluster_plan_fact AS fct
  INNER JOIN (SELECT week_idnt AS snapshot_week_id,
    month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim
   WHERE day_date < CURRENT_DATE('PST8PDT')
    AND day_num_of_fiscal_week = 7
   QUALIFY (ROW_NUMBER() OVER (ORDER BY day_date DESC)) = 1) AS dcd ON fct.month_num >= dcd.month_idnt
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_supplier_groupid_assignment_dim AS dm ON LOWER(fct.supp_group_id) = LOWER(dm.supplier_group_id
    ));


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;


BEGIN
SET _ERROR_CODE  =  0;


DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_assortment_suppgrp_cluster_plan_snapshot_fact AS sf
WHERE snapshot_week_id <= (SELECT retention_week_end_idnt
        FROM (SELECT month_start_day_date, month_end_day_date, month_end_week_idnt, MIN(month_end_week_idnt) OVER (ORDER BY month_idnt ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS retention_week_end_idnt
                FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_month_cal_454_vw) AS a
        WHERE CURRENT_DATE('PST8PDT') BETWEEN month_start_day_date AND month_end_day_date);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
/*SET QUERY_BAND = NONE FOR SESSION;*/
-- COMMIT TRANSACTION;
-- EXCEPTION WHEN ERROR THEN
-- ROLLBACK TRANSACTION;
-- RAISE USING MESSAGE = @@error.message;
END;
