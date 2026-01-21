
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=ap_plan_act_fct;
---Task_Name=t1_ap_plan_act_supgrp_bnr_fact_daily_load;'*/
BEGIN TRANSACTION;

BEGIN
SET ERROR_CODE  =  0;

DELETE FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_ap_plan_actual_suppgrp_banner_fact
WHERE month_num > (SELECT last_completed_fiscal_month_num
        FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
        WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'));



INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_ap_plan_actual_suppgrp_banner_fact (supplier_group, dept_num, banner_num, channel_country
 , month_num, fiscal_year_num, planned_net_sales_total_retail_amount, planned_net_sales_total_cost_amount,
 actual_onorder_regular_retail_amount, actual_onorder_clearance_retail_amount, actual_onorder_total_retail_amount,
 actual_onorder_regular_cost_amount, actual_onorder_clearance_cost_amount, actual_onorder_total_cost_amount,
 actual_onorder_regular_units, actual_onorder_clearance_units, actual_onorder_total_units, planned_net_sales_total_units
 , planned_receipts_total_retail_amount, planned_receipts_total_cost_amount, planned_receipts_total_units,
 planned_inv_total_retail_amount, planned_inv_total_cost_amount, planned_inv_total_units, planned_inv_bop_total_units,
 planned_inv_bop_total_cost_amount, planned_rack_transfer_cost_amount, planned_rack_transfer_units,
 planned_rtv_total_cost_amount, planned_rtv_total_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT supplier_group,
  dept_num,
  banner_num,
  channel_country,
  month_num,
  fiscal_year_num,
  SUM(planned_net_sales_total_retail_amount),
  SUM(planned_net_sales_total_cost_amount),
  SUM(actual_onorder_regular_retail_amount),
  SUM(actual_onorder_clearance_retail_amount),
  SUM(actual_onorder_total_retail_amount),
  SUM(actual_onorder_regular_cost_amount),
  SUM(actual_onorder_clearance_cost_amount),
  SUM(actual_onorder_total_cost_amount),
  SUM(actual_onorder_regular_units),
  SUM(actual_onorder_clearance_units),
  SUM(actual_onorder_total_units),
  SUM(planned_net_sales_total_units),
  SUM(planned_receipts_total_retail_amount),
  SUM(planned_receipts_total_cost_amount),
  SUM(planned_receipts_total_units),
  SUM(planned_inv_total_retail_amount),
  SUM(planned_inv_total_cost_amount),
  SUM(planned_inv_total_units),
  SUM(planned_inv_bop_total_units),
  SUM(planned_inv_bop_total_cost_amount),
  SUM(planned_rack_transfer_cost_amount),
  SUM(planned_rack_transfer_units),
  SUM(planned_rtv_total_cost_amount),
  SUM(planned_rtv_total_units),
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk AS i01
 WHERE month_num > (SELECT last_completed_fiscal_month_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
 GROUP BY supplier_group,
  dept_num,
  banner_num,
  channel_country,
  month_num,
  fiscal_year_num);



/*SET QUERY_BAND = NONE FOR SESSION;*/
COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
ROLLBACK TRANSACTION;
RAISE USING MESSAGE = @@error.message;
END;
END;