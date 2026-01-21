
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=ap_plan_act_fct;
Task_Name=t0_1_ap_plan_act_wrk_plan_load;'*/


BEGIN
SET ERROR_CODE  =  0;

TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk;




INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country,
 month_num, fiscal_year_num, planned_net_sales_total_retail_amount, planned_net_sales_total_cost_amount,
 planned_net_sales_total_units, planned_receipts_total_retail_amount, planned_receipts_total_cost_amount,
 planned_receipts_total_units, planned_inv_total_retail_amount, planned_inv_total_cost_amount, planned_inv_total_units,
 planned_inv_bop_total_units, planned_inv_bop_total_cost_amount, planned_rack_transfer_cost_amount,
 planned_rack_transfer_units, planned_rtv_total_cost_amount, planned_rtv_total_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT p01.supplier_group,
  CAST(TRUNC(CAST(p01.department_number AS FLOAT64)) AS INTEGER) AS dept_num,
   CASE
   WHEN LOWER(p01.selling_brand) = LOWER('NORDSTROM') AND LOWER(p01.country) = LOWER('US')
   THEN 1
   WHEN LOWER(p01.selling_brand) = LOWER('NORDSTROM') AND LOWER(p01.country) = LOWER('CA')
   THEN 2
   WHEN LOWER(p01.selling_brand) = LOWER('NORDSTROM_RACK') AND LOWER(p01.country) = LOWER('US')
   THEN 3
   WHEN LOWER(p01.selling_brand) = LOWER('NORDSTROM_RACK') AND LOWER(p01.country) = LOWER('CA')
   THEN 4
   ELSE - 1
   END AS banner_num,
  SUBSTR(p01.country, 1, 10) AS channel_country,
  m01.month_num,
  m01.fiscal_year_num,
  SUM(p01.net_sales_retail_amt) AS planned_net_sales_total_retail_amount,
  SUM(p01.net_sales_cost_amt) AS planned_net_sales_total_cost_amount,
  SUM(p01.net_sales_units) AS planned_net_sales_total_units,
  SUM(p01.replenishment_receipt_retail_amt + p01.non_replenishment_receipt_retail_amt) AS
  planned_receipts_total_retail_amount,
  SUM(p01.replenishment_receipt_cost_amt + p01.non_replenishment_receipt_cost_amt) AS planned_receipts_total_cost_amount
  ,
  SUM(p01.replenishment_receipt_units + p01.non_replenishment_receipt_units) AS planned_receipts_total_units,
  SUM(p01.plannable_inventory_retail_amt) AS planned_inv_total_retail_amount,
  SUM(p01.plannable_inventory_cost_amt) AS planned_inv_total_cost_amount,
  SUM(p01.plannable_inventory_units) AS planned_inv_total_units,
  SUM(p01.beginning_of_period_inventory_units) AS beginning_of_period_inventory_units,
  SUM(p01.beginning_of_period_inventory_cost_amt) AS beginning_of_period_inventory_cost_amt,
  SUM(p01.rack_transfer_cost_amt) AS rack_transfer_cost_amt,
  SUM(p01.rack_transfer_units) AS rack_transfer_units,
  SUM(p01.return_to_vendor_cost_amt) AS return_to_vendor_cost_amt,
  SUM(p01.return_to_vendor_units) AS return_to_vendor_units,
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_assortment_suppgrp_cluster_plan_fact_vw AS p01
  INNER JOIN (SELECT DISTINCT fiscal_year_num,
    month_idnt AS month_num,
    month_label
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.day_cal_454_dim) AS m01 ON LOWER(p01.month_label) = LOWER(m01.month_label)
 WHERE p01.month_num >= (SELECT MIN(month_idnt)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
    WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 2
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
  AND p01.month_num <= (SELECT MAX(month_idnt)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
    WHERE fiscal_year_num = (SELECT current_fiscal_year_num + 1
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
 GROUP BY p01.supplier_group,
  dept_num,
  banner_num,
  channel_country,
  m01.month_num,
  m01.fiscal_year_num);


--ET;
/*SET QUERY_BAND = NONE FOR SESSION;*/
--ET;
EXCEPTION WHEN ERROR THEN
RAISE USING MESSAGE = @@error.message;
END;
END;