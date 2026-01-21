/*
APT Cost Plan History DDL
Author: Sara Riker
Date Created: 9/29/22
Date Last Updated: 9/29/22

Datalab: t2dl_das_apt_cost_reporting
Updates Tables:
    - merch_assortment_supplier_cluster_plan_history_weekly
*/

-- Supplier Cluster Plans


--DROP TABLE IF EXISTS supp_cluster_plans;


CREATE TEMPORARY TABLE IF NOT EXISTS supp_cluster_plans
AS
SELECT cal.month_idnt AS snapshot_plan_month_idnt,
 cal.week_idnt AS snapshot_plan_week_idnt,
 cal.week_start_day_date AS snapshot_start_day_date,
  CASE
  WHEN cal.week_num_of_fiscal_month = 3 
  OR cal.month_idnt = 202212 
  AND cal.week_num_of_fiscal_month = 4
  THEN 1
  ELSE 0
  END AS replan_week,
 CAST(TRUNC(CAST(CONCAT('20', SUBSTR(REGEXP_EXTRACT_ALL(p.month_id, '[^FY]+')[SAFE_OFFSET(1)], 0, 2), CASE
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('FEB')
    THEN '01'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('MAR')
    THEN '02'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('APR')
    THEN '03'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('MAY')
    THEN '04'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('JUN')
    THEN '05'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('JUL')
    THEN '06'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('AUG')
    THEN '07'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('SEP')
    THEN '08'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('OCT')
    THEN '09'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('NOV')
    THEN '10'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('DEC')
    THEN '11'
    WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('JAN')
    THEN '12'
    ELSE NULL
    END) AS FLOAT64)) AS INTEGER) AS fiscal_month_idnt,
 p.event_time,
 p.selling_country,
 p.selling_brand,
 p.cluster_name,
 p.category,
 p.supplier_group,
 p.department_number AS dept_idnt,
 p.month_id,
 p.alternate_inventory_model,
 p.demand_dollar_currency_code,
 p.demand_dollar_amount,
 p.demand_units,
 p.gross_sales_dollar_currency_code,
 p.gross_sales_dollar_amount,
 p.gross_sales_units,
 p.returns_dollar_currency_code,
 p.returns_dollar_amount,
 p.returns_units,
 p.net_sales_units,
 p.net_sales_retail_currency_code,
 p.net_sales_retail_amount,
 p.net_sales_cost_currency_code,
 p.net_sales_cost_amount,
 p.product_margin_retail_currency_code,
 p.product_margin_retail_amount,
 p.demand_next_two_month_run_rate,
 p.sales_next_two_month_run_rate,
 p.replenishment_receipts_units,
 p.replenishment_receipts_retail_currency_code,
 p.replenishment_receipts_retail_amount,
 p.replenishment_receipts_cost_currency_code,
 p.replenishment_receipts_cost_amount,
 p.replenishment_receipts_less_reserve_units,
 p.replenishment_receipts_less_reserve_retail_currency_code,
 p.replenishment_receipts_less_reserve_retail_amount,
 p.replenishment_receipts_less_reserve_cost_currency_code,
 p.replenishment_receipts_less_reserve_cost_amount,
 p.nonreplenishment_receipts_units,
 p.nonreplenishment_receipts_retail_currency_code,
 p.nonreplenishment_receipts_retail_amount,
 p.nonreplenishment_receipts_cost_currency_code,
 p.nonreplenishment_receipts_cost_amount,
 p.nonreplenishment_receipts_less_reserve_units,
 p.nonreplenishment_receipts_less_reserve_retail_currency_code,
 p.nonreplenishment_receipts_less_reserve_retail_amount,
 p.nonreplenishment_receipts_less_reserve_cost_currency_code,
 p.nonreplenishment_receipts_less_reserve_cost_amount,
 p.dropship_receipt_units,
 p.dropship_receipt_retail_currency_code,
 p.dropship_receipt_retail_amount,
 p.dropship_receipt_cost_currency_code,
 p.dropship_receipt_cost_amount,
 p.average_inventory_units,
 p.average_inventory_retail_currency_code,
 p.average_inventory_retail_amount,
 p.average_inventory_cost_currency_code,
 p.average_inventory_cost_amount,
 p.beginning_of_period_inventory_units,
 p.beginning_of_period_inventory_retail_currency_code,
 p.beginning_of_period_inventory_retail_amount,
 p.beginning_of_period_inventory_cost_currency_code,
 p.beginning_of_period_inventory_cost_amount,
 p.beginning_of_period_inventory_target_units,
 p.beginning_of_period_inventory_target_retail_currency_code,
 p.beginning_of_period_inventory_target_retail_amount,
 p.beginning_of_period_inventory_target_cost_currency_code,
 p.beginning_of_period_inventory_target_cost_amount,
 p.return_to_vendor_units,
 p.return_to_vendor_retail_currency_code,
 p.return_to_vendor_retail_amount,
 p.return_to_vendor_cost_currency_code,
 p.return_to_vendor_cost_amount,
 p.rack_transfer_units,
 p.rack_transfer_retail_currency_code,
 p.rack_transfer_retail_amount,
 p.rack_transfer_cost_currency_code,
 p.rack_transfer_cost_amount,
 p.active_inventory_in_units,
 p.active_inventory_in_retail_currency_code,
 p.active_inventory_in_retail_amount,
 p.active_inventory_in_cost_currency_code,
 p.active_inventory_in_cost_amount,
 p.plannable_inventory_units,
 p.plannable_inventory_retail_currency_code,
 p.plannable_inventory_retail_amount,
 p.plannable_inventory_cost_currency_code,
 p.plannable_inventory_cost_amount,
 p.plannable_inventory_receipt_less_reserve_units,
 p.plannable_inventory_receipt_less_reserve_retail_currency_code,
 p.plannable_inventory_receipt_less_reserve_retail_amount,
 p.plannable_inventory_receipt_less_reserve_cost_currency_code,
 p.plannable_inventory_receipt_less_reserve_cost_amount,
 p.shrink_units,
 p.shrink_retail_currency_code,
 p.shrink_retail_amount,
 p.shrink_cost_currency_code,
 p.shrink_cost_amount,
 cast(p.dw_sys_load_tmstp as timestamp) AS quantrix_update,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(p.dw_sys_load_tmstp as string)) AS quantrix_update_tz,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS Timestamp) AS rcd_update_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as rcd_update_timestamp_tz,
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_reporting.merch_assortment_supplier_cluster_plan_fact AS p
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal 
 ON CURRENT_DATE('PST8PDT') = cal.day_date
WHERE CAST(TRUNC(CAST((CONCAT('20', SUBSTR(REGEXP_EXTRACT_ALL(p.month_id, '[^FY]+')[SAFE_OFFSET(1)], 0, 2), CASE
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('FEB')
     THEN '01'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('MAR')
     THEN '02'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('APR')
     THEN '03'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('MAY')
     THEN '04'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('JUN')
     THEN '05'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('JUL')
     THEN '06'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('AUG')
     THEN '07'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('SEP')
     THEN '08'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('OCT')
     THEN '09'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('NOV')
     THEN '10'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('DEC')
     THEN '11'
     WHEN LOWER(SUBSTR(p.month_id, 0, 3)) = LOWER('JAN')
     THEN '12'
     ELSE NULL
     END)) AS FLOAT64)) AS INTEGER) >= (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'));


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_assortment_supplier_cluster_plan_history_weekly
WHERE snapshot_plan_week_idnt = (SELECT DISTINCT week_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE('PST8PDT'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_assortment_supplier_cluster_plan_history_weekly
(SELECT snapshot_plan_month_idnt,
  snapshot_plan_week_idnt,
  snapshot_start_day_date,
  replan_week,
  fiscal_month_idnt,
  event_time,
  selling_country,
  selling_brand,
  cluster_name,
  category,
  supplier_group,
  CAST(TRUNC(CAST(dept_idnt AS FLOAT64)) AS INTEGER) AS dept_idnt,
  month_id,
  alternate_inventory_model,
  demand_dollar_currency_code,
  demand_dollar_amount,
  demand_units,
  gross_sales_dollar_currency_code,
  gross_sales_dollar_amount,
  gross_sales_units,
  returns_dollar_currency_code,
  returns_dollar_amount,
  returns_units,
  net_sales_units,
  net_sales_retail_currency_code,
  net_sales_retail_amount,
  net_sales_cost_currency_code,
  net_sales_cost_amount,
  product_margin_retail_currency_code,
  product_margin_retail_amount,
  demand_next_two_month_run_rate,
  sales_next_two_month_run_rate,
  replenishment_receipts_units,
  replenishment_receipts_retail_currency_code,
  replenishment_receipts_retail_amount,
  replenishment_receipts_cost_currency_code,
  replenishment_receipts_cost_amount,
  replenishment_receipts_less_reserve_units,
  replenishment_receipts_less_reserve_retail_currency_code,
  replenishment_receipts_less_reserve_retail_amount,
  replenishment_receipts_less_reserve_cost_currency_code,
  replenishment_receipts_less_reserve_cost_amount,
  nonreplenishment_receipts_units,
  nonreplenishment_receipts_retail_currency_code,
  nonreplenishment_receipts_retail_amount,
  nonreplenishment_receipts_cost_currency_code,
  nonreplenishment_receipts_cost_amount,
  nonreplenishment_receipts_less_reserve_units,
  nonreplenishment_receipts_less_reserve_retail_currency_code,
  nonreplenishment_receipts_less_reserve_retail_amount,
  nonreplenishment_receipts_less_reserve_cost_currency_code,
  nonreplenishment_receipts_less_reserve_cost_amount,
  dropship_receipt_units,
  dropship_receipt_retail_currency_code,
  dropship_receipt_retail_amount,
  dropship_receipt_cost_currency_code,
  dropship_receipt_cost_amount,
  average_inventory_units,
  average_inventory_retail_currency_code,
  average_inventory_retail_amount,
  average_inventory_cost_currency_code,
  average_inventory_cost_amount,
  beginning_of_period_inventory_units,
  beginning_of_period_inventory_retail_currency_code,
  beginning_of_period_inventory_retail_amount,
  beginning_of_period_inventory_cost_currency_code,
  beginning_of_period_inventory_cost_amount,
  beginning_of_period_inventory_target_units,
  beginning_of_period_inventory_target_retail_currency_code,
  beginning_of_period_inventory_target_retail_amount,
  beginning_of_period_inventory_target_cost_currency_code,
  beginning_of_period_inventory_target_cost_amount,
  return_to_vendor_units,
  return_to_vendor_retail_currency_code,
  return_to_vendor_retail_amount,
  return_to_vendor_cost_currency_code,
  return_to_vendor_cost_amount,
  rack_transfer_units,
  rack_transfer_retail_currency_code,
  rack_transfer_retail_amount,
  rack_transfer_cost_currency_code,
  rack_transfer_cost_amount,
  active_inventory_in_units,
  active_inventory_in_retail_currency_code,
  active_inventory_in_retail_amount,
  active_inventory_in_cost_currency_code,
  active_inventory_in_cost_amount,
  plannable_inventory_units,
  plannable_inventory_retail_currency_code,
  plannable_inventory_retail_amount,
  plannable_inventory_cost_currency_code,
  plannable_inventory_cost_amount,
  plannable_inventory_receipt_less_reserve_units,
  plannable_inventory_receipt_less_reserve_retail_currency_code,
  plannable_inventory_receipt_less_reserve_retail_amount,
  plannable_inventory_receipt_less_reserve_cost_currency_code,
  plannable_inventory_receipt_less_reserve_cost_amount,
  shrink_units,
  shrink_retail_currency_code,
  shrink_retail_amount,
  shrink_cost_currency_code,
  shrink_cost_amount,
  quantrix_update,
  quantrix_update_tz,
  CAST(rcd_update_timestamp AS TIMESTAMP) AS rcd_update_timestamp,
 rcd_update_timestamp_tz
 FROM supp_cluster_plans
 WHERE snapshot_plan_week_idnt = (SELECT DISTINCT week_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE('PST8PDT')));