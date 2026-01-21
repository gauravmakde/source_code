--main script that when it runs, either everyday or weekly to update the table 

/*
ast_pipeline_prod_main_mfp_on_order_hist.sql
Author: Paria Avij
Date Created: 01/24/23

Datalab: t2dl_das_open_to_buy
Updates Tables:
    - merch_on_order_hist
*/







CREATE TEMPORARY TABLE IF NOT EXISTS merch_on_order_history_plans AS
SELECT cal.month_idnt AS snapshot_plan_month_idnt,
 cal.month_start_day_date AS snapshot_start_day_date,
 p.purchase_order_number,
 p.rms_sku_num,
 p.epm_sku_num,
 p.store_num,
 p.week_num,
 p.ship_location_id,
 p.rms_casepack_num,
 p.epm_casepack_num,
 p.order_from_vendor_id,
 p.anticipated_price_type,
 p.order_category_code,
 p.po_type,
 p.order_type,
 p.purchase_type,
 p.status,
 p.start_ship_date,
 p.end_ship_date,
 p.otb_eow_date,
 p.first_approval_date,
 p.latest_approval_date,
 p.first_approval_event_tmstp_pacific,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(p.first_approval_event_tmstp_pacific as string)) as first_approval_event_tmstp_pacific_tz,
 p.latest_approval_event_tmstp_pacific,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(cast(p.latest_approval_event_tmstp_pacific AS STRING)) as latest_approval_event_tmstp_pacific_tz,
 p.anticipated_retail_amt,
 p.total_expenses_per_unit_currency,
 p.quantity_ordered,
 p.quantity_canceled,
 p.quantity_received,
 p.quantity_open,
 p.unit_cost_amt,
 p.total_expenses_per_unit_amt,
 p.total_duty_per_unit_amt,
 p.unit_estimated_landing_cost,
 p.total_estimated_landing_cost,
 p.total_anticipated_retail_amt,
 p.dw_batch_date,
 p.dw_sys_load_tmstp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(p.dw_sys_load_tmstp AS STRING)) AS dw_sys_load_tmstp_tz,
 CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS rcd_update_timestamp,
 `{{params.gcp_project_id}}`.JWN_UDF.UDF_TIME_ZONE(CAST(CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS STRING)) AS rcd_update_timestamp_tz,
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.merch_on_order_fact_vw AS p
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS cal ON CURRENT_DATE('PST8PDT') = cal.day_date
WHERE p.week_num >= (SELECT DISTINCT week_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE day_date = CURRENT_DATE('PST8PDT'));


DELETE FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_on_order_hist_plans{{params.env_suffix}}
WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE day_date = CURRENT_DATE('PST8PDT'));


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_on_order_hist_plans{{params.env_suffix}}
(SELECT snapshot_plan_month_idnt,
  snapshot_start_day_date,
  purchase_order_number,
  rms_sku_num,
  epm_sku_num,
  store_num,
  week_num,
  ship_location_id,
  rms_casepack_num,
  epm_casepack_num,
  order_from_vendor_id,
  anticipated_price_type,
  order_category_code,
  po_type,
  order_type,
  purchase_type,
  status,
  start_ship_date,
  end_ship_date,
  otb_eow_date,
  first_approval_date,
  latest_approval_date,
  CAST(first_approval_event_tmstp_pacific AS TIMESTAMP ),
  first_approval_event_tmstp_pacific_tz,
  CAST(latest_approval_event_tmstp_pacific AS TIMESTAMP),
  latest_approval_event_tmstp_pacific_tz,
  anticipated_retail_amt,
  total_expenses_per_unit_currency,
  quantity_ordered,
  quantity_canceled,
  quantity_received,
  quantity_open,
  unit_cost_amt,
  total_expenses_per_unit_amt,
  total_duty_per_unit_amt,
  unit_estimated_landing_cost,
  total_estimated_landing_cost,
  total_anticipated_retail_amt,
  dw_batch_date,
  CAST(dw_sys_load_tmstp AS TIMESTAMP),
  dw_sys_load_tmstp_tz,
  CAST(rcd_update_timestamp AS TIMESTAMP) AS rcd_update_timestamp,
  rcd_update_timestamp_tz
 FROM merch_on_order_history_plans
 WHERE snapshot_plan_month_idnt = (SELECT DISTINCT month_idnt
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE day_date = CURRENT_DATE('PST8PDT')));