BEGIN
DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=ap_plan_act_fct;
---Task_Name=t0_3_ap_plan_act_wrk_rcpt_load;'*/
BEGIN
SET _ERROR_CODE  =  0;
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country,
 month_num, fiscal_year_num, actual_receipts_total_retail_amount, actual_receipts_total_cost_amount,
 actual_receipts_regular_retail_amount, actual_receipts_clearance_retail_amount, actual_receipts_regular_cost_amount,
 actual_receipts_clearance_cost_amount, actual_receipts_regular_units, actual_receipts_clearance_units,
 actual_receipts_total_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT COALESCE(g01.supplier_group, '-1') AS supplier_group,
  r01.department_num AS dept_num,
  COALESCE(b01.banner_id, - 1) AS banner_num,
  COALESCE(b01.channel_country, 'NA') AS channel_country,
  r01.month_num,
  r01.year_num AS fiscal_year_num,
  CAST(SUM(r01.receipts_regular_retail + r01.receipts_clearance_retail + r01.receipts_crossdock_regular_retail + r01.receipts_crossdock_clearance_retail) AS NUMERIC)
  AS actual_receipts_total_retail_amount,
  CAST(SUM(r01.receipts_regular_cost + r01.receipts_clearance_cost + r01.receipts_crossdock_regular_cost + r01.receipts_crossdock_clearance_cost) AS NUMERIC)
  AS actual_receipts_total_cost_amount,
  CAST(SUM(r01.receipts_regular_retail + r01.receipts_crossdock_regular_retail) AS NUMERIC) AS
  actual_receipts_regular_retail_amount,
  CAST(SUM(r01.receipts_clearance_retail + r01.receipts_crossdock_clearance_retail) AS NUMERIC) AS
  actual_receipts_clearance_retail_amount,
  CAST(SUM(r01.receipts_regular_cost + r01.receipts_crossdock_regular_cost) AS NUMERIC) AS
  actual_receipts_regular_cost_amount,
  CAST(SUM(r01.receipts_clearance_cost + r01.receipts_crossdock_clearance_cost) AS NUMERIC) AS
  actual_receipts_clearance_cost_amount,
  SUM(r01.receipts_regular_units + r01.receipts_crossdock_regular_units) AS actual_receipts_regular_units,
  SUM(r01.receipts_clearance_units + r01.receipts_crossdock_clearance_units) AS actual_receipts_clearance_units,
  SUM(r01.receipts_regular_units + r01.receipts_crossdock_regular_units + r01.receipts_clearance_units + r01.receipts_crossdock_clearance_units
    ) AS actual_receipts_total_units,
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_poreceipt_sku_store_week_fact_vw AS r01
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS b01 ON r01.channel_num = b01.channel_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS g01 ON LOWER(r01.supplier_num) = LOWER(g01.supplier_num) AND r01.department_num
        = CAST(g01.dept_num AS FLOAT64) AND LOWER(b01.banner_code) = LOWER(g01.banner) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
     >= CAST(g01.eff_begin_tmstp AS DATETIME) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    < CAST(g01.eff_end_tmstp AS DATETIME)
 WHERE r01.week_num >= (SELECT MIN(week_idnt)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
    WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 2
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
  AND r01.week_num <= (SELECT last_completed_fiscal_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
 GROUP BY supplier_group,
  dept_num,
  banner_num,
  channel_country,
  r01.month_num,
  fiscal_year_num);
EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE  =  1;
SET _ERROR_MESSAGE  =  @@error.message;
END;
/*SET QUERY_BAND = NONE FOR SESSION;*/
END;
