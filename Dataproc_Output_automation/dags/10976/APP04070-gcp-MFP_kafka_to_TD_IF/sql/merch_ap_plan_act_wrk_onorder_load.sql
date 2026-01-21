
BEGIN
DECLARE ERROR_CODE INT64;
DECLARE ERROR_MESSAGE STRING;
/*SET QUERY_BAND = '
App_ID=APP04070;
DAG_ID=ap_plan_act_fct;
---Task_Name=t0_2_ap_plan_act_wrk_onorder_load;'*/


BEGIN
SET ERROR_CODE  =  0;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country,
 month_num, fiscal_year_num, actual_onorder_regular_retail_amount, actual_onorder_clearance_retail_amount,
 actual_onorder_total_retail_amount, actual_onorder_regular_cost_amount, actual_onorder_clearance_cost_amount,
 actual_onorder_total_cost_amount, actual_onorder_regular_units, actual_onorder_clearance_units,
 actual_onorder_total_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT COALESCE(sdmd.supplier_group, '-1') AS supplier_group,
  COALESCE(dd.dept_num, - 1) AS dept_num,
  COALESCE(occbd.banner_id, - 1) AS banner_num,
  COALESCE(occbd.channel_country, 'NA') AS channel_country,
  cal.month_idnt AS month_num,
  cal.fiscal_year_num,
  CAST(SUM(CASE
     WHEN LOWER(oo.anticipated_price_type) = LOWER('R')
     THEN oo.total_anticipated_retail_amt
     ELSE 0
     END) AS NUMERIC) AS actual_onorder_regular_retail_amount,
  CAST(SUM(CASE
     WHEN LOWER(oo.anticipated_price_type) = LOWER('C')
     THEN oo.total_anticipated_retail_amt
     ELSE 0
     END) AS NUMERIC) AS actual_onorder_clearance_retail_amount,
  SUM(oo.total_anticipated_retail_amt) AS actual_onorder_total_retail_amount,
  CAST(SUM(CASE
     WHEN LOWER(oo.anticipated_price_type) = LOWER('R')
     THEN oo.total_estimated_landing_cost
     ELSE 0
     END) AS NUMERIC) AS actual_onorder_regular_cost_amount,
  CAST(SUM(CASE
     WHEN LOWER(oo.anticipated_price_type) = LOWER('C')
     THEN oo.total_estimated_landing_cost
     ELSE 0
     END) AS NUMERIC) AS actual_onorder_clearance_cost_amount,
  SUM(oo.total_estimated_landing_cost) AS actual_onorder_total_cost_amount,
  SUM(CASE
    WHEN LOWER(oo.anticipated_price_type) = LOWER('R')
    THEN oo.quantity_open
    ELSE 0
    END) AS actual_onorder_regular_units,
  SUM(CASE
    WHEN LOWER(oo.anticipated_price_type) = LOWER('C')
    THEN oo.quantity_open
    ELSE 0
    END) AS actual_onorder_clearance_units,
  SUM(oo.quantity_open) AS actual_onorder_total_units,
  MAX(oo.dw_batch_date) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM (SELECT CASE
     WHEN week_num < (SELECT week_idnt
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.day_cal_454_dim
       WHERE day_date = CURRENT_DATE('PST8PDT'))
     THEN (SELECT week_idnt
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_dim.day_cal_454_dim
      WHERE day_date = CURRENT_DATE('PST8PDT'))
     ELSE week_num
     END AS week_num,
    rms_sku_num,
    store_num,
    anticipated_price_type,
    status,
    end_ship_date,
    first_approval_date,
    quantity_open,
    total_estimated_landing_cost,
    total_anticipated_retail_amt,
    dw_batch_date
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_on_order_fact_vw) AS oo
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.store_dim AS orgstore ON oo.store_num = orgstore.store_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS occbd ON orgstore.channel_num = occbd.channel_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.product_sku_dim AS psd ON LOWER(psd.rms_sku_num) = LOWER(TRIM(oo.rms_sku_num)) AND LOWER(orgstore
     .store_country_code) = LOWER(psd.channel_country)
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.department_dim AS dd ON psd.dept_num = dd.dept_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS sdmd ON LOWER(sdmd.supplier_num) = LOWER(COALESCE(psd.prmy_supp_num,
         FORMAT('%11d', - 1))) AND CAST(sdmd.dept_num AS FLOAT64) = dd.dept_num AND LOWER(sdmd.banner) = LOWER(occbd.banner_code
       ) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) >= dd.eff_begin_tmstp_utc
      AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS TIMESTAMP) < dd.eff_end_tmstp_utc
  INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_week_cal_454_vw AS cal ON oo.week_num = cal.week_idnt
 WHERE oo.quantity_open > 0
  AND (LOWER(oo.status) = LOWER('CLOSED') AND oo.end_ship_date >= DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 45 DAY) 
  OR LOWER(oo.status
      ) IN (LOWER('APPROVED'), LOWER('WORKSHEET')))
  AND oo.first_approval_date IS NOT NULL
 GROUP BY supplier_group,
  dept_num,
  banner_num,
  channel_country,
  month_num,
  cal.fiscal_year_num);

--ET;
/*SET QUERY_BAND = NONE FOR SESSION;*/

EXCEPTION WHEN ERROR THEN
--ET;
RAISE USING MESSAGE = @@error.message;
END;
END;
