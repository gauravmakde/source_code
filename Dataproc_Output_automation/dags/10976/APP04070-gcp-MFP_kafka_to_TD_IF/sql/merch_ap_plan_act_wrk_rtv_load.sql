INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country,
 month_num, fiscal_year_num, actual_rtv_total_cost_amount, actual_rtv_total_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT COALESCE(g01.supplier_group, '-1') AS supplier_group,
  s01.department_num AS dept_num,
   CASE
   WHEN s01.channel_num = {{params.planningchannelcode_rtv_banner_id_1}}
   THEN 1
   ELSE COALESCE(b01.banner_id, - 1)
   END AS banner_num,
   CASE
   WHEN s01.channel_num = {{params.planningchannelcode_rtv_banner_id_1}}
   THEN 'US'
   ELSE COALESCE(b01.channel_country, 'NA')
   END AS channel_country,
  s01.month_num,
  s01.year_num AS fiscal_year_num,
  SUM(s01.rtv_total_cost) AS actual_rtv_total_cost_amount,
  SUM(s01.rtv_total_units) AS actual_rtv_total_units,
  MAX(s01.dw_batch_date) AS dw_batch_date,
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_return_to_vendor_sku_loc_week_agg_fact_vw AS s01
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS b01 ON s01.channel_num = b01.channel_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS g01 ON LOWER(s01.supplier_num) = LOWER(g01.supplier_num) AND s01.department_num
        = CAST(g01.dept_num AS FLOAT64) AND LOWER(CASE
        WHEN s01.channel_num = {{params.planningchannelcode_rtv_banner_id_1}}
        THEN 'FP'
        ELSE b01.banner_code
        END) = LOWER(g01.banner) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) >= CAST(g01.eff_begin_tmstp AS DATETIME)
      AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) < CAST(g01.eff_end_tmstp AS DATETIME)
 WHERE s01.week_num >= (SELECT MIN(week_idnt)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
    WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 2
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
       WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
  AND s01.week_num <= (SELECT last_completed_fiscal_week_num
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
    WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY'))
 GROUP BY supplier_group,
  dept_num,
  banner_num,
  channel_country,
  s01.month_num,
  fiscal_year_num);