
INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_stg.merch_ap_plan_actual_suppgrp_banner_wrk (supplier_group, dept_num, banner_num, channel_country,
 month_num, fiscal_year_num, actual_net_sales_total_retail_amount, actual_net_sales_regular_retail_amount,
 actual_net_sales_promo_retail_amount, actual_net_sales_clearance_retail_amount, actual_net_sales_regular_cost_amount,
 actual_net_sales_promo_cost_amount, actual_net_sales_clearance_cost_amount, actual_net_sales_total_cost_amount,
 actual_net_sales_regular_units, actual_net_sales_promo_units, actual_net_sales_clearance_units,
 actual_net_sales_total_units, dw_batch_date, dw_sys_load_tmstp)
(SELECT COALESCE(g01.supplier_group, '-1') AS supplier_group,
  s01.department_num AS dept_num,
  COALESCE(b01.banner_id, - 1) AS banner_num,
  COALESCE(b01.channel_country, 'NA') AS channel_country,
  s01.month_num,
  s01.year_num AS fiscal_year_num,
  SUM(s01.net_sales_tot_retl) AS actual_net_sales_total_retail_amount,
  SUM(s01.net_sales_tot_regular_retl) AS actual_net_sales_regular_retail_amount,
  SUM(s01.net_sales_tot_promo_retl) AS actual_net_sales_promo_retail_amount,
  SUM(s01.net_sales_tot_clearance_retl) AS actual_net_sales_clearance_retail_amount,
  SUM(s01.net_sales_tot_regular_cost) AS actual_net_sales_regular_cost_amount,
  SUM(s01.net_sales_tot_promo_cost) AS actual_net_sales_promo_cost_amount,
  SUM(s01.net_sales_tot_clearance_cost) AS actual_net_sales_clearance_cost_amount,
  SUM(s01.net_sales_tot_cost) AS actual_net_sales_total_cost_amount,
  SUM(s01.net_sales_tot_regular_units) AS actual_net_sales_regular_units,
  SUM(s01.net_sales_tot_promo_units) AS actual_net_sales_promo_units,
  SUM(s01.net_sales_tot_clearance_units) AS actual_net_sales_clearance_units,
  SUM(s01.net_sales_tot_units) AS actual_net_sales_total_units,
  CURRENT_DATE('PST8PDT'),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.merch_sale_return_sku_store_week_fact_vw AS s01
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim_vw AS b01 ON s01.channel_num = b01.channel_num
  LEFT JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.supp_dept_map_dim AS g01 ON LOWER(s01.supplier_num) = LOWER(g01.supplier_num) AND s01.department_num
        = CAST(g01.dept_num AS FLOAT64) AND LOWER(b01.banner_code) = LOWER(g01.banner) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
     >= CAST(g01.eff_begin_tmstp AS DATETIME) AND CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME)
    < CAST(g01.eff_end_tmstp AS DATETIME)
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
  



