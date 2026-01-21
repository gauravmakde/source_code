TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_spe_mfp_dept_banner_year_fact;


INSERT INTO `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_fct.merch_spe_mfp_dept_banner_year_fact (dept_num, banner_num, channel_country, fiscal_year_num,
 ty_net_sales_total_retail_amount, ly_net_sales_total_retail_amount, lly_net_sales_total_retail_amount,
 ty_net_sales_total_cost_amount, ly_net_sales_total_cost_amount, lly_net_sales_total_cost_amount, dw_batch_date,
 dw_sys_load_tmstp)
(SELECT dept_num,
  banner_num,
  channel_country,
  fiscal_year_num,
  CAST(SUM(ty_net_sales_total_retail_amount) AS NUMERIC),
  CAST(SUM(ly_net_sales_total_retail_amount) AS NUMERIC),
  CAST(SUM(lly_net_sales_total_retail_amount) AS NUMERIC),
  CAST(SUM(ty_net_sales_total_cost_amount) AS NUMERIC),
  CAST(SUM(ly_net_sales_total_cost_amount) AS NUMERIC),
  CAST(SUM(lly_net_sales_total_cost_amount) AS NUMERIC),
  MAX(dw_batch_date),
  CAST(FORMAT_TIMESTAMP('%F %H:%M:%E6S', CURRENT_DATETIME('PST8PDT')) AS DATETIME) AS dw_sys_load_tmstp
 FROM (SELECT a.dept_num,
     b.banner_id AS banner_num,
     b.channel_country,
     c.fiscal_year_num,
      CASE
      WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
      THEN a.ty_net_sales_retail_amt
      ELSE a.op_net_sales_retail_amt
      END AS ty_net_sales_total_retail_amount,
     0 AS ly_net_sales_total_retail_amount,
     0 AS lly_net_sales_total_retail_amount,
      CASE
      WHEN c.month_idnt <= l01.last_completed_fiscal_month_num
      THEN a.ty_net_sales_cost_amt
      ELSE a.op_net_sales_cost_amt
      END AS ty_net_sales_total_cost_amount,
     0 AS ly_net_sales_total_cost_amount,
     0 AS lly_net_sales_total_cost_amount,
     l01.dw_batch_dt AS dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_plan_actual_channel_fact AS a
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim AS b ON a.channel_num = b.channel_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS c ON a.week_num = c.week_idnt
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l01 ON LOWER(l01.interface_code) = LOWER('MERCH_NAP_SPE_DLY'
       )
    WHERE a.week_num >= (SELECT MIN(week_idnt)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
       WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 1
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
          WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
     AND a.week_num <= (SELECT MAX(week_idnt)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
       WHERE fiscal_year_num = (SELECT current_fiscal_year_num + 2
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
          WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
    UNION ALL
    SELECT a0.dept_num,
     b0.banner_id AS banner_num,
     b0.channel_country,
     d.year_num AS fiscal_year_num,
     0 AS ty_net_sales_total_retail_amount,
      CASE
      WHEN c0.month_idnt <= l010.last_completed_fiscal_month_num
      THEN a0.ty_net_sales_retail_amt
      ELSE a0.op_net_sales_retail_amt
      END AS ly_net_sales_total_retail_amount,
     0 AS lly_net_sales_total_retail_amount,
     0 AS ty_net_sales_total_cost_amount,
      CASE
      WHEN c0.month_idnt <= l010.last_completed_fiscal_month_num
      THEN a0.ty_net_sales_cost_amt
      ELSE a0.op_net_sales_cost_amt
      END AS ly_net_sales_total_cost_amount,
     0 AS lly_net_sales_total_cost_amount,
     l010.dw_batch_dt AS dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_plan_actual_channel_fact AS a0
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim AS b0 ON a0.channel_num = b0.channel_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS c0 ON a0.week_num = c0.week_idnt
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.month_cal_vw AS d ON c0.month_idnt = d.last_year_month_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l010 ON LOWER(l010.interface_code) = LOWER('MERCH_NAP_SPE_DLY'
       )
    WHERE a0.week_num >= (SELECT MIN(week_idnt)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
       WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 2
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
          WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
     AND a0.week_num <= (SELECT MAX(week_idnt)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
       WHERE fiscal_year_num = (SELECT current_fiscal_year_num + 1
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
          WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
    UNION ALL
    SELECT a1.dept_num,
     b1.banner_id AS baneer_num,
     b1.channel_country,
     d0.year_num AS fiscal_year_num,
     0 AS ty_net_sales_total_retail_amount,
     0 AS ly_net_sales_total_retail_amount,
      CASE
      WHEN c1.month_idnt <= l011.last_completed_fiscal_month_num
      THEN a1.ty_net_sales_retail_amt
      ELSE a1.op_net_sales_retail_amt
      END AS lly_net_sales_total_retail_amount,
     0 AS ty_net_sales_total_cost_amount,
     0 AS ly_net_sales_total_cost_amount,
      CASE
      WHEN c1.month_idnt <= l011.last_completed_fiscal_month_num
      THEN a1.ty_net_sales_cost_amt
      ELSE a1.op_net_sales_cost_amt
      END AS lly_net_sales_total_cost_amount,
     l011.dw_batch_dt AS dw_batch_date
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.mfp_cost_plan_actual_channel_fact AS a1
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.org_channel_country_banner_dim AS b1 ON a1.channel_num = b1.channel_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw AS c1 ON a1.week_num = c1.week_idnt
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_base_vws.month_cal_vw AS d0 ON c1.month_idnt = d0.lly_month_num
     INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw AS l011 ON LOWER(l011.interface_code) = LOWER('MERCH_NAP_SPE_DLY'
       )
    WHERE a1.week_num >= (SELECT MIN(week_idnt)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
       WHERE fiscal_year_num = (SELECT current_fiscal_year_num - 3
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
          WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))
     AND a1.week_num <= (SELECT MAX(week_idnt)
       FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_week_cal_454_vw
       WHERE fiscal_year_num = (SELECT current_fiscal_year_num
          FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.merch_tran_last_completed_month_vw
          WHERE LOWER(interface_code) = LOWER('MERCH_NAP_SPE_DLY')))) AS X
 GROUP BY dept_num,
  banner_num,
  channel_country,
  fiscal_year_num);