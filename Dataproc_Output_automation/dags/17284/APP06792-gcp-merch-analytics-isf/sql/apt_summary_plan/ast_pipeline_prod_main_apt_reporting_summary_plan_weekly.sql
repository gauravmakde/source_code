/*
APT Plan Summary Query
Owner: Jihyun Yu
Date Created: 2/6/23

Datalab: t2dl_das_apt_cost_reporting
Creates Tables:
    - plan_summary_category_weekly
    - plan_summary_suppliergroup_weekly
*/

-- Category APT Summary Plan



TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.plan_summary_category_weekly{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.plan_summary_category_weekly{{params.env_suffix}}
(SELECT wk.fiscal_year_num,
  wk.quarter_label,
  wk.month_idnt,
  wk.month_label,
  wk.month_454_label,
  wk.week_idnt,
  wk.week_label,
  wk.week_454_label,
  wk.week_start_day_date,
  wk.week_end_day_date,
  base.country,
  base.currency_code,
  base.banner,
  base.chnl_idnt,
  base.fulfill_type_num,
  base.category,
  base.price_band,
  TRIM(FORMAT('%11d', dpt.division_num) || ', ' || dpt.division_name) AS division_label,
  TRIM(FORMAT('%11d', dpt.subdivision_num) || ', ' || dpt.subdivision_name) AS subdivision_label,
  base.dept_idnt,
  TRIM(FORMAT('%11d', dpt.dept_num) || ', ' || dpt.dept_name) AS dept_label,
  map1.category_planner_1,
  map1.category_planner_2,
  map1.category_group,
  map1.seasonal_designation,
  map1.rack_merch_zone,
  map1.is_activewear,
  map1.channel_category_roles_1 AS nordstrom_ccs_roles,
  map1.channel_category_roles_2 AS rack_ccs_roles,
  COALESCE(sl.channel_label, 'UNKNOWN') AS channel_name,
  base.demand_units,
  base.demand_r_dollars,
  base.gross_sls_units,
  base.gross_sls_r_dollars,
  base.return_units,
  base.return_r_dollars,
  base.net_sls_units,
  base.net_sls_r_dollars,
  base.net_sls_c_dollars,
  base.avg_inv_ttl_c,
  base.avg_inv_ttl_u,
  base.rcpt_need_r,
  base.rcpt_need_c,
  CAST(TRUNC(CAST(base.rcpt_need_u AS FLOAT64)) AS INTEGER) AS rcpt_need_u,
  base.rcpt_need_lr_r,
  base.rcpt_need_lr_c,
  CAST(TRUNC(CAST(base.rcpt_need_lr_u AS FLOAT64)) AS INTEGER) AS rcpt_need_lr_u,
  base.plan_bop_c_dollars,
  CAST(TRUNC(CAST(base.plan_bop_u AS FLOAT64)) AS INTEGER) AS plan_bop_u,
  base.beginofmonth_bop_c,
  CAST(TRUNC(CAST(base.beginofmonth_bop_u AS FLOAT64)) AS INTEGER) AS beginofmonth_bop_u,
  base.endofmonth_bop_c,
  CAST(TRUNC(CAST(base.endofmonth_bop_u AS FLOAT64)) AS INTEGER) AS endofmonth_bop_u,
  base.ly_sales_c_dollars,
  base.ly_sales_r_dollars,
  base.ly_sales_u,
  base.ly_gross_r_dollars,
  base.ly_gross_u,
  base.ly_demand_r_dollars,
  base.ly_demand_u,
  base.ly_bop_c_dollars,
  base.ly_bop_u,
  base.ly_bom_bop_c_dollars,
  base.ly_bom_bop_u,
  base.ly_rcpt_need_c_dollars,
  base.ly_rcpt_need_u,
  base.cp_bop_ttl_c_dollars,
  base.cp_bop_ttl_u,
  base.cp_beginofmonth_bop_c,
  base.cp_beginofmonth_bop_u,
  base.cp_eop_ttl_c_dollars,
  base.cp_eop_ttl_u,
  base.cp_eop_eom_c_dollars,
  base.cp_eop_eom_u,
  base.cp_rcpt_need_c_dollars,
  base.cp_rcpt_need_u,
  base.cp_rcpt_need_lr_c_dollars,
  base.cp_rcpt_need_lr_u,
  base.cp_demand_r_dollars,
  base.cp_demand_u,
  base.cp_gross_sls_r_dollars,
  base.cp_gross_sls_u,
  base.cp_return_r_dollars,
  base.cp_return_u,
  base.cp_net_sls_r_dollars,
  base.cp_net_sls_c_dollars,
  base.cp_net_sls_u,
  timestamp(current_datetime('PST8PDT')) AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz
 FROM (SELECT COALESCE(pl_ly.week_idnt, mb.week_idnt, mc.week_idnt) AS week_idnt,
    COALESCE(pl_ly.month_idnt, mb.month_idnt, mc.month_idnt) AS month_idnt,
    COALESCE(pl_ly.chnl_idnt, mb.channel, mc.channel) AS chnl_idnt,
    COALESCE(pl_ly.banner, mb.banner, mc.banner) AS banner,
    COALESCE(pl_ly.country, mb.channel_country, mc.channel_country) AS country,
    COALESCE(pl_ly.dept_idnt, mb.dept_idnt, mc.dept_idnt) AS dept_idnt,
    COALESCE(pl_ly.fulfill_type_num, mb.fulfill_type_num, mc.fulfill_type_num) AS fulfill_type_num,
    COALESCE(pl_ly.category, mb.category, mc.category) AS category,
    COALESCE(pl_ly.price_band, mb.price_band, mc.price_band) AS price_band,
    COALESCE(pl_ly.currency_code, mb.currency, mc.currency) AS currency_code,
    
    
    pl_ly.demand_units,
    pl_ly.demand_r_dollars,
    pl_ly.gross_sls_units,
    pl_ly.gross_sls_r_dollars,
    pl_ly.return_units,
    pl_ly.return_r_dollars,
    pl_ly.net_sls_units,
    pl_ly.net_sls_r_dollars,
    pl_ly.net_sls_c_dollars,
    pl_ly.avg_inv_ttl_c,
    pl_ly.avg_inv_ttl_u,
    pl_ly.rcpt_need_r,
    pl_ly.rcpt_need_c,
    pl_ly.rcpt_need_u,
    pl_ly.rcpt_need_lr_r,
    pl_ly.rcpt_need_lr_c,
    pl_ly.rcpt_need_lr_u,
    pl_ly.plan_bop_c_dollars,
    pl_ly.plan_bop_u,
    pl_ly.beginofmonth_bop_c,
    pl_ly.beginofmonth_bop_u,
    pl_ly.endofmonth_bop_c,
    pl_ly.endofmonth_bop_u,
    pl_ly.ly_sales_c_dollars,
    pl_ly.ly_sales_r_dollars,
    pl_ly.ly_sales_u,
    pl_ly.ly_gross_r_dollars,
    pl_ly.ly_gross_u,
    pl_ly.ly_demand_r_dollars,
    pl_ly.ly_demand_u,
    pl_ly.ly_bop_c_dollars,
    pl_ly.ly_bop_u,
    pl_ly.ly_bom_bop_c_dollars,
    pl_ly.ly_bom_bop_u,
    pl_ly.ly_rcpt_need_c_dollars,
    pl_ly.ly_rcpt_need_u,
    mb.cp_bop_ttl_c_dollars,
    mb.cp_bop_ttl_u,
    mb.cp_beginofmonth_bop_c,
    mb.cp_beginofmonth_bop_u,
    mb.cp_eop_ttl_c_dollars,
    mb.cp_eop_ttl_u,
    mb.cp_eop_eom_c_dollars,
    mb.cp_eop_eom_u,
    mb.cp_rept_need_c_dollars AS cp_rcpt_need_c_dollars,
    mb.cp_rept_need_u AS cp_rcpt_need_u,
    mb.cp_rept_need_lr_c_dollars AS cp_rcpt_need_lr_c_dollars,
    mb.cp_rept_need_lr_u AS cp_rcpt_need_lr_u,
    mc.cp_demand_r_dollars,
    mc.cp_demand_u,
    mc.cp_gross_sls_r_dollars,
    mc.cp_gross_sls_u,
    mc.cp_return_r_dollars,
    mc.cp_return_u,
    mc.cp_net_sls_r_dollars,
    mc.cp_net_sls_c_dollars,
    mc.cp_net_sls_u
   FROM (
    SELECT 
    pl.week_idnt,
      pl.month_idnt,
      pl.chnl_idnt,
      pl.banner,
      pl.country,
      CAST(TRUNC(CAST(pl.dept_idnt AS FLOAT64)) AS INTEGER) AS dept_idnt,
      pl.fulfill_type_num,
      pl.category,
      pl.price_band_final AS price_band,
      pl.currency_code,
      pl.demand_units,
      pl.demand_r_dollars,
      pl.gross_sls_units,
      pl.gross_sls_r_dollars,
      pl.return_units,
      pl.return_r_dollars,
      pl.net_sls_units,
      pl.net_sls_r_dollars,
      pl.net_sls_c_dollars,
      pl.next_2months_sales_run_rate,
      pl.avg_inv_ttl_c,
      pl.avg_inv_ttl_u,
      pl.rcpt_need_r,
      pl.rcpt_need_c,
      pl.rcpt_need_u,
      pl.rcpt_need_lr_r,
      pl.rcpt_need_lr_c,
      pl.rcpt_need_lr_u,
      pl.plan_bop_c_dollars AS plan_bop_c_dollars,
      pl.plan_bop_c_units AS plan_bop_u,
       CASE
       WHEN pl.week_start_day_date = pl.month_start_day_date
       THEN pl.plan_bop_c_dollars
       ELSE NULL
       END AS beginofmonth_bop_c,
       CASE
       WHEN pl.week_start_day_date = pl.month_start_day_date
       THEN pl.plan_bop_c_units
       ELSE NULL
       END AS beginofmonth_bop_u,
       CASE
       WHEN pl.week_end_day_date = pl.month_end_day_date
       THEN pl.plan_eop_c_dollars
       ELSE NULL
       END AS endofmonth_bop_c,
       CASE
       WHEN pl.week_end_day_date = pl.month_end_day_date
       THEN pl.plan_eop_c_units
       ELSE NULL
       END AS endofmonth_bop_u,
      COALESCE(ly.net_sls_c, lly.net_sls_c) AS ly_sales_c_dollars,
      COALESCE(ly.net_sls_r, lly.net_sls_r) AS ly_sales_r_dollars,
      COALESCE(ly.net_sls_units, lly.net_sls_units) AS ly_sales_u,
      COALESCE(ly.gross_sls_r, lly.gross_sls_r) AS ly_gross_r_dollars,
      COALESCE(ly.gross_sls_units, lly.gross_sls_units) AS ly_gross_u,
      COALESCE(ly.demand_ttl_r, lly.demand_ttl_r) AS ly_demand_r_dollars,
      COALESCE(ly.demand_ttl_units, lly.demand_ttl_units) AS ly_demand_u,
      COALESCE(ly.boh_ttl_c, lly.boh_ttl_c) AS ly_bop_c_dollars,
      COALESCE(ly.boh_ttl_units, lly.boh_ttl_units) AS ly_bop_u,
      COALESCE(ly.beginofmonth_bop_c, lly.beginofmonth_bop_c) AS ly_bom_bop_c_dollars,
      COALESCE(ly.beginofmonth_bop_u, lly.beginofmonth_bop_u) AS ly_bom_bop_u,
       CASE
       WHEN LOWER(pl.banner) = LOWER('NORDSTROM')
       THEN COALESCE(ly.ttl_porcpt_c, lly.ttl_porcpt_c)
       WHEN LOWER(pl.banner) = LOWER('NORDSTROM_RACK')
       THEN COALESCE(ly.ttl_porcpt_c + ly.pah_tsfr_in_c + ly.rs_stk_tsfr_in_c, lly.ttl_porcpt_c + lly.pah_tsfr_in_c +
         lly.rs_stk_tsfr_in_c)
       ELSE 0
       END AS ly_rcpt_need_c_dollars,
       CASE
       WHEN LOWER(pl.banner) = LOWER('NORDSTROM')
       THEN COALESCE(ly.ttl_porcpt_c_units, lly.ttl_porcpt_c_units)
       WHEN LOWER(pl.banner) = LOWER('NORDSTROM_RACK')
       THEN COALESCE(ly.ttl_porcpt_c_units + ly.pah_tsfr_in_u + ly.rs_stk_tsfr_in_u, lly.ttl_porcpt_c_units + lly.pah_tsfr_in_u
           + lly.rs_stk_tsfr_in_u)
       ELSE 0
       END AS ly_rcpt_need_u
     FROM (SELECT month_idnt,
        month_start_day_date,
        month_end_day_date,
        week_idnt,
        week_start_day_date,
        week_end_day_date,
        chnl_idnt,
        banner,
        country,
        channel,
        dept_idnt,
        alternate_inventory_model,
        fulfill_type_num,
        category,
        price_band,
        currency_code,
        demand_units,
        demand_r_dollars,
        gross_sls_units,
        gross_sls_r_dollars,
        return_units,
        return_r_dollars,
        net_sls_units,
        net_sls_r_dollars,
        net_sls_c_dollars,
        next_2months_sales_run_rate,
        avg_inv_ttl_c,
        avg_inv_ttl_u,
        plan_bop_c_dollars,
        plan_bop_c_units,
        plan_eop_c_dollars,
        plan_eop_c_units,
        rcpt_need_r,
        rcpt_need_c,
        rcpt_need_u,
        rcpt_need_lr_r,
        rcpt_need_lr_c,
        rcpt_need_lr_u,
        rcd_update_timestamp,
        'TOTAL' AS price_band_final
       FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.category_channel_cost_plans_weekly AS p) AS pl
      LEFT JOIN (SELECT DISTINCT a.week_idnt,
        b.week_idnt AS week_idnt_ly,
        c.week_idnt AS week_idnt_lly
       FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
        LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.day_date_last_year_realigned = b.day_date
        LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c ON b.day_date_last_year_realigned = c.day_date
       WHERE a.day_date <= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 365 * 3 DAY)) AS dt ON pl.week_idnt = dt.week_idnt
      LEFT JOIN (SELECT channel_num,
        week_idnt,
        department_num,
        quantrix_category,
        'TOTAL' AS price_band,
         CASE
         WHEN LOWER(dropship_ind) = LOWER('Y')
         THEN 1
         ELSE 3
         END AS fulfill_type_num,
        SUM(net_sls_r) AS net_sls_r,
        SUM(net_sls_c) AS net_sls_c,
        SUM(net_sls_units) AS net_sls_units,
        SUM(gross_sls_r) AS gross_sls_r,
        SUM(gross_sls_units) AS gross_sls_units,
        SUM(demand_ttl_r) AS demand_ttl_r,
        SUM(demand_ttl_units) AS demand_ttl_units,
        SUM(eoh_ttl_c) AS eoh_ttl_c,
        SUM(eoh_ttl_units) AS eoh_ttl_units,
        SUM(boh_ttl_c) AS boh_ttl_c,
        SUM(boh_ttl_units) AS boh_ttl_units,
        SUM(beginofmonth_bop_c) AS beginofmonth_bop_c,
        SUM(beginofmonth_bop_u) AS beginofmonth_bop_u,
        SUM(ttl_porcpt_c) AS ttl_porcpt_c,
        SUM(ttl_porcpt_c_units) AS ttl_porcpt_c_units,
        SUM(pah_tsfr_in_c) AS pah_tsfr_in_c,
        SUM(pah_tsfr_in_u) AS pah_tsfr_in_u,
        SUM(rs_stk_tsfr_in_c) AS rs_stk_tsfr_in_c,
        SUM(rs_stk_tsfr_in_u) AS rs_stk_tsfr_in_u
       FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.category_priceband_cost_ly
       GROUP BY channel_num,
        week_idnt,
        department_num,
        quantrix_category,
        price_band,
        fulfill_type_num) AS ly 
        ON pl.week_idnt - 100 = ly.week_idnt 
        AND pl.chnl_idnt = ly.channel_num 
        AND CAST(pl.dept_idnt AS FLOAT64) = ly.department_num 
        AND LOWER(pl.category) = LOWER(ly.quantrix_category) AND pl.fulfill_type_num = ly.fulfill_type_num
        AND LOWER(pl.price_band_final) = LOWER(ly.price_band)
      LEFT JOIN 
      (
        SELECT channel_num,
        week_idnt,
        department_num,
        quantrix_category,
        'TOTAL' AS price_band,
         CASE
         WHEN LOWER(dropship_ind) = LOWER('Y')
         THEN 1
         ELSE 3
         END AS fulfill_type_num,
        SUM(net_sls_r) AS net_sls_r,
        SUM(net_sls_c) AS net_sls_c,
        SUM(net_sls_units) AS net_sls_units,
        SUM(gross_sls_r) AS gross_sls_r,
        SUM(gross_sls_units) AS gross_sls_units,
        SUM(demand_ttl_r) AS demand_ttl_r,
        SUM(demand_ttl_units) AS demand_ttl_units,
        SUM(eoh_ttl_c) AS eoh_ttl_c,
        SUM(eoh_ttl_units) AS eoh_ttl_units,
        SUM(boh_ttl_c) AS boh_ttl_c,
        SUM(boh_ttl_units) AS boh_ttl_units,
        SUM(beginofmonth_bop_c) AS beginofmonth_bop_c,
        SUM(beginofmonth_bop_u) AS beginofmonth_bop_u,
        SUM(ttl_porcpt_c) AS ttl_porcpt_c,
        SUM(ttl_porcpt_c_units) AS ttl_porcpt_c_units,
        SUM(pah_tsfr_in_c) AS pah_tsfr_in_c,
        SUM(pah_tsfr_in_u) AS pah_tsfr_in_u,
        SUM(rs_stk_tsfr_in_c) AS rs_stk_tsfr_in_c,
        SUM(rs_stk_tsfr_in_u) AS rs_stk_tsfr_in_u
       FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.category_priceband_cost_ly
       GROUP BY channel_num,
        week_idnt,
        department_num,
        quantrix_category,
        price_band,
        fulfill_type_num) AS lly 
        ON pl.week_idnt - 200 = lly.week_idnt 
        AND ly.week_idnt IS NULL 
        AND pl.chnl_idnt = lly.channel_num
        AND CAST(pl.dept_idnt AS FLOAT64) = lly.department_num 
        AND LOWER(pl.category) = LOWER(lly.quantrix_category
           ) AND pl.fulfill_type_num = lly.fulfill_type_num 
           AND LOWER(pl.price_band_final) = LOWER(lly.price_band)) AS
    pl_ly
    FULL JOIN (SELECT CASE
       WHEN LOWER(banner) = LOWER('FP')
       THEN 'NORDSTROM'
       WHEN LOWER(banner) = LOWER('OP')
       THEN 'NORDSTROM_RACK'
       ELSE NULL
       END AS banner,
      week_idnt,
      month_idnt,
      dept_idnt,
      channel_country,
      0 AS channel,
      currency,
      fulfill_type_num,
      'NOT_PLANNED' AS category,
      'TOTAL' AS price_band,
      cp_bop_ttl_c_dollars,
      cp_bop_ttl_u,
      cp_beginofmonth_bop_c,
      cp_beginofmonth_bop_u,
      cp_eop_ttl_c_dollars,
      cp_eop_ttl_u,
      cp_eop_eom_c_dollars,
      cp_eop_eom_u,
      cp_rept_need_c_dollars,
      cp_rept_need_u,
      cp_rept_need_lr_c_dollars,
      cp_rept_need_lr_u
     FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.mfp_cp_cost_banner_weekly) AS mb 
     ON pl_ly.week_idnt = mb.week_idnt 
     AND pl_ly.dept_idnt
             = mb.dept_idnt 
             AND LOWER(pl_ly.country) = LOWER(mb.channel_country) 
             AND pl_ly.chnl_idnt = mb.channel 
             AND pl_ly.fulfill_type_num = mb.fulfill_type_num 
         AND LOWER(pl_ly.banner) = LOWER(mb.banner) 
         AND LOWER(pl_ly.price_band) = LOWER(mb.price_band) 
         AND LOWER(pl_ly.category) = LOWER(mb.category)
    FULL JOIN (SELECT CASE
       WHEN chnl_idnt IN (110, 111, 120, 121, 140, 310, 311, 940, 990)
       THEN 'NORDSTROM'
       WHEN chnl_idnt IN (210, 211, 220, 221, 240, 250, 260, 261)
       THEN 'NORDSTROM_RACK'
       ELSE NULL
       END AS banner,
      week_idnt,
      month_idnt,
      dept_idnt,
      channel_country,
      chnl_idnt AS channel,
      currency,
      fulfill_type_num,
      'NOT_PLANNED' AS category,
      'TOTAL' AS price_band,
      cp_demand_r_dollars,
      cp_demand_u,
      cp_gross_sls_r_dollars,
      cp_gross_sls_u,
      cp_return_r_dollars,
      cp_return_u,
      cp_net_sls_r_dollars,
      cp_net_sls_c_dollars,
      cp_net_sls_u
     FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.mfp_cp_cost_channel_weekly) AS mc 
     ON pl_ly.week_idnt = mc.week_idnt 
     AND pl_ly.dept_idnt = mc.dept_idnt 
    AND LOWER(pl_ly.country) = LOWER(mc.channel_country) 
    AND pl_ly.chnl_idnt = mc.channel AND
         pl_ly.fulfill_type_num = mc.fulfill_type_num 
         AND LOWER(pl_ly.banner) = LOWER(mc.banner) 
         AND LOWER(pl_ly.price_band) = LOWER(mc.price_band) 
        AND LOWER(pl_ly.category) = LOWER(mc.category)) AS base
  LEFT JOIN (SELECT DISTINCT dept_num,
    category,
    category_planner_1,
    category_planner_2,
    category_group,
    seasonal_designation,
    rack_merch_zone,
    is_activewear,
    channel_category_roles_1,
    channel_category_roles_2
   FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.catg_subclass_map_dim) AS map1 
   ON base.dept_idnt = map1.dept_num 
   AND LOWER(base.category) =  LOWER(map1.category)
  LEFT JOIN (SELECT DISTINCT channel_country,
    channel_num,
    TRIM(FORMAT('%11d', channel_num) || ', ' || channel_desc) AS channel_label,
    channel_brand AS banner
   FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.price_store_dim_vw) AS sl ON LOWER(base.country) = LOWER(sl.channel_country) AND base.chnl_idnt
     = sl.channel_num AND LOWER(base.banner) = LOWER(sl.banner)
  LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt ON base.dept_idnt = dpt.dept_num
  INNER JOIN (SELECT DISTINCT week_idnt,
    week_label,
    week_454_label,
    month_idnt,
    month_label,
    month_454_label,
    week_start_day_date,
    week_end_day_date,
    quarter_label,
    fiscal_year_num
   FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE month_idnt >= (SELECT month_idnt
      FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE day_date = DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 7 DAY))) AS wk ON base.week_idnt = wk.week_idnt);


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.plan_summary_suppliergroup_weekly{{params.env_suffix}};

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.plan_summary_suppliergroup_weekly{{params.env_suffix}}
SELECT wk.fiscal_year_num,
 wk.quarter_label,
 wk.month_idnt,
 wk.month_label,
 wk.month_454_label,
 wk.month_start_day_date,
 wk.month_end_day_date,
 wk.week_idnt,
 wk.week_label,
 wk.week_454_label,
 wk.week_start_day_date,
 wk.week_end_day_date,
 pl.country,
 pl.currency_code,
 pl.banner,
 pl.chnl_idnt,
 pl.fulfill_type_num,
 pl.alternate_inventory_model,
 pl.category,
   TRIM(FORMAT('%11d', dpt.division_num)) || ', ' || TRIM(dpt.division_name) AS division_label,
 TRIM(FORMAT('%11d', dpt.subdivision_num) || ', ' || dpt.subdivision_name) AS subdivision_label,
 CAST(TRUNC(CAST(pl.dept_idnt AS FLOAT64)) AS INT64) AS dept_idnt,
 TRIM(FORMAT('%11d', dpt.dept_num) || ', ' || dpt.dept_name) AS dept_label,
 pl.supplier_group,
 supp.buy_planner,
 supp.preferred_partner_desc,
 supp.areas_of_responsibility,
 supp.is_npg,
 supp.diversity_group,
 supp.nord_to_rack_transfer_rate,
 pl.demand_units,
 pl.demand_r_dollars,
 pl.gross_sls_units,
 pl.gross_sls_r_dollars,
 pl.return_units,
 pl.return_r_dollars,
 pl.net_sls_units,
 pl.net_sls_r_dollars,
 pl.net_sls_c_dollars,
 pl.avg_inv_ttl_c,
 pl.avg_inv_ttl_u,
 pl.plan_bop_c_dollars,
 pl.plan_bop_c_units,
 pl.plan_eop_c_dollars,
 pl.plan_eop_c_units,
 pl.rcpt_need_r,
 pl.rcpt_need_c,
 CAST(TRUNC(pl.rcpt_need_u) AS INT64) AS rcpt_need_u,
 pl.rcpt_need_lr_r,
 pl.rcpt_need_lr_c,
 CAST(TRUNC(pl.rcpt_need_lr_u) AS INT64) AS rcpt_need_lr_u,
  CASE
  WHEN pl.week_start_day_date = pl.month_start_day_date
  THEN pl.plan_bop_c_dollars
  ELSE NULL
  END AS beginofmonth_bop_c,
  CAST(TRUNC(CASE
  WHEN pl.week_start_day_date = pl.month_start_day_date
  THEN pl.plan_bop_c_units
  ELSE NULL
  END) AS INT64) AS beginofmonth_bop_u,
 COALESCE(ly.net_sls_c, lly.net_sls_c) AS ly_sales_c_dollars,
 COALESCE(ly.net_sls_r, lly.net_sls_r) AS ly_sales_r_dollars,
 COALESCE(ly.net_sls_units, lly.net_sls_units) AS ly_sales_u,
 COALESCE(ly.gross_sls_r, lly.gross_sls_r) AS ly_gross_r_dollars,
 COALESCE(ly.gross_sls_units, lly.gross_sls_units) AS ly_gross_u,
 COALESCE(ly.demand_ttl_r, lly.demand_ttl_r) AS ly_demand_r_dollars,
 COALESCE(ly.demand_ttl_units, lly.demand_ttl_units) AS ly_demand_u,
 COALESCE(ly.boh_ttl_c, lly.boh_ttl_c) AS ly_bop_c_dollars,
 COALESCE(ly.boh_ttl_units, lly.boh_ttl_units) AS ly_bop_u,
 COALESCE(ly.beginofmonth_bop_c, lly.beginofmonth_bop_c) AS ly_bom_bop_c_dollars,
 COALESCE(ly.beginofmonth_bop_u, lly.beginofmonth_bop_u) AS ly_bom_bop_u,
  CASE
  WHEN LOWER(pl.banner) = LOWER('NORDSTROM')
  THEN COALESCE(ly.ttl_porcpt_c, lly.ttl_porcpt_c)
  WHEN LOWER(pl.banner) = LOWER('NORDSTROM_RACK')
  THEN COALESCE(ly.ttl_porcpt_c + ly.pah_tsfr_in_c + ly.rs_stk_tsfr_in_c, lly.ttl_porcpt_c + lly.pah_tsfr_in_c + lly.rs_stk_tsfr_in_c
    )
  ELSE 0
  END AS ly_rcpt_need_c_dollars,
  CASE
  WHEN LOWER(pl.banner) = LOWER('NORDSTROM')
  THEN COALESCE(ly.ttl_porcpt_c_units, lly.ttl_porcpt_c_units)
  WHEN LOWER(pl.banner) = LOWER('NORDSTROM_RACK')
  THEN COALESCE(ly.ttl_porcpt_c_units + ly.pah_tsfr_in_u + ly.rs_stk_tsfr_in_u, lly.ttl_porcpt_c_units + lly.pah_tsfr_in_u
      + lly.rs_stk_tsfr_in_u)
  ELSE 0
  END AS ly_rcpt_need_u,
  timestamp(current_datetime('PST8PDT')) AS update_timestamp,
  `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() AS update_timestamp_tz
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.suppliergroup_channel_cost_plans_weekly AS pl
 LEFT JOIN (SELECT DISTINCT a.week_idnt,
   b.week_idnt AS week_idnt_ly,
   c.week_idnt AS week_idnt_lly
  FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
   LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS b ON a.day_date_last_year_realigned = b.day_date
   LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS c ON b.day_date_last_year_realigned = c.day_date
  WHERE a.day_date <= DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 365 * 3 DAY)) AS dt ON pl.week_idnt = dt.week_idnt
 INNER JOIN (SELECT DISTINCT week_idnt,
   fiscal_year_num,
   quarter_label,
   month_idnt,
   month_label,
   month_454_label,
   month_start_day_date,
   month_end_day_date,
   week_label,
   week_454_label,
   week_start_day_date,
   week_end_day_date
  FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE month_idnt >= (SELECT month_idnt
     FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE day_date = CURRENT_DATE('PST8PDT'))) AS wk ON pl.week_idnt = wk.week_idnt
 LEFT JOIN (SELECT DISTINCT dept_num,
   supplier_group,
    CASE
    WHEN LOWER(UPPER(banner)) = LOWER('FP')
    THEN 'NORDSTROM'
    ELSE 'NORDSTROM_RACK'
    END AS banner,
   buy_planner,
   preferred_partner_desc,
   areas_of_responsibility,
   is_npg,
   diversity_group,
   nord_to_rack_transfer_rate
  FROM  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.supp_dept_map_dim
  QUALIFY (ROW_NUMBER() OVER (PARTITION BY dept_num, supplier_group, banner ORDER BY dw_sys_updt_tmstp DESC)) = 1) AS
 supp ON CAST(pl.dept_idnt AS FLOAT64) = CAST(TRUNC(CAST(supp.dept_num AS FLOAT64)) AS INTEGER) AND LOWER(pl.supplier_group) = LOWER(supp.supplier_group
     ) AND LOWER(pl.banner) = LOWER(supp.banner)
 LEFT JOIN  `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim AS dpt ON CAST(pl.dept_idnt AS FLOAT64) = dpt.dept_num
 LEFT JOIN (SELECT channel_num,
   week_idnt,
   department_num,
   quantrix_category,
   supplier_group,
    CASE
    WHEN LOWER(dropship_ind) = LOWER('Y')
    THEN 1
    ELSE 3
    END AS fulfill_type_num,
   SUM(net_sls_r) AS net_sls_r,
   SUM(net_sls_c) AS net_sls_c,
   SUM(net_sls_units) AS net_sls_units,
   SUM(gross_sls_r) AS gross_sls_r,
   SUM(gross_sls_units) AS gross_sls_units,
   SUM(demand_ttl_r) AS demand_ttl_r,
   SUM(demand_ttl_units) AS demand_ttl_units,
   SUM(eoh_ttl_c) AS eoh_ttl_c,
   SUM(eoh_ttl_units) AS eoh_ttl_units,
   SUM(boh_ttl_c) AS boh_ttl_c,
   SUM(boh_ttl_units) AS boh_ttl_units,
   SUM(beginofmonth_bop_c) AS beginofmonth_bop_c,
   SUM(beginofmonth_bop_u) AS beginofmonth_bop_u,
   SUM(ttl_porcpt_c) AS ttl_porcpt_c,
   SUM(ttl_porcpt_c_units) AS ttl_porcpt_c_units,
   SUM(pah_tsfr_in_c) AS pah_tsfr_in_c,
   SUM(pah_tsfr_in_u) AS pah_tsfr_in_u,
   SUM(rs_stk_tsfr_in_c) AS rs_stk_tsfr_in_c,
   SUM(rs_stk_tsfr_in_u) AS rs_stk_tsfr_in_u
  FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.category_suppliergroup_cost_ly
  GROUP BY channel_num,
   week_idnt,
   department_num,
   quantrix_category,
   supplier_group,
   fulfill_type_num) AS ly ON pl.week_idnt - 100 = ly.week_idnt AND pl.chnl_idnt = ly.channel_num AND CAST(pl.dept_idnt AS FLOAT64)
      = ly.department_num AND LOWER(ly.quantrix_category) = LOWER(pl.category) AND LOWER(pl.supplier_group) = LOWER(ly.supplier_group
     ) AND pl.fulfill_type_num = ly.fulfill_type_num
 LEFT JOIN (SELECT channel_num,
   week_idnt,
   department_num,
   quantrix_category,
   supplier_group,
    CASE
    WHEN LOWER(dropship_ind) = LOWER('Y')
    THEN 1
    ELSE 3
    END AS fulfill_type_num,
   SUM(net_sls_r) AS net_sls_r,
   SUM(net_sls_c) AS net_sls_c,
   SUM(net_sls_units) AS net_sls_units,
   SUM(gross_sls_r) AS gross_sls_r,
   SUM(gross_sls_units) AS gross_sls_units,
   SUM(demand_ttl_r) AS demand_ttl_r,
   SUM(demand_ttl_units) AS demand_ttl_units,
   SUM(eoh_ttl_c) AS eoh_ttl_c,
   SUM(eoh_ttl_units) AS eoh_ttl_units,
   SUM(boh_ttl_c) AS boh_ttl_c,
   SUM(boh_ttl_units) AS boh_ttl_units,
   SUM(beginofmonth_bop_c) AS beginofmonth_bop_c,
   SUM(beginofmonth_bop_u) AS beginofmonth_bop_u,
   SUM(ttl_porcpt_c) AS ttl_porcpt_c,
   SUM(ttl_porcpt_c_units) AS ttl_porcpt_c_units,
   SUM(pah_tsfr_in_c) AS pah_tsfr_in_c,
   SUM(pah_tsfr_in_u) AS pah_tsfr_in_u,
   SUM(rs_stk_tsfr_in_c) AS rs_stk_tsfr_in_c,
   SUM(rs_stk_tsfr_in_u) AS rs_stk_tsfr_in_u
  FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.category_suppliergroup_cost_ly
  GROUP BY channel_num,
   week_idnt,
   department_num,
   quantrix_category,
   supplier_group,
   fulfill_type_num) AS lly ON pl.week_idnt - 200 = lly.week_idnt AND ly.week_idnt IS NULL AND pl.chnl_idnt = lly.channel_num
   AND CAST(pl.dept_idnt AS FLOAT64) = lly.department_num AND LOWER(lly.quantrix_category) = LOWER(pl.category) AND
    LOWER(pl.supplier_group) = LOWER(lly.supplier_group) AND pl.fulfill_type_num = lly.fulfill_type_num;