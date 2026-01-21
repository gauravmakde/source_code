/*
MFP Cp Cost
Author: Michelle Du
Owner: Jihyun Yu
Date Created: 10/18/22
Date Last Updated: 2/17/23

Purpose: Create MFP Cp cost at channel and banner level tables.

*/


CREATE TEMPORARY TABLE IF NOT EXISTS dates_lkup_mfp
AS
SELECT DISTINCT week_idnt,
 week_start_day_date,
 week_end_day_date,
 month_idnt,
 month_start_day_date,
 month_end_day_date,
 week_num_of_fiscal_month,
 MAX(week_num_of_fiscal_month) OVER (PARTITION BY month_idnt RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
 AS weeks_in_month,
 week_label,
 month_label,
 quarter_label,
 fiscal_year_num
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
WHERE week_end_day_date BETWEEN DATE_SUB(CURRENT_DATE('PST8PDT'), INTERVAL 365 * 2 DAY) AND (DATE_ADD(CURRENT_DATE('PST8PDT'), INTERVAL 365 * 2 DAY))
 AND fiscal_year_num <> 2020;


CREATE TEMPORARY TABLE IF NOT EXISTS dept_lkup_ccp
AS
SELECT DISTINCT dept_num,
 division_num,
 subdivision_num,
 TRIM(FORMAT('%11d', dept_num) || ', ' || dept_name) AS dept_label,
 TRIM(FORMAT('%11d', division_num) || ', ' || division_name) AS division_label,
 TRIM(FORMAT('%11d', subdivision_num) || ', ' || subdivision_name) AS subdivision_label
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim;


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.mfp_cp_cost_channel_weekly{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.mfp_cp_cost_channel_weekly{{params.env_suffix}}
(SELECT dc.month_idnt,
  dc.month_start_day_date,
  dc.month_end_day_date,
  mfpc.week_num AS week_idnt,
  dc.week_start_day_date,
  dc.week_end_day_date,
  dc.quarter_label,
  dc.fiscal_year_num,
  mfpc.fulfill_type_num,
   CASE
   WHEN mfpc.channel_num IN (110, 120, 210, 220, 250, 260, 310)
   THEN 'US'
   WHEN mfpc.channel_num IN (111, 121, 211, 221, 261, 311)
   THEN 'CA'
   ELSE NULL
   END AS channel_country,
   CASE
   WHEN LOWER(CASE
      WHEN mfpc.channel_num IN (110, 120, 210, 220, 250, 260, 310)
      THEN 'US'
      WHEN mfpc.channel_num IN (111, 121, 211, 221, 261, 311)
      THEN 'CA'
      ELSE NULL
      END) = LOWER('US')
   THEN 'USD'
   WHEN LOWER(CASE
      WHEN mfpc.channel_num IN (110, 120, 210, 220, 250, 260, 310)
      THEN 'US'
      WHEN mfpc.channel_num IN (111, 121, 211, 221, 261, 311)
      THEN 'CA'
      ELSE NULL
      END) = LOWER('CA')
   THEN 'CAD'
   ELSE NULL
   END AS currency,
  mfpc.channel_num AS chnl_idnt,
  mfpc.dept_num AS dept_idnt,
  dp.dept_label,
  dp.division_label,
  dp.subdivision_label,
  SUM(mfpc.cp_demand_total_retail_amt) AS cp_demand_r_dollars,
  SUM(mfpc.cp_demand_total_qty) AS cp_demand_u,
  SUM(mfpc.cp_gross_sales_retail_amt) AS cp_gross_sls_r_dollars,
  SUM(mfpc.cp_gross_sales_qty) AS cp_gross_sls_u,
  SUM(mfpc.cp_returns_retail_amt) AS cp_return_r_dollars,
  SUM(mfpc.cp_returns_qty) AS cp_return_u,
  SUM(mfpc.cp_net_sales_retail_amt) AS cp_net_sls_r_dollars,
  SUM(mfpc.cp_net_sales_cost_amt) AS cp_net_sls_c_dollars,
  SUM(mfpc.cp_net_sales_qty) AS cp_net_sls_u
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_channel_fact AS mfpc
  INNER JOIN dates_lkup_mfp AS dc ON mfpc.week_num = dc.week_idnt
  LEFT JOIN dept_lkup_ccp AS dp ON mfpc.dept_num = dp.dept_num
 WHERE mfpc.fulfill_type_num IN (1, 3)
 GROUP BY dc.month_idnt,
  dc.month_start_day_date,
  dc.month_end_day_date,
  week_idnt,
  dc.week_start_day_date,
  dc.week_end_day_date,
  dc.quarter_label,
  dc.fiscal_year_num,
  mfpc.fulfill_type_num,
  channel_country,
  currency,
  chnl_idnt,
  dept_idnt,
  dp.dept_label,
  dp.division_label,
  dp.subdivision_label);


--COLLECT STATS     PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, chnl_idnt, channel_country, dept_idnt)     ON t2dl_das_apt_cost_reporting.mfp_cp_cost_channel_weekly


TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.mfp_cp_cost_banner_weekly{{params.env_suffix}};


INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.mfp_cp_cost_banner_weekly{{params.env_suffix}}
(SELECT dc.month_idnt,
  dc.month_start_day_date,
  dc.month_end_day_date,
  mfpc.week_num AS week_idnt,
  dc.week_start_day_date,
  dc.week_end_day_date,
  dc.quarter_label,
  dc.fiscal_year_num,
  mfpc.fulfill_type_num,
   CASE
   WHEN mfpc.banner_country_num IN (1, 3)
   THEN 'US'
   WHEN mfpc.banner_country_num IN (2, 4)
   THEN 'CA'
   ELSE NULL
   END AS channel_country,
   CASE
   WHEN mfpc.banner_country_num IN (1, 2)
   THEN 'FP'
   WHEN mfpc.banner_country_num IN (3, 4)
   THEN 'OP'
   ELSE NULL
   END AS banner,
  mfpc.dept_num AS dept_idnt,
  dp.dept_label,
  dp.division_label,
  dp.subdivision_label,
   CASE
   WHEN mfpc.banner_country_num IN (1, 3)
   THEN 'USD'
   WHEN mfpc.banner_country_num IN (2, 4)
   THEN 'CAD'
   ELSE NULL
   END AS currency,
  SUM(mfpc.cp_beginning_of_period_active_cost_amt) AS cp_bop_ttl_c_dollars,
  SUM(mfpc.cp_beginning_of_period_active_qty) AS cp_bop_ttl_u,
  SUM(CASE
    WHEN dc.week_num_of_fiscal_month = 1
    THEN mfpc.cp_beginning_of_period_active_cost_amt
    ELSE NULL
    END) AS cp_beginofmonth_bop_c,
  SUM(CASE
    WHEN dc.week_num_of_fiscal_month = 1
    THEN mfpc.cp_beginning_of_period_active_qty
    ELSE NULL
    END) AS cp_beginofmonth_bop_u,
  SUM(mfpc.cp_ending_of_period_active_cost_amt) AS cp_eop_ttl_c_dollars,
  SUM(mfpc.cp_ending_of_period_active_qty) AS cp_eop_ttl_u,
  SUM(CASE
    WHEN dc.week_num_of_fiscal_month = dc.weeks_in_month
    THEN mfpc.cp_ending_of_period_active_cost_amt
    ELSE NULL
    END) AS cp_eop_eom_c_dollars,
  SUM(CASE
    WHEN dc.week_num_of_fiscal_month = dc.weeks_in_month
    THEN mfpc.cp_ending_of_period_active_qty
    ELSE NULL
    END) AS cp_eop_eom_u,
  SUM(mfpc.cp_receipts_active_cost_amt) AS cp_rept_need_c_dollars,
  SUM(mfpc.cp_receipts_active_qty) AS cp_rept_need_u,
  SUM(mfpc.cp_receipts_active_cost_amt - mfpc.cp_receipts_reserve_cost_amt) AS cp_rept_need_lr_c_dollars,
  SUM(mfpc.cp_receipts_active_qty - mfpc.cp_receipts_reserve_qty) AS cp_rept_need_lr_u
 FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.mfp_cost_plan_actual_banner_country_fact AS mfpc
  INNER JOIN dates_lkup_mfp AS dc ON mfpc.week_num = dc.week_idnt
  LEFT JOIN dept_lkup_ccp AS dp ON mfpc.dept_num = dp.dept_num
 WHERE mfpc.fulfill_type_num IN (1, 3)
 GROUP BY dc.month_idnt,
  dc.month_start_day_date,
  dc.month_end_day_date,
  week_idnt,
  dc.week_start_day_date,
  dc.week_end_day_date,
  dc.quarter_label,
  dc.fiscal_year_num,
  mfpc.fulfill_type_num,
  channel_country,
  banner,
  dept_idnt,
  dp.dept_label,
  dp.division_label,
  dp.subdivision_label,
  currency);


--COLLECT STATS     PRIMARY INDEX (week_idnt, week_start_day_date, month_idnt, banner, channel_country, dept_idnt) 	, COLUMN (week_idnt, fulfill_type_num, channel_country, dept_idnt)     	ON t2dl_das_apt_cost_reporting.mfp_cp_cost_banner_weekly