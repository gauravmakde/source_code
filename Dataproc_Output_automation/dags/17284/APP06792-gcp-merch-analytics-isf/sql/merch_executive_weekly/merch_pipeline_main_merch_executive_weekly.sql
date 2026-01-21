/*
Name:Monday Morning Reporting script
Project:Monday Morning Reporting Dash
Purpose:  Combine MFP, WBR and APT data source into a T2 Table that can direclty feed into Monday Morning Reporting for performance improvment
Variable(s):    {T2DL_DAS_SELECTION} T2DL_NAP_AIS_BATCH
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
DAG:
Author(s):Xiao Tong, Manuela Hurtado, Tanner Moxcey
Date Created:3/31/23
Date Last Updated:1/19/24
*/

------------------------------------------------------------- START TEMPORARY TABLES -----------------------------------------------------------------------





CREATE TEMPORARY TABLE IF NOT EXISTS date_lookup
AS
SELECT DISTINCT week_idnt,
 month_idnt
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim;


--COLLECT STATS 	PRIMARY INDEX ("week_idnt") 	,COLUMN ("week_idnt") 	ON date_lookup


CREATE TEMPORARY TABLE IF NOT EXISTS aor
AS
SELECT DISTINCT 
REGEXP_REPLACE(channel_brand, '_', ' ') AS banner,
 dept_num,
 general_merch_manager_executive_vice_president,
 div_merch_manager_senior_vice_president,
 div_merch_manager_vice_president,
 merch_director,
 buyer,
 merch_planning_executive_vice_president,
 merch_planning_senior_vice_president,
 merch_planning_vice_president,
 merch_planning_director_manager,
 assortment_planner
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.area_of_responsibility_dim
QUALIFY (ROW_NUMBER() OVER (PARTITION BY banner, dept_num ORDER BY 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)) = 1;


--COLLECT STATS 	PRIMARY INDEX (banner,dept_num)  		ON aor 


-- apt eoh no intransit -- wbr eop, bop include intransit


CREATE TEMPORARY TABLE IF NOT EXISTS mfp
AS
SELECT 'MFP' AS data_source,
  CASE
  WHEN CAST(SUBSTR(SUBSTR(CAST(d.week_idnt AS STRING), 1, 6), 0, 4) AS FLOAT64) = (SELECT MAX(fiscal_year_num)
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
  THEN SUBSTR('TY', 1, 3)
  WHEN CAST(SUBSTR(SUBSTR(CAST(d.week_idnt AS STRING), 1, 6), 0, 4) AS FLOAT64) = (SELECT MAX(fiscal_year_num) - 1
    FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
    WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
  THEN SUBSTR('LY', 1, 3)
  ELSE 'NA'
  END AS ty_ly_ind,
 m.week_num AS week_idnt,
 d.month_idnt,
 m.banner,
 m.country,
 m.channel,
 m.division,
 m.subdivision,
 m.department_label AS department,
 m.dept_num,
 SUBSTR('NA', 1, 10) AS price_type,
 SUBSTR('NA', 1, 40) AS category,
 SUBSTR('NA', 1, 40) AS supplier_group,
 0 AS apt_net_sls_r,
 0 AS apt_fut_net_sls_r_dollars,
 0 AS apt_net_sls_c,
 0 AS apt_fut_net_sls_c_dollars,
 0 AS apt_net_sls_units,
 0 AS apt_fut_net_sls_c_units,
 0 AS apt_demand_r_online,
 0 AS apt_fut_demand_r_online,
 0 AS apt_eop_ttl_units,
 0 AS apt_eop_ttl_c,
 0 AS apt_fut_eop_ttl_c,
 0 AS apt_ttl_porcpt_c,
 0 AS apt_fut_rcpt_need_c,
 0 AS apt_plan_bop_c,
 0 AS apt_plan_bop_u,
 0 AS apt_plan_eop_u,
 0 AS wbr_sales_dollars,
 0 AS wbr_sales_units,
 0 AS wbr_eoh_units,
 0 AS wbr_eoh_cost,
 0 AS wbr_sales_cost,
 0 AS wbr_sales_pm,
 SUM(m.cp_bop_tot_cost_amt) AS cp_bop_total_c,
 SUM(m.op_bop_tot_cost_amt) AS op_bop_total_c,
 SUM(m.sp_bop_tot_cost_amt) AS sp_bop_total_c,
 SUM(m.ty_bop_tot_cost_amt) AS ty_bop_total_c,
 SUM(m.ly_bop_tot_cost_amt) AS ly_bop_total_c,
 SUM(m.cp_bop_tot_units) AS cp_bop_total_u,
 SUM(m.op_bop_tot_units) AS op_bop_total_u,
 SUM(m.sp_bop_tot_units) AS sp_bop_total_u,
 SUM(m.ty_bop_tot_units) AS ty_bop_total_u,
 SUM(m.ly_bop_tot_units) AS ly_bop_total_u,
 SUM(m.cp_eop_active_cost_amt) AS cp_eop_active_c,
 SUM(m.op_eop_active_cost_amt) AS op_eop_active_c,
 SUM(m.sp_eop_active_cost_amt) AS sp_eop_active_c,
 SUM(m.ty_eop_active_cost_amt) AS ty_eop_active_c,
 SUM(m.ly_eop_active_cost_amt) AS ly_eop_active_c,
 SUM(m.cp_eop_active_units) AS cp_eop_active_u,
 SUM(m.op_eop_active_units) AS op_eop_active_u,
 SUM(m.sp_eop_active_units) AS sp_eop_active_u,
 SUM(m.ty_eop_active_units) AS ty_eop_active_u,
 SUM(m.ly_eop_active_units) AS ly_eop_active_u,
 SUM(m.cp_eop_tot_cost_amt) AS cp_eop_total_c,
 SUM(m.op_eop_tot_cost_amt) AS op_eop_total_c,
 SUM(m.sp_eop_tot_cost_amt) AS sp_eop_total_c,
 SUM(m.ty_eop_tot_cost_amt) AS ty_eop_total_c,
 SUM(m.ly_eop_tot_cost_amt) AS ly_eop_total_c,
 SUM(m.cp_eop_tot_units) AS cp_eop_total_u,
 SUM(m.op_eop_tot_units) AS op_eop_total_u,
 SUM(m.sp_eop_tot_units) AS sp_eop_total_u,
 SUM(m.ty_eop_tot_units) AS ty_eop_total_u,
 SUM(m.ly_eop_tot_units) AS ly_eop_total_u,
 SUM(m.cp_gross_margin_retail_amt) AS cp_gross_margin,
 SUM(m.op_gross_margin_retail_amt) AS op_gross_margin,
 SUM(m.sp_gross_margin_retail_amt) AS sp_gross_margin,
 SUM(m.ty_gross_margin_retail_amt) AS ty_gross_margin,
 SUM(m.ly_gross_margin_retail_amt) AS ly_gross_margin,
 SUM(m.cp_merch_margin_amt) AS cp_merch_margin,
 SUM(m.op_merch_margin_amt) AS op_merch_margin,
 SUM(m.sp_merch_margin_amt) AS sp_merch_margin,
 SUM(m.ty_merch_margin_amt) AS ty_merch_margin,
 SUM(m.ly_merch_margin_amt) AS ly_merch_margin,
 SUM(m.cp_net_sales_cost_amt) AS cp_net_sales_c,
 SUM(m.op_net_sales_cost_amt) AS op_net_sales_c,
 SUM(m.sp_net_sales_cost_amt) AS sp_net_sales_c,
 SUM(m.ty_net_sales_cost_amt) AS ty_net_sales_c,
 SUM(m.ly_net_sales_cost_amt) AS ly_net_sales_c,
 SUM(m.cp_net_sales_retail_amt) AS cp_net_sales_r,
 SUM(m.op_net_sales_retail_amt) AS op_net_sales_r,
 SUM(m.sp_net_sales_retail_amt) AS sp_net_sales_r,
 SUM(m.ty_net_sales_retail_amt) AS ty_net_sales_r,
 SUM(m.ly_net_sales_retail_amt) AS ly_net_sales_r,
 SUM(m.cp_net_sales_units) AS cp_net_sales_u,
 SUM(m.op_net_sales_units) AS op_net_sales_u,
 SUM(m.sp_net_sales_units) AS sp_net_sales_u,
 SUM(m.ty_net_sales_units) AS ty_net_sales_u,
 SUM(m.ly_net_sales_units) AS ly_net_sales_u,
 SUM(m.cp_rcpts_tot_cost_amt) AS cp_rcpts_total_c,
 SUM(m.op_rcpts_tot_cost_amt) AS op_rcpts_total_c,
 SUM(m.sp_rcpts_tot_cost_amt) AS sp_rcpts_total_c,
 SUM(m.ty_rcpts_tot_cost_amt) AS ty_rcpts_total_c,
 SUM(m.ly_rcpts_tot_cost_amt) AS ly_rcpts_total_c,
 SUM(m.cp_rcpts_tot_units) AS cp_rcpts_total_u,
 SUM(m.op_rcpts_tot_units) AS op_rcpts_total_u,
 SUM(m.sp_rcpts_tot_units) AS sp_rcpts_total_u,
 SUM(m.ty_rcpts_tot_units) AS ty_rcpts_total_u,
 SUM(m.ly_rcpts_tot_units) AS ly_rcpts_total_u,
 SUM(m.ty_rcpts_active_cost_amt) AS ty_rcpts_active_cost_amt,
 SUM(m.sp_rcpts_active_cost_amt) AS sp_rcpts_active_cost_amt,
 SUM(m.op_rcpts_active_cost_amt) AS op_rcpts_active_cost_amt,
 SUM(m.ly_rcpts_active_cost_amt) AS ly_rcpts_active_cost_amt,
 SUM(m.ty_rcpts_active_units) AS ty_rcpts_active_u,
 SUM(m.sp_rcpts_active_units) AS sp_rcpts_active_u,
 SUM(m.op_rcpts_active_units) AS op_rcpts_active_u,
 SUM(m.ly_rcpts_active_units) AS ly_rcpts_active_u,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN m.ty_demand_total_retail_amt
   ELSE 0
   END) AS ty_demand_total_retail_amt_online,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN m.sp_demand_total_retail_amt
   ELSE 0
   END) AS sp_demand_total_retail_amt_online,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN m.op_demand_total_retail_amt
   ELSE 0
   END) AS op_demand_total_retail_amt_online,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN m.ly_demand_total_retail_amt
   ELSE 0
   END) AS ly_demand_total_retail_amt_online,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN CAST(TRUNC(CAST(m.ty_demand_total_units AS FLOAT64)) AS INTEGER)
   ELSE 0
   END) AS ty_demand_total_units_online,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN CAST(TRUNC(CAST(m.sp_demand_total_units AS FLOAT64)) AS INTEGER)
   ELSE 0
   END) AS sp_demand_total_units_online,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN CAST(TRUNC(CAST(m.op_demand_total_units AS FLOAT64)) AS INTEGER)
   ELSE 0
   END) AS op_demand_total_units_online,
 SUM(CASE
   WHEN m.channel_num IN (120, 250)
   THEN CAST(TRUNC(CAST(m.ly_demand_total_units AS FLOAT64)) AS INTEGER)
   ELSE 0
   END) AS ly_demand_total_units_online,
 SUM(m.ty_returns_retail_amt) AS ty_returns_retail_amt,
 SUM(m.sp_returns_retail_amt) AS sp_returns_retail_amt,
 SUM(m.op_returns_retail_amt) AS op_returns_retail_amt,
 SUM(m.ly_returns_retail_amt) AS ly_returns_retail_amt,
 SUM(m.ty_returns_units) AS ty_returns_units,
 SUM(m.sp_returns_units) AS sp_returns_units,
 SUM(m.op_returns_units) AS op_returns_units,
 SUM(m.ly_returns_units) AS ly_returns_units
FROM `{{params.gcp_project_id}}`.t2dl_das_ace_mfp.mfp_banner_country_channel_stg AS m
 LEFT JOIN date_lookup AS d 
 ON m.week_num = d.week_idnt
WHERE m.year = (SELECT MAX(fiscal_year_num)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
 AND m.week_num <= (SELECT MAX(week_idnt)
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
GROUP BY 
 data_source,
 ty_ly_ind,
 week_idnt,
 d.month_idnt,
 m.banner,
 m.country,
 m.channel,
 m.division,
 m.subdivision,
 department,
 m.dept_num,
 price_type,
 category,
 supplier_group;


--COLLECT STATS 	PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group) 		ON MFP


CREATE TEMPORARY TABLE IF NOT EXISTS wbr
AS
SELECT 'WBR' AS data_source,
 ty_ly_ind,
 week_idnt,
 month_idnt,
  CASE
  WHEN LOWER(banner) = LOWER('N')
  THEN 'NORDSTROM'
  WHEN LOWER(banner) = LOWER('NR')
  THEN 'NORDSTROM RACK'
  ELSE banner
  END AS banner,
  SUBSTR('US', 1, 40) AS country,
 chnl_label AS channel,
 division,
 subdivision,
 department,
 dept_idnt AS dept_num,
  CASE
  WHEN LOWER(price_type) = LOWER('P')
  THEN 'PRO'
  WHEN LOWER(price_type) = LOWER('R')
  THEN 'REG'
  WHEN LOWER(price_type) = LOWER('C')
  THEN 'CLR'
  ELSE 'UNKNOWN'
  END AS price_type,
 SUBSTR('NA', 1, 40) AS category,
 SUBSTR('NA', 1, 40) AS supplier_group,
 0 AS apt_net_sls_r,
 0 AS apt_fut_net_sls_r_dollars,
 0 AS apt_net_sls_c,
 0 AS apt_fut_net_sls_c_dollars,
 0 AS apt_net_sls_units,
 0 AS apt_fut_net_sls_c_units,
 0 AS apt_demand_r_online,
 0 AS apt_fut_demand_r_online,
 0 AS apt_eop_ttl_units,
 0 AS apt_eop_ttl_c,
 0 AS apt_fut_eop_ttl_c,
 0 AS apt_ttl_porcpt_c,
 0 AS apt_fut_rcpt_need_c,
 0 AS apt_plan_bop_c,
 0 AS apt_plan_bop_u,
 0 AS apt_plan_eop_u,
 SUM(sales_dollars) AS wbr_sales_dollars,
 SUM(sales_units) AS wbr_sales_units,
 SUM(eoh_units) AS wbr_eoh_units,
 SUM(eoh_cost) AS wbr_eoh_cost,
 SUM(cost_of_goods_sold) AS wbr_sales_cost,
 SUM(sales_pm) AS wbr_sales_pm,
 0 AS cp_bop_total_c,
 0 AS op_bop_total_c,
 0 AS sp_bop_total_c,
 0 AS ty_bop_total_c,
 0 AS ly_bop_total_c,
 0 AS cp_bop_total_u,
 0 AS op_bop_total_u,
 0 AS sp_bop_total_u,
 0 AS ty_bop_total_u,
 0 AS ly_bop_total_u,
 0 AS cp_eop_active_c,
 0 AS op_eop_active_c,
 0 AS sp_eop_active_c,
 0 AS ty_eop_active_c,
 0 AS ly_eop_active_c,
 0 AS cp_eop_active_u,
 0 AS op_eop_active_u,
 0 AS sp_eop_active_u,
 0 AS ty_eop_active_u,
 0 AS ly_eop_active_u,
 0 AS cp_eop_total_c,
 0 AS op_eop_total_c,
 0 AS sp_eop_total_c,
 0 AS ty_eop_total_c,
 0 AS ly_eop_total_c,
 0 AS cp_eop_total_u,
 0 AS op_eop_total_u,
 0 AS sp_eop_total_u,
 0 AS ty_eop_total_u,
 0 AS ly_eop_total_u,
 0 AS cp_gross_margin,
 0 AS op_gross_margin,
 0 AS sp_gross_margin,
 0 AS ty_gross_margin,
 0 AS ly_gross_margin,
 0 AS cp_merch_margin,
 0 AS op_merch_margin,
 0 AS sp_merch_margin,
 0 AS ty_merch_margin,
 0 AS ly_merch_margin,
 0 AS cp_net_sales_c,
 0 AS op_net_sales_c,
 0 AS sp_net_sales_c,
 0 AS ty_net_sales_c,
 0 AS ly_net_sales_c,
 0 AS cp_net_sales_r,
 0 AS op_net_sales_r,
 0 AS sp_net_sales_r,
 0 AS ty_net_sales_r,
 0 AS ly_net_sales_r,
 0 AS cp_net_sales_u,
 0 AS op_net_sales_u,
 0 AS sp_net_sales_u,
 0 AS ty_net_sales_u,
 0 AS ly_net_sales_u,
 0 AS cp_rcpts_total_c,
 0 AS op_rcpts_total_c,
 0 AS sp_rcpts_total_c,
 0 AS ty_rcpts_total_c,
 0 AS ly_rcpts_total_c,
 0 AS cp_rcpts_total_u,
 0 AS op_rcpts_total_u,
 0 AS sp_rcpts_total_u,
 0 AS ty_rcpts_total_u,
 0 AS ly_rcpts_total_u,
 0 AS ty_rcpts_active_cost_amt,
 0 AS sp_rcpts_active_cost_amt,
 0 AS op_rcpts_active_cost_amt,
 0 AS ly_rcpts_active_cost_amt,
 0 AS ty_rcpts_active_u,
 0 AS sp_rcpts_active_u,
 0 AS op_rcpts_active_u,
 0 AS ly_rcpts_active_u,
 0 AS ty_demand_total_retail_amt_online,
 0 AS sp_demand_total_retail_amt_online,
 0 AS op_demand_total_retail_amt_online,
 0 AS ly_demand_total_retail_amt_online,
 0 AS ty_demand_total_units_online,
 0 AS sp_demand_total_units_online,
 0 AS op_demand_total_units_online,
 0 AS ly_demand_total_units_online,
 0 AS ty_returns_retail_amt,
 0 AS sp_returns_retail_amt,
 0 AS op_returns_retail_amt,
 0 AS ly_returns_retail_amt,
 0 AS ty_returns_units,
 0 AS sp_returns_units,
 0 AS op_returns_units,
 0 AS ly_returns_units
FROM `{{params.gcp_project_id}}`.{{params.environment_schema}}.wbr_supplier
WHERE LOWER(ss_ind) = LOWER('SS')
GROUP BY data_source,
 ty_ly_ind,
 week_idnt,
 month_idnt,
 banner,
 country,
 channel,
 division,
 subdivision,
 department,
 dept_num,
 price_type,
 category,
 supplier_group;


--COLLECT STATS 	PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group 	) ON WBR


CREATE TEMPORARY TABLE IF NOT EXISTS apt
AS
SELECT 'APT' AS data_source,
  CASE
  WHEN LOWER(date_ind) = LOWER('TY')
  THEN 'TY'
  WHEN LOWER(date_ind) = LOWER('LY')
  THEN 'LY'
  WHEN LOWER(date_ind) = LOWER('PL MTH')
  THEN 'TY'
  ELSE 'NA'
  END AS ty_ly_ind,
 0 AS week_idnt,
 month_idnt,
  CASE
  WHEN LOWER(banner) = LOWER('NORDSTROM_RACK')
  THEN 'NORDSTROM RACK'
  ELSE banner
  END AS banner,
 channel_country AS country,
 channel_label AS channel,
 division_desc AS division,
 subdivision_desc AS subdivision,
 department_desc AS department,
 department_num AS dept_num,
 SUBSTR('NA', 1, 10) AS price_type,
 category,
 supplier_group,
 SUM(net_sls_r) AS apt_net_sls_r,
 SUM(plan_sales_r) AS apt_fut_net_sls_r_dollars,
 SUM(net_sls_c) AS apt_net_sls_c,
 SUM(plan_sales_c) AS apt_fut_net_sls_c_dollars,
 SUM(net_sls_units) AS apt_net_sls_units,
 SUM(plan_sales_u) AS apt_fut_net_sls_c_units,
 SUM(CASE
   WHEN channel_num IN (120, 250)
   THEN demand_ttl_r
   ELSE 0
   END) AS apt_demand_r_online,
 SUM(CASE
   WHEN channel_num IN (120, 250)
   THEN plan_demand_r
   ELSE 0
   END) AS apt_fut_demand_r_online,
 SUM(eop_ttl_units) AS apt_eop_ttl_units,
 SUM(eop_ttl_c) AS apt_eop_ttl_c,
 SUM(plan_eop_c) AS apt_fut_eop_ttl_c,
 SUM(ttl_porcpt_c) AS apt_ttl_porcpt_c,
 SUM(plan_receipts_c) AS apt_fut_rcpt_need_c,
 SUM(plan_bop_c) AS apt_plan_bop_c,
 SUM(plan_bop_u) AS apt_plan_bop_u,
 SUM(plan_eop_u) AS apt_plan_eop_u,
 0 AS wbr_sales_dollars,
 0 AS wbr_sales_units,
 0 AS wbr_eoh_units,
 0 AS wbr_eoh_cost,
 0 AS wbr_sales_cost,
 0 AS wbr_sales_pm,
 0 AS cp_bop_total_c,
 0 AS op_bop_total_c,
 0 AS sp_bop_total_c,
 0 AS ty_bop_total_c,
 0 AS ly_bop_total_c,
 0 AS cp_bop_total_u,
 0 AS op_bop_total_u,
 0 AS sp_bop_total_u,
 0 AS ty_bop_total_u,
 0 AS ly_bop_total_u,
 0 AS cp_eop_active_c,
 0 AS op_eop_active_c,
 0 AS sp_eop_active_c,
 0 AS ty_eop_active_c,
 0 AS ly_eop_active_c,
 0 AS cp_eop_active_u,
 0 AS op_eop_active_u,
 0 AS sp_eop_active_u,
 0 AS ty_eop_active_u,
 0 AS ly_eop_active_u,
 0 AS cp_eop_total_c,
 0 AS op_eop_total_c,
 0 AS sp_eop_total_c,
 0 AS ty_eop_total_c,
 0 AS ly_eop_total_c,
 0 AS cp_eop_total_u,
 0 AS op_eop_total_u,
 0 AS sp_eop_total_u,
 0 AS ty_eop_total_u,
 0 AS ly_eop_total_u,
 0 AS cp_gross_margin,
 0 AS op_gross_margin,
 0 AS sp_gross_margin,
 0 AS ty_gross_margin,
 0 AS ly_gross_margin,
 0 AS cp_merch_margin,
 0 AS op_merch_margin,
 0 AS sp_merch_margin,
 0 AS ty_merch_margin,
 0 AS ly_merch_margin,
 0 AS cp_net_sales_c,
 0 AS op_net_sales_c,
 0 AS sp_net_sales_c,
 0 AS ty_net_sales_c,
 0 AS ly_net_sales_c,
 0 AS cp_net_sales_r,
 0 AS op_net_sales_r,
 0 AS sp_net_sales_r,
 0 AS ty_net_sales_r,
 0 AS ly_net_sales_r,
 0 AS cp_net_sales_u,
 0 AS op_net_sales_u,
 0 AS sp_net_sales_u,
 0 AS ty_net_sales_u,
 0 AS ly_net_sales_u,
 0 AS cp_rcpts_total_c,
 0 AS op_rcpts_total_c,
 0 AS sp_rcpts_total_c,
 0 AS ty_rcpts_total_c,
 0 AS ly_rcpts_total_c,
 0 AS cp_rcpts_total_u,
 0 AS op_rcpts_total_u,
 0 AS sp_rcpts_total_u,
 0 AS ty_rcpts_total_u,
 0 AS ly_rcpts_total_u,
 0 AS ty_rcpts_active_cost_amt,
 0 AS sp_rcpts_active_cost_amt,
 0 AS op_rcpts_active_cost_amt,
 0 AS ly_rcpts_active_cost_amt,
 0 AS ty_rcpts_active_u,
 0 AS sp_rcpts_active_u,
 0 AS op_rcpts_active_u,
 0 AS ly_rcpts_active_u,
 0 AS ty_demand_total_retail_amt_online,
 0 AS sp_demand_total_retail_amt_online,
 0 AS op_demand_total_retail_amt_online,
 0 AS ly_demand_total_retail_amt_online,
 0 AS ty_demand_total_units_online,
 0 AS sp_demand_total_units_online,
 0 AS op_demand_total_units_online,
 0 AS ly_demand_total_units_online,
 0 AS ty_returns_retail_amt,
 0 AS sp_returns_retail_amt,
 0 AS op_returns_retail_amt,
 0 AS ly_returns_retail_amt,
 0 AS ty_returns_units,
 0 AS sp_returns_units,
 0 AS op_returns_units,
 0 AS ly_returns_units
FROM `{{params.gcp_project_id}}`.t2dl_das_apt_cost_reporting.apt_is_supplier
WHERE month_idnt_aligned IN 
(SELECT DISTINCT month_idnt
   FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
   WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num) AS A3977768
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))
    AND month_idnt < (SELECT MIN(month_idnt) AS A726271021
      FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
      WHERE month_start_day_date > CURRENT_DATE('PST8PDT')))
 AND LOWER(date_ind) IN (LOWER('TY'), LOWER('PL MTH'), LOWER('LY'))
GROUP BY data_source,
 ty_ly_ind,
 week_idnt,
 month_idnt,
 banner,
 country,
 channel,
 division,
 subdivision,
 department,
 dept_num,
 price_type,
 category,
 supplier_group;


--COLLECT STATS 	PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group 	) ON APT


CREATE TEMPORARY TABLE IF NOT EXISTS union_table
AS
SELECT Temp.data_source,
 COALESCE(Temp.week_idnt, 0) AS week_idnt,
 COALESCE(Temp.month_idnt, 0) AS month_idnt,
 COALESCE(wl1.week_454_label, 'NA') AS fiscal_week,
 COALESCE(FORMAT('%6d', wl1.fiscal_week_num), '0') AS fiscal_week_num,
 COALESCE(FORMAT('%11d', wl2.fiscal_month_num), '0') AS fiscal_month_num,
     TRIM(FORMAT('%11d', wl2.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', wl2.fiscal_month_num)) || ' ' || TRIM(wl2.month_abrv
   ) AS fiscal_month,
 COALESCE(wl2.fiscal_year_num, 0) AS fiscal_year,
 COALESCE(Temp.ty_ly_ind, 'NA') AS ty_ly_ind,
 COALESCE(Temp.banner, 'NA') AS banner,
 COALESCE(Temp.country, 'NA') AS country,
 COALESCE(Temp.channel, 'NA') AS channel,
 COALESCE(Temp.division, 'NA') AS division,
 COALESCE(Temp.subdivision, 'NA') AS subdivision,
 COALESCE(Temp.department, 'NA') AS department,
 COALESCE(Temp.dept_num, 0) AS dept_num,
 COALESCE(Temp.price_type, 'NA') AS price_type,
 COALESCE(Temp.category, 'NA') AS category,
 COALESCE(Temp.supplier_group, 'NA') AS supplier_group,
 SUM(Temp.apt_net_sls_r) AS apt_net_sls_r,
 SUM(Temp.apt_fut_net_sls_r_dollars) AS apt_fut_net_sls_r_dollars,
 SUM(Temp.apt_net_sls_c) AS apt_net_sls_c,
 SUM(Temp.apt_fut_net_sls_c_dollars) AS apt_fut_net_sls_c_dollars,
 SUM(Temp.apt_net_sls_units) AS apt_net_sls_units,
 SUM(Temp.apt_fut_net_sls_c_units) AS apt_fut_net_sls_c_units,
 SUM(Temp.apt_demand_r_online) AS apt_demand_r_online,
 SUM(Temp.apt_fut_demand_r_online) AS apt_fut_demand_r_online,
 SUM(Temp.apt_eop_ttl_units) AS apt_eop_ttl_units,
 SUM(Temp.apt_eop_ttl_c) AS apt_eop_ttl_c,
 SUM(Temp.apt_fut_eop_ttl_c) AS apt_fut_eop_ttl_c,
 SUM(Temp.apt_ttl_porcpt_c) AS apt_ttl_porcpt_c,
 SUM(Temp.apt_fut_rcpt_need_c) AS apt_fut_rcpt_need_c,
 SUM(Temp.apt_plan_bop_c) AS apt_plan_bop_c,
 SUM(Temp.apt_plan_bop_u) AS apt_plan_bop_u,
 SUM(Temp.apt_plan_eop_u) AS apt_plan_eop_u,
 SUM(Temp.wbr_sales_dollars) AS wbr_sales_dollars,
 SUM(Temp.wbr_sales_units) AS wbr_sales_units,
 SUM(Temp.wbr_eoh_units) AS wbr_eoh_units,
 SUM(Temp.wbr_eoh_cost) AS wbr_eoh_cost,
 SUM(Temp.wbr_sales_cost) AS wbr_sales_cost,
 SUM(Temp.wbr_sales_pm) AS wbr_sales_pm,
 SUM(Temp.cp_bop_total_c) AS cp_bop_total_c,
 SUM(Temp.op_bop_total_c) AS op_bop_total_c,
 SUM(Temp.sp_bop_total_c) AS sp_bop_total_c,
 SUM(Temp.ty_bop_total_c) AS ty_bop_total_c,
 SUM(Temp.ly_bop_total_c) AS ly_bop_total_c,
 SUM(Temp.cp_bop_total_u) AS cp_bop_total_u,
 SUM(Temp.op_bop_total_u) AS op_bop_total_u,
 SUM(Temp.sp_bop_total_u) AS sp_bop_total_u,
 SUM(Temp.ty_bop_total_u) AS ty_bop_total_u,
 SUM(Temp.ly_bop_total_u) AS ly_bop_total_u,
 SUM(Temp.cp_eop_active_c) AS cp_eop_active_c,
 SUM(Temp.op_eop_active_c) AS op_eop_active_c,
 SUM(Temp.sp_eop_active_c) AS sp_eop_active_c,
 SUM(Temp.ty_eop_active_c) AS ty_eop_active_c,
 SUM(Temp.ly_eop_active_c) AS ly_eop_active_c,
 SUM(Temp.cp_eop_active_u) AS cp_eop_active_u,
 SUM(Temp.op_eop_active_u) AS op_eop_active_u,
 SUM(Temp.sp_eop_active_u) AS sp_eop_active_u,
 SUM(Temp.ty_eop_active_u) AS ty_eop_active_u,
 SUM(Temp.ly_eop_active_u) AS ly_eop_active_u,
 SUM(Temp.cp_eop_total_c) AS cp_eop_total_c,
 SUM(Temp.op_eop_total_c) AS op_eop_total_c,
 SUM(Temp.sp_eop_total_c) AS sp_eop_total_c,
 SUM(Temp.ty_eop_total_c) AS ty_eop_total_c,
 SUM(Temp.ly_eop_total_c) AS ly_eop_total_c,
 SUM(Temp.cp_eop_total_u) AS cp_eop_total_u,
 SUM(Temp.op_eop_total_u) AS op_eop_total_u,
 SUM(Temp.sp_eop_total_u) AS sp_eop_total_u,
 SUM(Temp.ty_eop_total_u) AS ty_eop_total_u,
 SUM(Temp.ly_eop_total_u) AS ly_eop_total_u,
 SUM(Temp.cp_gross_margin) AS cp_gross_margin,
 SUM(Temp.op_gross_margin) AS op_gross_margin,
 SUM(Temp.sp_gross_margin) AS sp_gross_margin,
 SUM(Temp.ty_gross_margin) AS ty_gross_margin,
 SUM(Temp.ly_gross_margin) AS ly_gross_margin,
 SUM(Temp.cp_merch_margin) AS cp_merch_margin,
 SUM(Temp.op_merch_margin) AS op_merch_margin,
 SUM(Temp.sp_merch_margin) AS sp_merch_margin,
 SUM(Temp.ty_merch_margin) AS ty_merch_margin,
 SUM(Temp.ly_merch_margin) AS ly_merch_margin,
 SUM(Temp.cp_net_sales_c) AS cp_net_sales_c,
 SUM(Temp.op_net_sales_c) AS op_net_sales_c,
 SUM(Temp.sp_net_sales_c) AS sp_net_sales_c,
 SUM(Temp.ty_net_sales_c) AS ty_net_sales_c,
 SUM(Temp.ly_net_sales_c) AS ly_net_sales_c,
 SUM(Temp.cp_net_sales_r) AS cp_net_sales_r,
 SUM(Temp.op_net_sales_r) AS op_net_sales_r,
 SUM(Temp.sp_net_sales_r) AS sp_net_sales_r,
 SUM(Temp.ty_net_sales_r) AS ty_net_sales_r,
 SUM(Temp.ly_net_sales_r) AS ly_net_sales_r,
 SUM(Temp.cp_net_sales_u) AS cp_net_sales_u,
 SUM(Temp.op_net_sales_u) AS op_net_sales_u,
 SUM(Temp.sp_net_sales_u) AS sp_net_sales_u,
 SUM(Temp.ty_net_sales_u) AS ty_net_sales_u,
 SUM(Temp.ly_net_sales_u) AS ly_net_sales_u,
 SUM(Temp.cp_rcpts_total_c) AS cp_rcpts_total_c,
 SUM(Temp.op_rcpts_total_c) AS op_rcpts_total_c,
 SUM(Temp.sp_rcpts_total_c) AS sp_rcpts_total_c,
 SUM(Temp.ty_rcpts_total_c) AS ty_rcpts_total_c,
 SUM(Temp.ly_rcpts_total_c) AS ly_rcpts_total_c,
 SUM(Temp.cp_rcpts_total_u) AS cp_rcpts_total_u,
 SUM(Temp.op_rcpts_total_u) AS op_rcpts_total_u,
 SUM(Temp.sp_rcpts_total_u) AS sp_rcpts_total_u,
 SUM(Temp.ty_rcpts_total_u) AS ty_rcpts_total_u,
 SUM(Temp.ly_rcpts_total_u) AS ly_rcpts_total_u,
 SUM(Temp.ty_rcpts_active_cost_amt) AS ty_rcpts_active_cost_amt,
 SUM(Temp.sp_rcpts_active_cost_amt) AS sp_rcpts_active_cost_amt,
 SUM(Temp.op_rcpts_active_cost_amt) AS op_rcpts_active_cost_amt,
 SUM(Temp.ly_rcpts_active_cost_amt) AS ly_rcpts_active_cost_amt,
 SUM(Temp.ty_rcpts_active_u) AS ty_rcpts_active_u,
 SUM(Temp.sp_rcpts_active_u) AS sp_rcpts_active_u,
 SUM(Temp.op_rcpts_active_u) AS op_rcpts_active_u,
 SUM(Temp.ly_rcpts_active_u) AS ly_rcpts_active_u,
 SUM(Temp.ty_demand_total_retail_amt_online) AS ty_demand_total_retail_amt_online,
 SUM(Temp.sp_demand_total_retail_amt_online) AS sp_demand_total_retail_amt_online,
 SUM(Temp.op_demand_total_retail_amt_online) AS op_demand_total_retail_amt_online,
 SUM(Temp.ly_demand_total_retail_amt_online) AS ly_demand_total_retail_amt_online,
 SUM(Temp.ty_demand_total_units_online) AS ty_demand_total_units_online,
 SUM(Temp.sp_demand_total_units_online) AS sp_demand_total_units_online,
 SUM(Temp.op_demand_total_units_online) AS op_demand_total_units_online,
 SUM(Temp.ly_demand_total_units_online) AS ly_demand_total_units_online,
 SUM(Temp.ty_returns_retail_amt) AS ty_returns_retail_amt,
 SUM(Temp.sp_returns_retail_amt) AS sp_returns_retail_amt,
 SUM(Temp.op_returns_retail_amt) AS op_returns_retail_amt,
 SUM(Temp.ly_returns_retail_amt) AS ly_returns_retail_amt,
 SUM(Temp.ty_returns_units) AS ty_returns_units,
 SUM(Temp.sp_returns_units) AS sp_returns_units,
 SUM(Temp.op_returns_units) AS op_returns_units,
 SUM(Temp.ly_returns_units) AS ly_returns_units
FROM (SELECT data_source,
    ty_ly_ind,
    week_idnt,
    month_idnt,
    banner,
    country,
    channel,
    division,
    subdivision,
    department,
    dept_num,
    price_type,
    category,
    supplier_group,
    apt_net_sls_r,
    apt_fut_net_sls_r_dollars,
    apt_net_sls_c,
    apt_fut_net_sls_c_dollars,
    apt_net_sls_units,
    apt_fut_net_sls_c_units,
    apt_demand_r_online,
    apt_fut_demand_r_online,
    apt_eop_ttl_units,
    apt_eop_ttl_c,
    apt_fut_eop_ttl_c,
    apt_ttl_porcpt_c,
    apt_fut_rcpt_need_c,
    apt_plan_bop_c,
    apt_plan_bop_u,
    apt_plan_eop_u,
    wbr_sales_dollars,
    wbr_sales_units,
    wbr_eoh_units,
    wbr_eoh_cost,
    wbr_sales_cost,
    wbr_sales_pm,
    cp_bop_total_c,
    op_bop_total_c,
    sp_bop_total_c,
    ty_bop_total_c,
    ly_bop_total_c,
    cp_bop_total_u,
    op_bop_total_u,
    sp_bop_total_u,
    ty_bop_total_u,
    ly_bop_total_u,
    cp_eop_active_c,
    op_eop_active_c,
    sp_eop_active_c,
    ty_eop_active_c,
    ly_eop_active_c,
    cp_eop_active_u,
    op_eop_active_u,
    sp_eop_active_u,
    ty_eop_active_u,
    ly_eop_active_u,
    cp_eop_total_c,
    op_eop_total_c,
    sp_eop_total_c,
    ty_eop_total_c,
    ly_eop_total_c,
    cp_eop_total_u,
    op_eop_total_u,
    sp_eop_total_u,
    ty_eop_total_u,
    ly_eop_total_u,
    cp_gross_margin,
    op_gross_margin,
    sp_gross_margin,
    ty_gross_margin,
    ly_gross_margin,
    cp_merch_margin,
    op_merch_margin,
    sp_merch_margin,
    ty_merch_margin,
    ly_merch_margin,
    cp_net_sales_c,
    op_net_sales_c,
    sp_net_sales_c,
    ty_net_sales_c,
    ly_net_sales_c,
    cp_net_sales_r,
    op_net_sales_r,
    sp_net_sales_r,
    ty_net_sales_r,
    ly_net_sales_r,
    cp_net_sales_u,
    op_net_sales_u,
    sp_net_sales_u,
    ty_net_sales_u,
    ly_net_sales_u,
    cp_rcpts_total_c,
    op_rcpts_total_c,
    sp_rcpts_total_c,
    ty_rcpts_total_c,
    ly_rcpts_total_c,
    cp_rcpts_total_u,
    op_rcpts_total_u,
    sp_rcpts_total_u,
    ty_rcpts_total_u,
    ly_rcpts_total_u,
    ty_rcpts_active_cost_amt,
    sp_rcpts_active_cost_amt,
    op_rcpts_active_cost_amt,
    ly_rcpts_active_cost_amt,
    ty_rcpts_active_u,
    sp_rcpts_active_u,
    op_rcpts_active_u,
    ly_rcpts_active_u,
    ty_demand_total_retail_amt_online,
    sp_demand_total_retail_amt_online,
    op_demand_total_retail_amt_online,
    ly_demand_total_retail_amt_online,
    ty_demand_total_units_online,
    sp_demand_total_units_online,
    op_demand_total_units_online,
    ly_demand_total_units_online,
    ty_returns_retail_amt,
    sp_returns_retail_amt,
    op_returns_retail_amt,
    ly_returns_retail_amt,
    ty_returns_units,
    sp_returns_units,
    op_returns_units,
    ly_returns_units
   FROM mfp
   UNION ALL
   SELECT data_source,
    ty_ly_ind,
    week_idnt,
    month_idnt,
    banner,
    country,
    channel,
    division,
    subdivision,
    department,
    dept_num,
    price_type,
    category,
    supplier_group,
    apt_net_sls_r,
    apt_fut_net_sls_r_dollars,
    apt_net_sls_c,
    apt_fut_net_sls_c_dollars,
    apt_net_sls_units,
    apt_fut_net_sls_c_units,
    apt_demand_r_online,
    apt_fut_demand_r_online,
    apt_eop_ttl_units,
    apt_eop_ttl_c,
    apt_fut_eop_ttl_c,
    apt_ttl_porcpt_c,
    apt_fut_rcpt_need_c,
    apt_plan_bop_c,
    apt_plan_bop_u,
    apt_plan_eop_u,
    wbr_sales_dollars,
    wbr_sales_units,
    wbr_eoh_units,
    wbr_eoh_cost,
    wbr_sales_cost,
    wbr_sales_pm,
    cp_bop_total_c,
    op_bop_total_c,
    sp_bop_total_c,
    ty_bop_total_c,
    ly_bop_total_c,
    cp_bop_total_u,
    op_bop_total_u,
    sp_bop_total_u,
    ty_bop_total_u,
    ly_bop_total_u,
    cp_eop_active_c,
    op_eop_active_c,
    sp_eop_active_c,
    ty_eop_active_c,
    ly_eop_active_c,
    cp_eop_active_u,
    op_eop_active_u,
    sp_eop_active_u,
    ty_eop_active_u,
    ly_eop_active_u,
    cp_eop_total_c,
    op_eop_total_c,
    sp_eop_total_c,
    ty_eop_total_c,
    ly_eop_total_c,
    cp_eop_total_u,
    op_eop_total_u,
    sp_eop_total_u,
    ty_eop_total_u,
    ly_eop_total_u,
    cp_gross_margin,
    op_gross_margin,
    sp_gross_margin,
    ty_gross_margin,
    ly_gross_margin,
    cp_merch_margin,
    op_merch_margin,
    sp_merch_margin,
    ty_merch_margin,
    ly_merch_margin,
    cp_net_sales_c,
    op_net_sales_c,
    sp_net_sales_c,
    ty_net_sales_c,
    ly_net_sales_c,
    cp_net_sales_r,
    op_net_sales_r,
    sp_net_sales_r,
    ty_net_sales_r,
    ly_net_sales_r,
    cp_net_sales_u,
    op_net_sales_u,
    sp_net_sales_u,
    ty_net_sales_u,
    ly_net_sales_u,
    cp_rcpts_total_c,
    op_rcpts_total_c,
    sp_rcpts_total_c,
    ty_rcpts_total_c,
    ly_rcpts_total_c,
    cp_rcpts_total_u,
    op_rcpts_total_u,
    sp_rcpts_total_u,
    ty_rcpts_total_u,
    ly_rcpts_total_u,
    ty_rcpts_active_cost_amt,
    sp_rcpts_active_cost_amt,
    op_rcpts_active_cost_amt,
    ly_rcpts_active_cost_amt,
    ty_rcpts_active_u,
    sp_rcpts_active_u,
    op_rcpts_active_u,
    ly_rcpts_active_u,
    ty_demand_total_retail_amt_online,
    sp_demand_total_retail_amt_online,
    op_demand_total_retail_amt_online,
    ly_demand_total_retail_amt_online,
    ty_demand_total_units_online,
    sp_demand_total_units_online,
    op_demand_total_units_online,
    ly_demand_total_units_online,
    ty_returns_retail_amt,
    sp_returns_retail_amt,
    op_returns_retail_amt,
    ly_returns_retail_amt,
    ty_returns_units,
    sp_returns_units,
    op_returns_units,
    ly_returns_units
   FROM wbr
   UNION ALL
   SELECT data_source,
    ty_ly_ind,
    week_idnt,
    month_idnt,
    banner,
    country,
    channel,
    division,
    subdivision,
    department,
    dept_num,
    price_type,
    category,
    supplier_group,
    apt_net_sls_r,
    apt_fut_net_sls_r_dollars,
    apt_net_sls_c,
    apt_fut_net_sls_c_dollars,
    apt_net_sls_units,
    apt_fut_net_sls_c_units,
    apt_demand_r_online,
    apt_fut_demand_r_online,
    apt_eop_ttl_units,
    apt_eop_ttl_c,
    apt_fut_eop_ttl_c,
    apt_ttl_porcpt_c,
    apt_fut_rcpt_need_c,
    apt_plan_bop_c,
    apt_plan_bop_u,
    apt_plan_eop_u,
    wbr_sales_dollars,
    wbr_sales_units,
    wbr_eoh_units,
    wbr_eoh_cost,
    wbr_sales_cost,
    wbr_sales_pm,
    cp_bop_total_c,
    op_bop_total_c,
    sp_bop_total_c,
    ty_bop_total_c,
    ly_bop_total_c,
    cp_bop_total_u,
    op_bop_total_u,
    sp_bop_total_u,
    ty_bop_total_u,
    ly_bop_total_u,
    cp_eop_active_c,
    op_eop_active_c,
    sp_eop_active_c,
    ty_eop_active_c,
    ly_eop_active_c,
    cp_eop_active_u,
    op_eop_active_u,
    sp_eop_active_u,
    ty_eop_active_u,
    ly_eop_active_u,
    cp_eop_total_c,
    op_eop_total_c,
    sp_eop_total_c,
    ty_eop_total_c,
    ly_eop_total_c,
    cp_eop_total_u,
    op_eop_total_u,
    sp_eop_total_u,
    ty_eop_total_u,
    ly_eop_total_u,
    cp_gross_margin,
    op_gross_margin,
    sp_gross_margin,
    ty_gross_margin,
    ly_gross_margin,
    cp_merch_margin,
    op_merch_margin,
    sp_merch_margin,
    ty_merch_margin,
    ly_merch_margin,
    cp_net_sales_c,
    op_net_sales_c,
    sp_net_sales_c,
    ty_net_sales_c,
    ly_net_sales_c,
    cp_net_sales_r,
    op_net_sales_r,
    sp_net_sales_r,
    ty_net_sales_r,
    ly_net_sales_r,
    cp_net_sales_u,
    op_net_sales_u,
    sp_net_sales_u,
    ty_net_sales_u,
    ly_net_sales_u,
    cp_rcpts_total_c,
    op_rcpts_total_c,
    sp_rcpts_total_c,
    ty_rcpts_total_c,
    ly_rcpts_total_c,
    cp_rcpts_total_u,
    op_rcpts_total_u,
    sp_rcpts_total_u,
    ty_rcpts_total_u,
    ly_rcpts_total_u,
    ty_rcpts_active_cost_amt,
    sp_rcpts_active_cost_amt,
    op_rcpts_active_cost_amt,
    ly_rcpts_active_cost_amt,
    ty_rcpts_active_u,
    sp_rcpts_active_u,
    op_rcpts_active_u,
    ly_rcpts_active_u,
    ty_demand_total_retail_amt_online,
    sp_demand_total_retail_amt_online,
    op_demand_total_retail_amt_online,
    ly_demand_total_retail_amt_online,
    ty_demand_total_units_online,
    sp_demand_total_units_online,
    op_demand_total_units_online,
    ly_demand_total_units_online,
    ty_returns_retail_amt,
    sp_returns_retail_amt,
    op_returns_retail_amt,
    ly_returns_retail_amt,
    ty_returns_units,
    sp_returns_units,
    op_returns_units,
    ly_returns_units
   FROM apt) AS Temp
 LEFT JOIN (SELECT DISTINCT week_idnt,
   fiscal_week_num,
   week_454_label
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS wl1 
  ON Temp.week_idnt = wl1.week_idnt
 LEFT JOIN 
 (SELECT DISTINCT month_idnt,
   fiscal_month_num,
   month_abrv,
   fiscal_year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim) AS wl2 
  ON Temp.month_idnt = wl2.month_idnt
GROUP BY Temp.data_source,
 week_idnt,
 month_idnt,
 fiscal_week,
 fiscal_week_num,
 fiscal_month_num,
 fiscal_month,
 fiscal_year,
 ty_ly_ind,
 banner,
 country,
 channel,
 division,
 subdivision,
 department,
 dept_num,
 price_type,
 category,
 supplier_group;


--COLLECT STATS 		PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group 		) 	ON union_table


-- get realigned fiscal week/month on final table


CREATE TEMPORARY TABLE IF NOT EXISTS final_table
AS
SELECT ut.data_source,
 ut.week_idnt,
 ut.month_idnt,
 ut.fiscal_week,
 ut.fiscal_month,
 ut.fiscal_year,
 ut.ty_ly_ind,
 date_align.week_454_label AS fiscal_week_realigned,
     TRIM(FORMAT('%11d', date_align2.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', date_align2.fiscal_month_num)) ||
   ' ' || TRIM(date_align2.month_abrv) AS fiscal_month_realigned,
 ut.banner,
 ut.country,
 ut.channel,
 ut.division,
 ut.subdivision,
 ut.department,
 ut.price_type,
 ut.category,
 ut.supplier_group,
 aor.general_merch_manager_executive_vice_president,
 aor.div_merch_manager_senior_vice_president,
 aor.div_merch_manager_vice_president,
 aor.merch_director,
 aor.buyer,
 aor.merch_planning_executive_vice_president,
 aor.merch_planning_senior_vice_president,
 aor.merch_planning_vice_president,
 aor.merch_planning_director_manager,
 aor.assortment_planner,
 ut.apt_net_sls_r,
 ut.apt_fut_net_sls_r_dollars,
 ut.apt_net_sls_c,
 ut.apt_fut_net_sls_c_dollars,
 ut.apt_net_sls_units,
 ut.apt_fut_net_sls_c_units,
 ut.apt_demand_r_online,
 ut.apt_fut_demand_r_online,
 ut.apt_eop_ttl_units,
 ut.apt_eop_ttl_c,
 ut.apt_fut_eop_ttl_c,
 ut.apt_ttl_porcpt_c,
 ut.apt_fut_rcpt_need_c,
 ut.apt_plan_bop_c,
 ut.apt_plan_bop_u,
 ut.apt_plan_eop_u,
 ut.wbr_sales_dollars,
 ut.wbr_sales_units,
 ut.wbr_eoh_units,
 ut.wbr_eoh_cost,
 ut.wbr_sales_cost,
 ut.wbr_sales_pm,
 ut.cp_bop_total_c,
 ut.op_bop_total_c,
 ut.sp_bop_total_c,
 ut.ty_bop_total_c,
 ut.ly_bop_total_c,
 ut.cp_bop_total_u,
 ut.op_bop_total_u,
 ut.sp_bop_total_u,
 ut.ty_bop_total_u,
 ut.ly_bop_total_u,
 ut.cp_eop_active_c,
 ut.op_eop_active_c,
 ut.sp_eop_active_c,
 ut.ty_eop_active_c,
 ut.ly_eop_active_c,
 ut.cp_eop_active_u,
 ut.op_eop_active_u,
 ut.sp_eop_active_u,
 ut.ty_eop_active_u,
 ut.ly_eop_active_u,
 ut.cp_eop_total_c,
 ut.op_eop_total_c,
 ut.sp_eop_total_c,
 ut.ty_eop_total_c,
 ut.ly_eop_total_c,
 ut.cp_eop_total_u,
 ut.op_eop_total_u,
 ut.sp_eop_total_u,
 ut.ty_eop_total_u,
 ut.ly_eop_total_u,
 ut.cp_gross_margin,
 ut.op_gross_margin,
 ut.sp_gross_margin,
 ut.ty_gross_margin,
 ut.ly_gross_margin,
 ut.cp_merch_margin,
 ut.op_merch_margin,
 ut.sp_merch_margin,
 ut.ty_merch_margin,
 ut.ly_merch_margin,
 ut.cp_net_sales_c,
 ut.op_net_sales_c,
 ut.sp_net_sales_c,
 ut.ty_net_sales_c,
 ut.ly_net_sales_c,
 ut.cp_net_sales_r,
 ut.op_net_sales_r,
 ut.sp_net_sales_r,
 ut.ty_net_sales_r,
 ut.ly_net_sales_r,
 ut.cp_net_sales_u,
 ut.op_net_sales_u,
 ut.sp_net_sales_u,
 ut.ty_net_sales_u,
 ut.ly_net_sales_u,
 ut.cp_rcpts_total_c,
 ut.op_rcpts_total_c,
 ut.sp_rcpts_total_c,
 ut.ty_rcpts_total_c,
 ut.ly_rcpts_total_c,
 ut.cp_rcpts_total_u,
 ut.op_rcpts_total_u,
 ut.sp_rcpts_total_u,
 ut.ty_rcpts_total_u,
 ut.ly_rcpts_total_u,
 ut.ty_rcpts_active_cost_amt,
 ut.sp_rcpts_active_cost_amt,
 ut.op_rcpts_active_cost_amt,
 ut.ly_rcpts_active_cost_amt,
 ut.ty_rcpts_active_u,
 ut.sp_rcpts_active_u,
 ut.op_rcpts_active_u,
 ut.ly_rcpts_active_u,
 ut.ty_demand_total_retail_amt_online,
 ut.sp_demand_total_retail_amt_online,
 ut.op_demand_total_retail_amt_online,
 ut.ly_demand_total_retail_amt_online,
 ut.ty_demand_total_units_online,
 ut.sp_demand_total_units_online,
 ut.op_demand_total_units_online,
 ut.ly_demand_total_units_online,
 ut.ty_returns_retail_amt,
 ut.sp_returns_retail_amt,
 ut.op_returns_retail_amt,
 ut.ly_returns_retail_amt,
 ut.ty_returns_units,
 ut.sp_returns_units,
 ut.op_returns_units,
 ut.ly_returns_units
FROM union_table AS ut
 LEFT JOIN (SELECT DISTINCT week_454_label,
   fiscal_week_num,
   fiscal_month_num,
   month_abrv,
   fiscal_year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num)
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))) AS date_align 
     ON ut.fiscal_week_num = CAST(date_align.fiscal_week_num AS STRING)
  
 LEFT JOIN (SELECT DISTINCT fiscal_month_num,
   month_abrv,
   fiscal_year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE fiscal_year_num = (SELECT MAX(fiscal_year_num)
     FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
     WHERE week_end_day_date < CURRENT_DATE('PST8PDT'))) AS date_align2 
     ON CAST(ut.fiscal_month_num AS FLOAT64) = date_align2.fiscal_month_num
  
 LEFT JOIN aor 
 ON LOWER(ut.banner) = LOWER(aor.banner) 
 AND ut.dept_num = aor.dept_num;


--COLLECT STATS 		PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group 		) 	ON final_table


------------------------------------------------------------- END TEMPORARY TABLES -----------------------------------------------------------------------


---------------------------------------------------------------- START MAIN QUERY ----------------------------------------------------------------

truncate table `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_executive_weekly ;






INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.merch_executive_weekly
	SELECT
	data_source
		, week_idnt
		, month_idnt
		, fiscal_week
		, fiscal_month
		, fiscal_year
		, ty_ly_ind
		, fiscal_week_realigned
		, fiscal_month_realigned
		, banner
		, country
		, channel
		, division
		, subdivision
		, department
		, price_type
		, category
		, supplier_group
		, general_merch_manager_executive_vice_president
		, div_merch_manager_senior_vice_president
		, div_merch_manager_vice_president
		, merch_director
		, buyer
		, merch_planning_executive_vice_president
		, merch_planning_senior_vice_president
		, merch_planning_vice_president
		, merch_planning_director_manager
		, assortment_planner
		, apt_net_sls_r
		, apt_fut_net_sls_r_dollars
		, apt_net_sls_c
		, apt_fut_net_sls_c_dollars
		, apt_net_sls_units
		, apt_fut_net_sls_c_units
		, apt_demand_r_online
		, apt_fut_demand_r_online
		, apt_eop_ttl_units
		, apt_eop_ttl_c
		, apt_fut_eop_ttl_c
		, apt_ttl_porcpt_c
		, apt_fut_rcpt_need_c
		, apt_plan_bop_c
		, apt_plan_bop_u
		, apt_plan_eop_u
		, wbr_sales_dollars
		, cast(trunc(wbr_sales_units) as int64) 
		, cast(trunc(wbr_eoh_units) as int64)
		, wbr_eoh_cost
		, wbr_sales_cost
		, wbr_sales_pm
		, cp_bop_total_c
		, op_bop_total_c
		, sp_bop_total_c
		, ty_bop_total_c
		, ly_bop_total_c
		, cp_bop_total_u
		, op_bop_total_u
		, sp_bop_total_u
		, ty_bop_total_u
		, ly_bop_total_u
		, cp_eop_active_c
		, op_eop_active_c
		, sp_eop_active_c
		, ty_eop_active_c
		, ly_eop_active_c
		, cp_eop_active_u
		, op_eop_active_u
		, sp_eop_active_u
		, ty_eop_active_u
		, ly_eop_active_u
		, cp_eop_total_c
		, op_eop_total_c
		, sp_eop_total_c
		, ty_eop_total_c
		, ly_eop_total_c
		, cp_eop_total_u
		, op_eop_total_u
		, sp_eop_total_u
		, ty_eop_total_u
		, ly_eop_total_u
		, cp_gross_margin
		, op_gross_margin
		, sp_gross_margin
		, ty_gross_margin
		, ly_gross_margin
		, cp_merch_margin
		, op_merch_margin
		, sp_merch_margin
		, ty_merch_margin
		, ly_merch_margin
		, cp_net_sales_c
		, op_net_sales_c
		, sp_net_sales_c
		, ty_net_sales_c
		, ly_net_sales_c
		, cp_net_sales_r
		, op_net_sales_r
		, sp_net_sales_r
		, ty_net_sales_r
		, ly_net_sales_r
		, cp_net_sales_u
		, op_net_sales_u
		, sp_net_sales_u
		, ty_net_sales_u
		, ly_net_sales_u
		, cp_rcpts_total_c
		, op_rcpts_total_c
		, sp_rcpts_total_c
		, ty_rcpts_total_c
		, ly_rcpts_total_c
		, cp_rcpts_total_u
		, op_rcpts_total_u
		, sp_rcpts_total_u
		, ty_rcpts_total_u
		, ly_rcpts_total_u
		, ty_rcpts_active_cost_amt
		, sp_rcpts_active_cost_amt
		, op_rcpts_active_cost_amt
		, ly_rcpts_active_cost_amt
		, ty_rcpts_active_u
		, sp_rcpts_active_u
		, op_rcpts_active_u
		, ly_rcpts_active_u
		, ty_demand_total_retail_amt_online
		, sp_demand_total_retail_amt_online
		, op_demand_total_retail_amt_online
		, ly_demand_total_retail_amt_online
		, ty_demand_total_units_online
		, sp_demand_total_units_online
		, op_demand_total_units_online
		, ly_demand_total_units_online
		, ty_returns_retail_amt
		, sp_returns_retail_amt
		, op_returns_retail_amt
		, ly_returns_retail_amt
		, ty_returns_units
		, sp_returns_units
		, op_returns_units
		, ly_returns_units
		, timestamp(current_datetime('PST8PDT')) as process_tmstp
    , `{{params.gcp_project_id}}`.JWN_UDF.DEFAULT_TZ_PST() as process_tmstp_tz
	FROM final_table
;






--COLLECT STATISTICS     PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group )    on T2DL_DAS_SELECTION.merch_executive_weekly


---------------------------------------------------------------- END MAIN QUERY -------------------------------------------------------------------------