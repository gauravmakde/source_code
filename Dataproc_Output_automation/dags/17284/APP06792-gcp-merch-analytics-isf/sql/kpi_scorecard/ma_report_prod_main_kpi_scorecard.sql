/**********************************************************************************************************
Name:						Merch KPI Scorecard
APPID-Name:					APP09125
Purpose:					Inserts data in {environment_schema} table for Merch KPI Scorecard
Account:					T2DL_NAP_AIS_BATCH
Variable:         
	{environment_schema} 	t2dl_das_in_season_management_reporting 
DAG:						merch_main_kpi_scorecard
Author:						Asiyah Fox
Date Created:				2023/12/22
Date Last Updated:			2023/12/22
*************************************************************************************************************/

--Date Lookup
--DROP TABLE date_lookup;

CREATE OR REPLACE TEMPORARY TABLE date_lookup

AS
SELECT DISTINCT a.week_idnt AS week_num,
 a.week_num_of_fiscal_month,
     TRIM(FORMAT('%11d', a.fiscal_year_num)) || ', ' || TRIM(FORMAT('%11d', a.fiscal_month_num)) || ', Wk ' || TRIM(FORMAT('%11d'
    , a.week_num_of_fiscal_month)) AS week_label,
 a.week_start_day_date,
 a.week_end_day_date,
 a.month_idnt AS month_num,
     TRIM(FORMAT('%11d', a.fiscal_year_num)) || ' ' || TRIM(FORMAT('%11d', a.fiscal_month_num)) || ' ' || TRIM(a.month_abrv
   ) AS month_label,
 a.month_desc,
 a.month_start_day_date,
 a.month_end_day_date,
 a.quarter_idnt AS quarter_num,
 a.quarter_label,
 a.quarter_start_day_date,
 a.quarter_end_day_date,
 a.fiscal_year_num,
  CASE
  WHEN a.month_idnt = b.max_month_num
  THEN 1
  ELSE 0
  END AS mtd_flag,
  CASE
  WHEN a.quarter_idnt = b.max_quarter_num
  THEN 1
  ELSE 0
  END AS qtd_flag,
  CASE
  WHEN a.fiscal_year_num = b.max_fiscal_year_num
  THEN 1
  ELSE 0
  END AS ytd_flag
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim AS a
 LEFT JOIN (SELECT MAX(month_idnt) AS max_month_num,
   MAX(quarter_idnt) AS max_quarter_num,
   MAX(fiscal_year_num) AS max_fiscal_year_num
  FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.day_cal_454_dim
  WHERE week_end_day_date < CURRENT_DATE('PST8PDT')) AS b ON TRUE
WHERE a.week_end_day_date < CURRENT_DATE('PST8PDT')
QUALIFY (DENSE_RANK() OVER (ORDER BY month_num DESC)) <= 13;


--AOR Lookup

CREATE OR REPLACE TEMPORARY TABLE aor

AS
SELECT DISTINCT REGEXP_REPLACE(channel_brand, '_', ' ') AS banner,
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


--All metrics sourced from from MFP

CREATE OR REPLACE TEMPORARY TABLE mfp

AS
SELECT fct.week_num,
 fct.dept_num,
  CASE
  WHEN fct.banner_country_num = 1
  THEN 'NORDSTROM'
  WHEN fct.banner_country_num = 3
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
 fct.fulfill_type_num,
 0 AS ty_net_sales_retail_amt,
 0 AS ly_net_sales_retail_amt,
 0 AS op_net_sales_retail_amt,
 0 AS sp_net_sales_retail_amt,
 0 AS cp_net_sales_retail_amt,
 0 AS ty_net_sales_cost_amt,
 0 AS ly_net_sales_cost_amt,
 0 AS op_net_sales_cost_amt,
 0 AS sp_net_sales_cost_amt,
 0 AS cp_net_sales_cost_amt,
 0 AS ty_net_sales_units,
 0 AS ly_net_sales_units,
 0 AS op_net_sales_units,
 0 AS sp_net_sales_units,
 0 AS cp_net_sales_units,
 SUM(fct.ty_merch_margin_retail_amt) AS ty_merch_margin_amt,
 SUM(fct.ly_merch_margin_retail_amt) AS ly_merch_margin_amt,
 SUM(fct.op_merch_margin_retail_amt) AS op_merch_margin_amt,
 SUM(fct.sp_merch_margin_retail_amt) AS sp_merch_margin_amt,
 SUM(fct.cp_merch_margin_retail_amt) AS cp_merch_margin_amt,
 SUM(fct.ty_beginning_of_period_active_cost_amt + fct.ty_beginning_of_period_inactive_cost_amt) AS ty_bop_tot_cost_amt,
 SUM(fct.ly_beginning_of_period_active_cost_amt + fct.ly_beginning_of_period_inactive_cost_amt) AS ly_bop_tot_cost_amt,
 SUM(fct.op_beginning_of_period_active_cost_amt + fct.op_beginning_of_period_inactive_cost_amt) AS op_bop_tot_cost_amt,
 SUM(fct.sp_beginning_of_period_active_cost_amt + fct.sp_beginning_of_period_inactive_cost_amt) AS sp_bop_tot_cost_amt,
 SUM(fct.cp_beginning_of_period_active_cost_amt + fct.cp_beginning_of_period_inactive_cost_amt) AS cp_bop_tot_cost_amt,
 SUM(fct.ty_beginning_of_period_active_qty + fct.ty_beginning_of_period_inactive_qty) AS ty_bop_tot_units,
 SUM(fct.ly_beginning_of_period_active_qty + fct.ly_beginning_of_period_inactive_qty) AS ly_bop_tot_units,
 SUM(fct.op_beginning_of_period_active_qty + fct.op_beginning_of_period_inactive_qty) AS op_bop_tot_units,
 SUM(fct.sp_beginning_of_period_active_qty + fct.sp_beginning_of_period_inactive_qty) AS sp_bop_tot_units,
 SUM(fct.cp_beginning_of_period_active_qty + fct.cp_beginning_of_period_inactive_qty) AS cp_bop_tot_units,
 SUM(fct.ty_ending_of_period_active_cost_amt + fct.ty_ending_of_period_inactive_cost_amt) AS ty_eop_tot_cost_amt,
 SUM(fct.ly_ending_of_period_active_cost_amt + fct.ly_ending_of_period_inactive_cost_amt) AS ly_eop_tot_cost_amt,
 SUM(fct.op_ending_of_period_active_cost_amt + fct.op_ending_of_period_inactive_cost_amt) AS op_eop_tot_cost_amt,
 SUM(fct.sp_ending_of_period_active_cost_amt + fct.sp_ending_of_period_inactive_cost_amt) AS sp_eop_tot_cost_amt,
 SUM(fct.cp_ending_of_period_active_cost_amt + fct.cp_ending_of_period_inactive_cost_amt) AS cp_eop_tot_cost_amt,
 SUM(fct.ty_ending_of_period_active_qty + fct.ty_ending_of_period_inactive_qty) AS ty_eop_tot_units,
 SUM(fct.ly_ending_of_period_active_qty + fct.ly_ending_of_period_inactive_qty) AS ly_eop_tot_units,
 SUM(fct.op_ending_of_period_active_qty + fct.op_ending_of_period_inactive_qty) AS op_eop_tot_units,
 SUM(fct.sp_ending_of_period_active_qty + fct.sp_ending_of_period_inactive_qty) AS sp_eop_tot_units,
 SUM(fct.cp_ending_of_period_active_qty + fct.cp_ending_of_period_inactive_qty) AS cp_eop_tot_units,
 SUM(fct.ty_receipts_active_cost_amt + fct.ty_receipts_inactive_cost_amt) AS ty_rcpts_tot_cost_amt,
 SUM(fct.ly_receipts_active_cost_amt + fct.ly_receipts_inactive_cost_amt) AS ly_rcpts_tot_cost_amt,
 SUM(fct.op_receipts_active_cost_amt + fct.op_receipts_inactive_cost_amt) AS op_rcpts_tot_cost_amt,
 SUM(fct.sp_receipts_active_cost_amt + fct.sp_receipts_inactive_cost_amt) AS sp_rcpts_tot_cost_amt,
 SUM(fct.cp_receipts_active_cost_amt + fct.cp_receipts_inactive_cost_amt) AS cp_rcpts_tot_cost_amt,
 SUM(fct.ty_receipts_active_qty + fct.ty_receipts_inactive_qty) AS ty_rcpts_tot_units,
 SUM(fct.ly_receipts_active_qty + fct.ly_receipts_inactive_qty) AS ly_rcpts_tot_units,
 SUM(fct.op_receipts_active_qty + fct.op_receipts_inactive_qty) AS op_rcpts_tot_units,
 SUM(fct.sp_receipts_active_qty + fct.sp_receipts_inactive_qty) AS sp_rcpts_tot_units,
 SUM(fct.cp_receipts_active_qty + fct.cp_receipts_inactive_qty) AS cp_rcpts_tot_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_banner_country_fact AS fct
 INNER JOIN date_lookup AS dt ON fct.week_num = dt.week_num
WHERE fct.banner_country_num IN (1, 3)
GROUP BY fct.week_num,
 fct.dept_num,
 banner,
 fct.fulfill_type_num
UNION ALL
SELECT fct0.week_num,
 fct0.dept_num,
  CASE
  WHEN ch.banner_country_num = 1
  THEN 'NORDSTROM'
  WHEN ch.banner_country_num = 3
  THEN 'NORDSTROM RACK'
  ELSE NULL
  END AS banner,
 fct0.fulfill_type_num,
 SUM(fct0.ty_net_sales_retail_amt) AS ty_net_sales_retail_amt,
 SUM(fct0.ly_net_sales_retail_amt) AS ly_net_sales_retail_amt,
 SUM(fct0.op_net_sales_retail_amt) AS op_net_sales_retail_amt,
 SUM(fct0.sp_net_sales_retail_amt) AS sp_net_sales_retail_amt,
 SUM(fct0.cp_net_sales_retail_amt) AS cp_net_sales_retail_amt,
 SUM(fct0.ty_net_sales_cost_amt) AS ty_net_sales_cost_amt,
 SUM(fct0.ly_net_sales_cost_amt) AS ly_net_sales_cost_amt,
 SUM(fct0.op_net_sales_cost_amt) AS op_net_sales_cost_amt,
 SUM(fct0.sp_net_sales_cost_amt) AS sp_net_sales_cost_amt,
 SUM(fct0.cp_net_sales_cost_amt) AS cp_net_sales_cost_amt,
 SUM(fct0.ty_net_sales_qty) AS ty_net_sales_units,
 SUM(fct0.ly_net_sales_qty) AS ly_net_sales_units,
 SUM(fct0.op_net_sales_qty) AS op_net_sales_units,
 SUM(fct0.sp_net_sales_qty) AS sp_net_sales_units,
 SUM(fct0.cp_net_sales_qty) AS cp_net_sales_units,
 0 AS ty_merch_margin_amt,
 0 AS ly_merch_margin_amt,
 0 AS op_merch_margin_amt,
 0 AS sp_merch_margin_amt,
 0 AS cp_merch_margin_amt,
 0 AS ty_bop_tot_cost_amt,
 0 AS ly_bop_tot_cost_amt,
 0 AS op_bop_tot_cost_amt,
 0 AS sp_bop_tot_cost_amt,
 0 AS cp_bop_tot_cost_amt,
 0 AS ty_bop_tot_units,
 0 AS ly_bop_tot_units,
 0 AS op_bop_tot_units,
 0 AS sp_bop_tot_units,
 0 AS cp_bop_tot_units,
 0 AS ty_eop_tot_cost_amt,
 0 AS ly_eop_tot_cost_amt,
 0 AS op_eop_tot_cost_amt,
 0 AS sp_eop_tot_cost_amt,
 0 AS cp_eop_tot_cost_amt,
 0 AS ty_eop_tot_units,
 0 AS ly_eop_tot_units,
 0 AS op_eop_tot_units,
 0 AS sp_eop_tot_units,
 0 AS cp_eop_tot_units,
 0 AS ty_rcpts_tot_cost_amt,
 0 AS ly_rcpts_tot_cost_amt,
 0 AS op_rcpts_tot_cost_amt,
 0 AS sp_rcpts_tot_cost_amt,
 0 AS cp_rcpts_tot_cost_amt,
 0 AS ty_rcpts_tot_units,
 0 AS ly_rcpts_tot_units,
 0 AS op_rcpts_tot_units,
 0 AS sp_rcpts_tot_units,
 0 AS cp_rcpts_tot_units
FROM `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.mfp_cost_plan_actual_channel_fact AS fct0
 INNER JOIN date_lookup AS dt0 ON fct0.week_num = dt0.week_num
 INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_vws.org_channel_dim AS ch ON fct0.channel_num = ch.channel_num AND ch.banner_country_num IN (1, 3)
GROUP BY fct0.week_num,
 fct0.dept_num,
 banner,
 fct0.fulfill_type_num;



--Join dimensions and insert into table
TRUNCATE TABLE `{{params.gcp_project_id}}`.{{params.environment_schema}}.kpi_scorecard{{params.env_suffix}} ; 

INSERT INTO `{{params.gcp_project_id}}`.{{params.environment_schema}}.kpi_scorecard{{params.env_suffix}} 
SELECT
	 dt.week_num
	,dt.week_num_of_fiscal_month
	,dt.week_label
	,dt.week_start_day_date
	,dt.week_end_day_date
	,dt.month_num
	,dt.month_label
	,dt.month_desc
	,dt.month_start_day_date
	,dt.month_end_day_date
	,dt.quarter_num
	,dt.quarter_label
	,dt.quarter_start_day_date
	,dt.quarter_end_day_date
	,dt.fiscal_year_num
	,dt.mtd_flag
	,dt.qtd_flag
	,dt.ytd_flag
	,fct.banner
	,fct.fulfill_type_num
	,dep.dept_num
	,TRIM(dep.dept_num || ', ' || dep.dept_name) AS dept_label
	,dep.division_num
	,TRIM(dep.division_num || ', ' || dep.division_name) AS div_label
	,dep.subdivision_num
	,TRIM(dep.subdivision_num || ', ' || dep.subdivision_name) AS sdiv_label --WBR uses this, not short name
	,dep.active_store_ind
	,dep.merch_dept_ind
	,aor.general_merch_manager_executive_vice_president
	,aor.div_merch_manager_senior_vice_president
	,aor.div_merch_manager_vice_president
	,aor.merch_director
	,aor.buyer
	,aor.merch_planning_executive_vice_president
	,aor.merch_planning_senior_vice_president
	,aor.merch_planning_vice_president
	,aor.merch_planning_director_manager
	,aor.assortment_planner 	
	--Net Sales
	,SUM(ty_net_sales_retail_amt         ) AS ty_net_sales_retail_amt         
	,SUM(ly_net_sales_retail_amt         ) AS ly_net_sales_retail_amt         
	,SUM(op_net_sales_retail_amt         ) AS op_net_sales_retail_amt         
	,SUM(sp_net_sales_retail_amt         ) AS sp_net_sales_retail_amt         
	,SUM(cp_net_sales_retail_amt         ) AS cp_net_sales_retail_amt         
	,SUM(ty_net_sales_cost_amt           ) AS ty_net_sales_cost_amt           
	,SUM(ly_net_sales_cost_amt           ) AS ly_net_sales_cost_amt           
	,SUM(op_net_sales_cost_amt           ) AS op_net_sales_cost_amt           
	,SUM(sp_net_sales_cost_amt           ) AS sp_net_sales_cost_amt           
	,SUM(cp_net_sales_cost_amt           ) AS cp_net_sales_cost_amt           
	,SUM(ty_net_sales_units              ) AS ty_net_sales_units              
	,SUM(ly_net_sales_units              ) AS ly_net_sales_units              
	,CAST(TRUNC(SUM(op_net_sales_units))AS INT64) AS op_net_sales_units              
	,CAST(TRUNC(SUM(sp_net_sales_units))AS INT64) AS sp_net_sales_units              
	,CAST(TRUNC(SUM(cp_net_sales_units))AS INT64) AS cp_net_sales_units              
	--Merch Margin               
	,SUM(ty_merch_margin_amt             ) AS ty_merch_margin_amt      
	,SUM(ly_merch_margin_amt             ) AS ly_merch_margin_amt      
	,SUM(op_merch_margin_amt             ) AS op_merch_margin_amt      
	,SUM(sp_merch_margin_amt             ) AS sp_merch_margin_amt      
	,SUM(cp_merch_margin_amt             ) AS cp_merch_margin_amt      
	--BOP                                
	,SUM(ty_bop_tot_cost_amt             ) AS ty_bop_tot_cost_amt             
	,SUM(ly_bop_tot_cost_amt             ) AS ly_bop_tot_cost_amt             
	,SUM(op_bop_tot_cost_amt             ) AS op_bop_tot_cost_amt             
	,SUM(sp_bop_tot_cost_amt             ) AS sp_bop_tot_cost_amt             
	,SUM(cp_bop_tot_cost_amt             ) AS cp_bop_tot_cost_amt             
	,SUM(ty_bop_tot_units                ) AS ty_bop_tot_units                
	,SUM(ly_bop_tot_units                ) AS ly_bop_tot_units                
	,CAST(TRUNC(SUM(op_bop_tot_units))AS INT64) AS op_bop_tot_units                
	,CAST(TRUNC(SUM(sp_bop_tot_units))AS INT64) AS sp_bop_tot_units                
	,CAST(TRUNC(SUM(cp_bop_tot_units))AS INT64) AS cp_bop_tot_units                
	--EOP                                
	,SUM(ty_eop_tot_cost_amt             ) AS ty_eop_tot_cost_amt             
	,SUM(ly_eop_tot_cost_amt             ) AS ly_eop_tot_cost_amt             
	,SUM(op_eop_tot_cost_amt             ) AS op_eop_tot_cost_amt             
	,SUM(sp_eop_tot_cost_amt             ) AS sp_eop_tot_cost_amt             
	,SUM(cp_eop_tot_cost_amt             ) AS cp_eop_tot_cost_amt             
	,SUM(ty_eop_tot_units                ) AS ty_eop_tot_units                
	,SUM(ly_eop_tot_units                ) AS ly_eop_tot_units                
	,CAST(TRUNC(SUM(op_eop_tot_units))AS INT64) AS op_eop_tot_units                
	,CAST(TRUNC(SUM(sp_eop_tot_units))AS INT64) AS sp_eop_tot_units                
	,CAST(TRUNC(SUM(cp_eop_tot_units))AS INT64) AS cp_eop_tot_units           
	--Receipts                                            
	,SUM(ty_rcpts_tot_cost_amt           ) AS ty_rcpts_tot_cost_amt      
	,SUM(ly_rcpts_tot_cost_amt           ) AS ly_rcpts_tot_cost_amt      
	,SUM(op_rcpts_tot_cost_amt           ) AS op_rcpts_tot_cost_amt      
	,SUM(sp_rcpts_tot_cost_amt           ) AS sp_rcpts_tot_cost_amt      
	,SUM(cp_rcpts_tot_cost_amt           ) AS cp_rcpts_tot_cost_amt      
	,SUM(ty_rcpts_tot_units              ) AS ty_rcpts_tot_units         
	,SUM(ly_rcpts_tot_units              ) AS ly_rcpts_tot_units         
	,CAST(TRUNC(SUM(op_rcpts_tot_units))AS INT64) AS op_rcpts_tot_units         
	,CAST(TRUNC(SUM(sp_rcpts_tot_units))AS INT64) AS sp_rcpts_tot_units         
	,CAST(TRUNC(SUM(cp_rcpts_tot_units))AS INT64) AS cp_rcpts_tot_units         
	,TIMESTAMP(MAX(CURRENT_DATETIME('PST8PDT'))) AS updated_timestamp
  , 'PST8PDT' AS updated_timestamp_tz
FROM mfp fct
INNER JOIN date_lookup dt
	ON fct.week_num = dt.week_num
INNER JOIN `{{params.gcp_project_id}}`.{{params.dbenv}}_nap_usr_vws.department_dim dep
	ON fct.dept_num = dep.dept_num
--	AND dep.merch_dept_ind = 'Y'
--	AND dep.dept_num NOT IN (584, 585) --remove beauty sample depts
LEFT JOIN aor
	ON fct.dept_num = aor.dept_num
	AND fct.banner = aor.banner
GROUP BY 1,2,3,4,5,6,7,8,9,10
	,11,12,13,14,15,16,17,18,19,20
	,21,22,23,24,25,26,27,28,29,30
	,31,32,33,34,35,36,37,38
;
