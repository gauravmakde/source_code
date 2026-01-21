/*
Name:Monday Morning Reporting script
Project:Monday Morning Reporting Dash
Purpose:  Combine MFP, WBR and APT data source into a T2 Table that can direclty feed into Monday Morning Reporting for performance improvment
Variable(s):    {{environment_schema}} T2DL_NAP_AIS_BATCH
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
DAG:
Author(s):Xiao Tong, Manuela Hurtado, Tanner Moxcey
Date Created:3/31/23
Date Last Updated:1/19/24
*/

------------------------------------------------------------- START TEMPORARY TABLES -----------------------------------------------------------------------


CREATE MULTISET VOLATILE TABLE date_lookup as
(
	SELECT
		DISTINCT week_idnt
		, month_idnt
	FROM prd_nap_usr_vws.day_cal_454_dim
)WITH DATA
PRIMARY INDEX ("week_idnt")
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX ("week_idnt")
	,COLUMN ("week_idnt")
	ON date_lookup;

CREATE MULTISET VOLATILE TABLE aor AS (
SELECT DISTINCT
	REGEXP_REPLACE(channel_brand,'_',' ') AS banner
	,dept_num
	,general_merch_manager_executive_vice_president
	,div_merch_manager_senior_vice_president
	,div_merch_manager_vice_president
	,merch_director
	,buyer
	,merch_planning_executive_vice_president
	,merch_planning_senior_vice_president
	,merch_planning_vice_president
	,merch_planning_director_manager
	,assortment_planner       	
FROM prd_nap_usr_vws.area_of_responsibility_dim
QUALIFY ROW_NUMBER() OVER (PARTITION BY banner, dept_num ORDER BY 3,4,5,6,7,8,9,10,11,12) = 1
) WITH DATA
	PRIMARY INDEX (banner,dept_num) 
	ON COMMIT PRESERVE ROWS
;

COLLECT STATS
	PRIMARY INDEX (banner,dept_num) 
		ON aor
;


CREATE MULTISET VOLATILE TABLE MFP AS
(
	SELECT
	'MFP' AS data_source
	, CASE
			WHEN left(cast(week_idnt as varchar(6)),4) = (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < current_date)
				THEN CAST('TY' AS VARCHAR(3))
			WHEN left(cast(week_idnt as varchar(6)),4) = (SELECT MAX(fiscal_year_num) -1 FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < current_date)
				THEN CAST('LY' AS VARCHAR(3))
			ELSE 'NA' END AS ty_ly_ind
	, m.WEEK_NUM	 AS week_idnt
	, d.month_idnt AS month_idnt
	, m.BANNER	 AS banner
	, m.COUNTRY	 AS country
	, m.CHANNEL AS channel
	, m.DIVISION AS division
	, m.SUBDIVISION	 AS subdivision
	, m.DEPARTMENT_LABEL	 AS department
	, m.dept_num
	, CAST('NA' AS VARCHAR(10)) AS price_type
	, CAST('NA' AS VARCHAR(40)) AS category
	, CAST('NA' AS VARCHAR(40)) AS supplier_group
	, CAST(0 AS DECIMAL(38,2)) AS apt_net_sls_r
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_net_sls_r_dollars
	, CAST(0 AS DECIMAL(38,2)) AS apt_net_sls_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_net_sls_c_dollars
	, CAST(0 AS INTEGER) AS apt_net_sls_units
	, CAST(0 AS INTEGER) AS apt_fut_net_sls_c_units
	, CAST(0 AS DECIMAL(38,2)) AS apt_demand_r_online
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_demand_r_online
	, CAST(0 AS INTEGER) AS apt_eop_ttl_units-- apt eoh no intransit -- wbr eop, bop include intransit
	, CAST(0 AS DECIMAL(38,2)) AS apt_eop_ttl_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_eop_ttl_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_ttl_porcpt_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_rcpt_need_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_plan_bop_c
    , CAST(0 AS INTEGER) AS apt_plan_bop_u
    , CAST(0 AS INTEGER) AS apt_plan_eop_u
	, CAST(0 AS DECIMAL(38,2)) AS wbr_sales_dollars
	, CAST(0 AS INTEGER) AS wbr_sales_units
	, CAST(0 AS INTEGER) AS wbr_eoh_units
	, CAST(0 AS DECIMAL(38,2)) AS wbr_eoh_cost
	, CAST(0 AS DECIMAL(38,2)) AS wbr_sales_cost
	, CAST(0 AS DECIMAL(38,2)) AS wbr_sales_pm
	, SUM(CP_BOP_TOT_COST_AMT)	 AS 	cp_bop_total_c
	, SUM(OP_BOP_TOT_COST_AMT)	 AS 	op_bop_total_c
	, SUM(SP_BOP_TOT_COST_AMT)	 AS 	sp_bop_total_c
	, SUM(TY_BOP_TOT_COST_AMT)	 AS 	ty_bop_total_c
	, SUM(LY_BOP_TOT_COST_AMT)	 AS 	ly_bop_total_c
	, SUM(CP_BOP_TOT_UNITS)	 AS 	cp_bop_total_u
	, SUM(OP_BOP_TOT_UNITS)	 AS 	op_bop_total_u
	, SUM(SP_BOP_TOT_UNITS)	 AS 	sp_bop_total_u
	, SUM(TY_BOP_TOT_UNITS)	 AS 	ty_bop_total_u
	, SUM(LY_BOP_TOT_UNITS)	 AS 	ly_bop_total_u
	, SUM(CP_EOP_ACTIVE_COST_AMT)	 AS 	cp_eop_active_c
	, SUM(OP_EOP_ACTIVE_COST_AMT)	 AS 	op_eop_active_c
	, SUM(SP_EOP_ACTIVE_COST_AMT)	 AS 	sp_eop_active_c
	, SUM(TY_EOP_ACTIVE_COST_AMT)	 AS 	ty_eop_active_c
	, SUM(LY_EOP_ACTIVE_COST_AMT)	 AS 	ly_eop_active_c
	, SUM(CP_EOP_ACTIVE_UNITS)	 AS 	cp_eop_active_u
	, SUM(OP_EOP_ACTIVE_UNITS)	 AS 	op_eop_active_u
	, SUM(SP_EOP_ACTIVE_UNITS)	 AS 	sp_eop_active_u
	, SUM(TY_EOP_ACTIVE_UNITS)	 AS 	ty_eop_active_u
	, SUM(LY_EOP_ACTIVE_UNITS)	 AS 	ly_eop_active_u
	, SUM(CP_EOP_TOT_COST_AMT)	 AS 	cp_eop_total_c
	, SUM(OP_EOP_TOT_COST_AMT)	 AS 	op_eop_total_c
	, SUM(SP_EOP_TOT_COST_AMT)	 AS 	sp_eop_total_c
	, SUM(TY_EOP_TOT_COST_AMT)	 AS 	ty_eop_total_c
	, SUM(LY_EOP_TOT_COST_AMT)	 AS 	ly_eop_total_c
	, SUM(CP_EOP_TOT_UNITS)	 AS 	cp_eop_total_u
	, SUM(OP_EOP_TOT_UNITS)	 AS 	op_eop_total_u
	, SUM(SP_EOP_TOT_UNITS)	 AS 	sp_eop_total_u
	, SUM(TY_EOP_TOT_UNITS)	 AS 	ty_eop_total_u
	, SUM(LY_EOP_TOT_UNITS)	 AS 	ly_eop_total_u
	, SUM(CP_GROSS_MARGIN_RETAIL_AMT)	 AS 	cp_gross_margin
	, SUM(OP_GROSS_MARGIN_RETAIL_AMT)	 AS 	op_gross_margin
	, SUM(SP_GROSS_MARGIN_RETAIL_AMT)	 AS 	sp_gross_margin
	, SUM(TY_GROSS_MARGIN_RETAIL_AMT)	 AS 	ty_gross_margin
	, SUM(LY_GROSS_MARGIN_RETAIL_AMT)	 AS 	ly_gross_margin
	, SUM(CP_MERCH_MARGIN_AMT)	 AS 	cp_merch_margin
	, SUM(OP_MERCH_MARGIN_AMT)	 AS 	op_merch_margin
	, SUM(SP_MERCH_MARGIN_AMT)	 AS 	sp_merch_margin
	, SUM(TY_MERCH_MARGIN_AMT)	 AS 	ty_merch_margin
	, SUM(LY_MERCH_MARGIN_AMT)	 AS 	ly_merch_margin
	, SUM(CP_NET_SALES_COST_AMT)	 AS 	cp_net_sales_c
	, SUM(OP_NET_SALES_COST_AMT)	 AS 	op_net_sales_c
	, SUM(SP_NET_SALES_COST_AMT)	 AS 	sp_net_sales_c
	, SUM(TY_NET_SALES_COST_AMT)	 AS 	ty_net_sales_c
	, SUM(LY_NET_SALES_COST_AMT)	 AS 	ly_net_sales_c
	, SUM(CP_NET_SALES_RETAIL_AMT)	 AS 	cp_net_sales_r
	, SUM(OP_NET_SALES_RETAIL_AMT)	 AS 	op_net_sales_r
	, SUM(SP_NET_SALES_RETAIL_AMT)	 AS 	sp_net_sales_r
	, SUM(TY_NET_SALES_RETAIL_AMT)	 AS 	ty_net_sales_r
	, SUM(LY_NET_SALES_RETAIL_AMT)	 AS 	ly_net_sales_r
	, SUM(CP_NET_SALES_UNITS)	 AS 	cp_net_sales_u
	, SUM(OP_NET_SALES_UNITS)	 AS 	op_net_sales_u
	, SUM(SP_NET_SALES_UNITS)	 AS 	sp_net_sales_u
	, SUM(TY_NET_SALES_UNITS)	 AS 	ty_net_sales_u
	, SUM(LY_NET_SALES_UNITS)	 AS 	ly_net_sales_u
	, SUM(CP_RCPTS_TOT_COST_AMT)	 AS 	cp_rcpts_total_c
	, SUM(OP_RCPTS_TOT_COST_AMT)	 AS 	op_rcpts_total_c
	, SUM(SP_RCPTS_TOT_COST_AMT)	 AS 	sp_rcpts_total_c
	, SUM(TY_RCPTS_TOT_COST_AMT)	 AS 	ty_rcpts_total_c
	, SUM(LY_RCPTS_TOT_COST_AMT)	 AS 	ly_rcpts_total_c
	, SUM(CP_RCPTS_TOT_UNITS)	 AS 	cp_rcpts_total_u
	, SUM(OP_RCPTS_TOT_UNITS)	 AS 	op_rcpts_total_u
	, SUM(SP_RCPTS_TOT_UNITS)	 AS 	sp_rcpts_total_u
	, SUM(TY_RCPTS_TOT_UNITS)	 AS 	ty_rcpts_total_u
	, SUM(LY_RCPTS_TOT_UNITS)	 AS 	ly_rcpts_total_u
    , SUM(ty_rcpts_active_cost_amt) AS ty_rcpts_active_cost_amt
    , SUM(sp_rcpts_active_cost_amt) AS sp_rcpts_active_cost_amt
    , SUM(op_rcpts_active_cost_amt) AS op_rcpts_active_cost_amt
    , SUM(ly_rcpts_active_cost_amt) AS ly_rcpts_active_cost_amt
    , SUM(ty_rcpts_active_units) AS ty_rcpts_active_u
    , SUM(sp_rcpts_active_units) AS sp_rcpts_active_u
    , SUM(op_rcpts_active_units) AS op_rcpts_active_u
    , SUM(ly_rcpts_active_units) AS ly_rcpts_active_u
    , sum(CASE WHEN channel_num in (120,250) then TY_DEMAND_TOTAL_RETAIL_AMT else 0 end) as ty_demand_total_retail_amt_online 
    , sum(CASE WHEN channel_num in (120,250) then SP_DEMAND_TOTAL_RETAIL_AMT else 0 end) as sp_demand_total_retail_amt_online
    , sum(CASE WHEN channel_num in (120,250) then OP_DEMAND_TOTAL_RETAIL_AMT else 0 end) as op_demand_total_retail_amt_online
    , sum(CASE WHEN channel_num in (120,250) then LY_DEMAND_TOTAL_RETAIL_AMT else 0 end) as ly_demand_total_retail_amt_online
    , sum(CASE WHEN channel_num in (120,250) then TY_DEMAND_TOTAL_UNITS else 0 end) as ty_demand_total_units_online
    , sum(CASE WHEN channel_num in (120,250) then SP_DEMAND_TOTAL_UNITS else 0 end) as sp_demand_total_units_online
    , sum(CASE WHEN channel_num in (120,250) then OP_DEMAND_TOTAL_UNITS else 0 end) as op_demand_total_units_online
    , sum(CASE WHEN channel_num in (120,250) then LY_DEMAND_TOTAL_UNITS else 0 end) as ly_demand_total_units_online
    , sum(TY_RETURNS_RETAIL_AMT) as ty_returns_retail_amt
    , sum(SP_RETURNS_RETAIL_AMT) as sp_returns_retail_amt
    , sum(OP_RETURNS_RETAIL_AMT) as op_returns_retail_amt
    , sum(LY_RETURNS_RETAIL_AMT) as ly_returns_retail_amt
    , sum(TY_RETURNS_UNITS) as ty_returns_units
    , sum(SP_RETURNS_UNITS) as sp_returns_units
    , sum(OP_RETURNS_UNITS) as op_returns_units
    , sum(LY_RETURNS_UNITS) as ly_returns_units
FROM T2DL_DAS_ACE_MFP.MFP_BANNER_COUNTRY_CHANNEL_STG m
		LEFT JOIN date_lookup d
		ON d.week_idnt = m. WEEK_NUM
	WHERE (m."year" = (
					SELECT MAX(fiscal_year_num)
						FROM prd_nap_usr_vws.day_cal_454_dim
					WHERE week_end_day_date < current_date)
	AND m.WEEK_NUM <= (
					SELECT MAX(week_idnt)
						FROM prd_nap_usr_vws.day_cal_454_dim
					WHERE week_end_day_date < current_date))
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
WITH DATA
PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group)
		ON MFP;

CREATE MULTISET VOLATILE TABLE WBR AS
(
SELECT
	'WBR' AS data_source
	, ty_ly_ind
    , week_idnt AS week_idnt
    , month_idnt AS month_idnt
   	, CASE WHEN banner = 'N' THEN 'NORDSTROM' WHEN  banner = 'NR' THEN 'NORDSTROM RACK' ELSE banner END AS banner
    , CAST('US' AS VARCHAR(40)) AS country
	, chnl_label AS channel
    , division AS division
    , subdivision AS subdivision
    , department AS department
    , dept_idnt AS dept_num
    , CASE WHEN price_type = 'P' THEN 'PRO' WHEN price_type = 'R' THEN 'REG' WHEN price_type = 'C' THEN 'CLR' ELSE 'UNKNOWN' END AS price_type
    , CAST('NA' AS VARCHAR(40)) AS category
	, CAST('NA' AS VARCHAR(40)) AS supplier_group
	, CAST(0 AS DECIMAL(38,2)) AS apt_net_sls_r
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_net_sls_r_dollars
	, CAST(0 AS DECIMAL(38,2)) AS apt_net_sls_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_net_sls_c_dollars
	, CAST(0 AS INTEGER) AS apt_net_sls_units
	, CAST(0 AS INTEGER) AS apt_fut_net_sls_c_units
	, CAST(0 AS DECIMAL(38,2)) AS apt_demand_r_online
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_demand_r_online
	, CAST(0 AS INTEGER) AS apt_eop_ttl_units
	, CAST(0 AS DECIMAL(38,2)) AS apt_eop_ttl_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_eop_ttl_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_ttl_porcpt_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_fut_rcpt_need_c
	, CAST(0 AS DECIMAL(38,2)) AS apt_plan_bop_c
    , CAST(0 AS INTEGER) AS apt_plan_bop_u
    , CAST(0 AS INTEGER) AS apt_plan_eop_u
    , SUM(sales_dollars) AS wbr_sales_dollars
    , SUM(sales_units) AS wbr_sales_units
    , SUM(eoh_units) AS wbr_eoh_units
    , SUM(eoh_cost) AS wbr_eoh_cost
    , SUM(cost_of_goods_sold) AS wbr_sales_cost
    , SUM(sales_pm) AS wbr_sales_pm
	, CAST(0 AS DECIMAL(38,2)) AS cp_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS op_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_bop_total_c
    , CAST(0 AS INTEGER) AS cp_bop_total_u
	, CAST(0 AS INTEGER) AS op_bop_total_u
	, CAST(0 AS INTEGER) AS sp_bop_total_u
	, CAST(0 AS INTEGER) AS ty_bop_total_u
	, CAST(0 AS INTEGER) AS ly_bop_total_u
 	, CAST(0 AS DECIMAL(38,2)) AS cp_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS op_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_eop_active_c
	, CAST(0 AS INTEGER) AS cp_eop_active_u
	, CAST(0 AS INTEGER) AS op_eop_active_u
	, CAST(0 AS INTEGER) AS sp_eop_active_u
	, CAST(0 AS INTEGER) AS ty_eop_active_u
	, CAST(0 AS INTEGER) AS ly_eop_active_u
	, CAST(0 AS DECIMAL(38,2)) AS cp_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS op_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_eop_total_c
	, CAST(0 AS INTEGER) AS cp_eop_total_u
	, CAST(0 AS INTEGER) AS op_eop_total_u
	, CAST(0 AS INTEGER) AS sp_eop_total_u
	, CAST(0 AS INTEGER) AS ty_eop_total_u
	, CAST(0 AS INTEGER) AS ly_eop_total_u
	, CAST(0 AS DECIMAL(38,2)) AS cp_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS op_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS sp_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS ty_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS ly_gross_margin
 	, CAST(0 AS DECIMAL(38,2)) AS cp_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS op_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS sp_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS ty_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS ly_merch_margin
 	, CAST(0 AS DECIMAL(38,2)) AS cp_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS op_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS cp_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS op_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS sp_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS ty_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS ly_net_sales_r
	, CAST(0 AS INTEGER) AS cp_net_sales_u
	, CAST(0 AS INTEGER) AS op_net_sales_u
	, CAST(0 AS INTEGER) AS sp_net_sales_u
	, CAST(0 AS INTEGER) AS ty_net_sales_u
	, CAST(0 AS INTEGER) AS ly_net_sales_u
 	, CAST(0 AS DECIMAL(38,2)) AS cp_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS op_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_rcpts_total_c
	, CAST(0 AS INTEGER) AS cp_rcpts_total_u
	, CAST(0 AS INTEGER) AS op_rcpts_total_u
	, CAST(0 AS INTEGER) AS sp_rcpts_total_u
	, CAST(0 AS INTEGER) AS ty_rcpts_total_u
	, CAST(0 AS INTEGER) AS ly_rcpts_total_u
    , CAST(0 as DECIMAL(38,2)) AS ty_rcpts_active_cost_amt
    , CAST(0 as DECIMAL(38,2)) AS sp_rcpts_active_cost_amt
    , CAST(0 as DECIMAL(38,2)) AS op_rcpts_active_cost_amt
    , CAST(0 as DECIMAL(38,2)) AS ly_rcpts_active_cost_amt
    , CAST(0 as INTEGER) AS ty_rcpts_active_u
    , CAST(0 as INTEGER) AS sp_rcpts_active_u
    , CAST(0 as INTEGER) AS op_rcpts_active_u
    , CAST(0 as INTEGER) AS ly_rcpts_active_u
    , CAST(0 as DECIMAL(38,2)) as ty_demand_total_retail_amt_online
    , CAST(0 as DECIMAL(38,2)) as sp_demand_total_retail_amt_online
    , CAST(0 as DECIMAL(38,2)) as op_demand_total_retail_amt_online
    , CAST(0 as DECIMAL(38,2)) as ly_demand_total_retail_amt_online
    , CAST(0 as INTEGER) as ty_demand_total_units_online
    , CAST(0 as INTEGER) as sp_demand_total_units_online
    , CAST(0 as INTEGER) as op_demand_total_units_online
    , CAST(0 as INTEGER) as ly_demand_total_units_online
    , CAST(0 as DECIMAL(38,2)) as ty_returns_retail_amt
    , CAST(0 as DECIMAL(38,2)) as sp_returns_retail_amt
    , CAST(0 as DECIMAL(38,2)) as op_returns_retail_amt
    , CAST(0 as DECIMAL(38,2)) as ly_returns_retail_amt
    , CAST(0 as INTEGER) as ty_returns_units
    , CAST(0 as INTEGER) as sp_returns_units
    , CAST(0 as INTEGER) as op_returns_units
    , CAST(0 as INTEGER) as ly_returns_units
FROM t2dl_das_in_season_management_reporting.wbr_supplier
	WHERE ss_ind = 'SS'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
WITH DATA
PRIMARY INDEX (  department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group)
ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group
	)
ON WBR;

CREATE MULTISET VOLATILE TABLE APT as
(
SELECT
	'APT' AS data_source
	, CASE WHEN date_ind = 'TY' THEN 'TY' 
		WHEN date_ind = 'LY' THEN 'LY'
		WHEN date_ind = 'PL MTH' THEN 'TY'
		ELSE 'NA'
		END AS ty_ly_ind
	, CAST(0 AS INTEGER) AS week_idnt
	, month_idnt AS month_idnt
	, CASE WHEN BANNER = 'NORDSTROM_RACK' THEN 'NORDSTROM RACK' ELSE BANNER END AS 	banner
	, channel_country AS country
	, channel_label AS channel
	, DIVISION_desc AS division
	, SUBDIVISION_desc	AS subdivision
	, department_desc AS department
	, department_num AS dept_num
 	, CAST('NA' AS VARCHAR(10)) AS price_type
	, category AS category
	, supplier_group AS supplier_group
    , SUM(net_sls_r) AS apt_net_sls_r
    , SUM(plan_sales_r) AS apt_fut_net_sls_r_dollars
    , SUM(net_sls_c) AS apt_net_sls_c
    , SUM(plan_sales_c) AS apt_fut_net_sls_c_dollars
    , SUM(net_sls_units) AS apt_net_sls_units
    , SUM(plan_sales_u) AS apt_fut_net_sls_c_units
    , SUM(CASE WHEN channel_num IN (120,250) THEN demand_ttl_r ELSE 0 END) AS apt_demand_r_online
    , SUM(CASE WHEN channel_num IN (120,250) THEN plan_demand_r ELSE 0 END) AS apt_fut_demand_r_online
    , SUM(eop_ttl_units) AS apt_eop_ttl_units
    , SUM(eop_ttl_c) AS apt_eop_ttl_c
    , SUM(plan_eop_c) AS apt_fut_eop_ttl_c
    , SUM(ttl_porcpt_c) AS apt_ttl_porcpt_c
    , SUM(plan_receipts_c) AS apt_fut_rcpt_need_c
	, SUM(plan_bop_c) AS apt_plan_bop_c
	, SUM(plan_bop_u) AS apt_plan_bop_u
	, SUM(plan_eop_u) AS apt_plan_eop_u
	, CAST(0 AS DECIMAL(38,2)) AS wbr_sales_dollars
	, CAST(0 AS INTEGER) AS wbr_sales_units
	, CAST(0 AS INTEGER) AS wbr_eoh_units
	, CAST(0 AS DECIMAL(38,2)) AS wbr_eoh_cost
	, CAST(0 AS DECIMAL(38,2)) AS wbr_sales_cost
	, CAST(0 AS DECIMAL(38,2)) AS wbr_sales_pm
	, CAST(0 AS DECIMAL(38,2)) AS cp_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS op_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_bop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_bop_total_c
	, CAST(0 AS INTEGER) AS cp_bop_total_u
	, CAST(0 AS INTEGER) AS op_bop_total_u
	, CAST(0 AS INTEGER) AS sp_bop_total_u
	, CAST(0 AS INTEGER) AS ty_bop_total_u
	, CAST(0 AS INTEGER) AS ly_bop_total_u
	, CAST(0 AS DECIMAL(38,2)) AS cp_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS op_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_eop_active_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_eop_active_c
	, CAST(0 AS INTEGER) AS cp_eop_active_u
	, CAST(0 AS INTEGER) AS op_eop_active_u
	, CAST(0 AS INTEGER) AS sp_eop_active_u
	, CAST(0 AS INTEGER) AS ty_eop_active_u
	, CAST(0 AS INTEGER) AS ly_eop_active_u
	, CAST(0 AS DECIMAL(38,2)) AS cp_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS op_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_eop_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_eop_total_c
	, CAST(0 AS INTEGER) AS cp_eop_total_u
	, CAST(0 AS INTEGER) AS op_eop_total_u
	, CAST(0 AS INTEGER) AS sp_eop_total_u
	, CAST(0 AS INTEGER) AS ty_eop_total_u
	, CAST(0 AS INTEGER) AS ly_eop_total_u
	, CAST(0 AS DECIMAL(38,2)) AS cp_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS op_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS sp_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS ty_gross_margin
	, CAST(0 AS DECIMAL(38,2)) AS ly_gross_margin
 	, CAST(0 AS DECIMAL(38,2)) AS cp_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS op_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS sp_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS ty_merch_margin
	, CAST(0 AS DECIMAL(38,2)) AS ly_merch_margin
 	, CAST(0 AS DECIMAL(38,2)) AS cp_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS op_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_net_sales_c
	, CAST(0 AS DECIMAL(38,2)) AS cp_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS op_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS sp_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS ty_net_sales_r
	, CAST(0 AS DECIMAL(38,2)) AS ly_net_sales_r
	, CAST(0 AS INTEGER) AS cp_net_sales_u
	, CAST(0 AS INTEGER) AS op_net_sales_u
	, CAST(0 AS INTEGER) AS sp_net_sales_u
	, CAST(0 AS INTEGER) AS ty_net_sales_u
	, CAST(0 AS INTEGER) AS ly_net_sales_u
 	, CAST(0 AS DECIMAL(38,2)) AS cp_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS op_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS sp_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ty_rcpts_total_c
	, CAST(0 AS DECIMAL(38,2)) AS ly_rcpts_total_c
	, CAST(0 AS INTEGER) AS cp_rcpts_total_u
	, CAST(0 AS INTEGER) AS op_rcpts_total_u
	, CAST(0 AS INTEGER) AS sp_rcpts_total_u
	, CAST(0 AS INTEGER) AS ty_rcpts_total_u
	, CAST(0 AS INTEGER) AS ly_rcpts_total_u
	, CAST(0 as DECIMAL(38,2)) AS ty_rcpts_active_cost_amt
	, CAST(0 as DECIMAL(38,2)) AS sp_rcpts_active_cost_amt
	, CAST(0 as DECIMAL(38,2)) AS op_rcpts_active_cost_amt
	, CAST(0 as DECIMAL(38,2)) AS ly_rcpts_active_cost_amt
	, CAST(0 as INTEGER) AS ty_rcpts_active_u
	, CAST(0 as INTEGER) AS sp_rcpts_active_u
	, CAST(0 as INTEGER) AS op_rcpts_active_u
	, CAST(0 as INTEGER) AS ly_rcpts_active_u
	, CAST(0 as DECIMAL(38,2)) as ty_demand_total_retail_amt_online
    , CAST(0 as DECIMAL(38,2)) as sp_demand_total_retail_amt_online
    , CAST(0 as DECIMAL(38,2)) as op_demand_total_retail_amt_online
    , CAST(0 as DECIMAL(38,2)) as ly_demand_total_retail_amt_online
    , CAST(0 as INTEGER) as ty_demand_total_units_online
    , CAST(0 as INTEGER) as sp_demand_total_units_online
    , CAST(0 as INTEGER) as op_demand_total_units_online
    , CAST(0 as INTEGER) as ly_demand_total_units_online
    , CAST(0 as DECIMAL(38,2)) as ty_returns_retail_amt
    , CAST(0 as DECIMAL(38,2)) as sp_returns_retail_amt
    , CAST(0 as DECIMAL(38,2)) as op_returns_retail_amt
    , CAST(0 as DECIMAL(38,2)) as ly_returns_retail_amt
    , CAST(0 as INTEGER) as ty_returns_units
    , CAST(0 as INTEGER) as sp_returns_units
    , CAST(0 as INTEGER) as op_returns_units
    , CAST(0 as INTEGER) as ly_returns_units
FROM t2dl_das_apt_cost_reporting.apt_is_supplier
	WHERE month_idnt_aligned IN
								(SELECT DISTINCT month_idnt
										FROM prd_nap_usr_vws.day_cal_454_dim
									WHERE fiscal_year_num = (
										SELECT MAX(fiscal_year_num)
												FROM prd_nap_usr_vws.day_cal_454_dim
										WHERE week_end_day_date < current_date) AND month_idnt <(SELECT MIN(month_idnt) FROM prd_nap_usr_vws.day_cal_454_dim WHERE month_start_day_date > current_date))
	AND date_ind in ('TY', 'PL MTH', 'LY')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)
WITH DATA
	PRIMARY INDEX (  department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group)
	ON COMMIT PRESERVE ROWS;

COLLECT STATS
	PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group
	)
ON APT;

CREATE MULTISET VOLATILE TABLE union_table AS
(
	SELECT
		data_source
		, coalesce(temp.week_idnt,0) as week_idnt
		, coalesce(temp.month_idnt,0) as month_idnt
		, coalesce(wl1.week_454_label,'NA') AS fiscal_week
		, coalesce(wl1.fiscal_week_num,'0') as fiscal_week_num
		, coalesce(wl2.fiscal_month_num,'0') as fiscal_month_num
		, TRIM(wl2.fiscal_year_num) || ' ' || TRIM(wl2.fiscal_month_num) || ' ' || TRIM(wl2.month_abrv) AS fiscal_month
		, coalesce(wl2.fiscal_year_num,0) AS fiscal_year
		, coalesce(ty_ly_ind,'NA') as ty_ly_ind
		, coalesce(banner,'NA') as banner
		, coalesce(country,'NA') as country
		, coalesce(channel,'NA') as channel
		, coalesce(division,'NA') as division
		, coalesce(subdivision,'NA') as subdivision
		, coalesce(department,'NA') as department
		, coalesce(dept_num,0) as dept_num
		, coalesce(price_type,'NA') as price_type
		, coalesce(category,'NA') as category
		, coalesce(supplier_group,'NA') as supplier_group
	    , SUM(apt_net_sls_r) as apt_net_sls_r
	    , SUM(apt_fut_net_sls_r_dollars) as apt_fut_net_sls_r_dollars
	    , SUM(apt_net_sls_c) as apt_net_sls_c
	    , SUM(apt_fut_net_sls_c_dollars) as apt_fut_net_sls_c_dollars
	    , SUM(apt_net_sls_units) as apt_net_sls_units
	    , SUM(apt_fut_net_sls_c_units) as apt_fut_net_sls_c_units
	    , SUM(apt_demand_r_online) AS apt_demand_r_online
  	    , SUM(apt_fut_demand_r_online) AS apt_fut_demand_r_online
	    , SUM(apt_eop_ttl_units) as apt_eop_ttl_units
	    , SUM(apt_eop_ttl_c) as apt_eop_ttl_c
	    , SUM(apt_fut_eop_ttl_c) as apt_fut_eop_ttl_c
	    , SUM(apt_ttl_porcpt_c) as apt_ttl_porcpt_c
	    , SUM(apt_fut_rcpt_need_c) as apt_fut_rcpt_need_c
		, SUM(apt_plan_bop_c) AS apt_plan_bop_c
		, SUM(apt_plan_bop_u) AS apt_plan_bop_u
		, SUM(apt_plan_eop_u) AS apt_plan_eop_u
		, SUM(wbr_sales_dollars) as wbr_sales_dollars
		, SUM(wbr_sales_units) as wbr_sales_units
		, SUM(wbr_eoh_units) as wbr_eoh_units
		, SUM(wbr_eoh_cost) as wbr_eoh_cost
		, SUM(wbr_sales_cost) as wbr_sales_cost
		, SUM(wbr_sales_pm) as wbr_sales_pm
		, SUM(cp_bop_total_c) as cp_bop_total_c
		, SUM(op_bop_total_c) as op_bop_total_c
		, SUM(sp_bop_total_c) as sp_bop_total_c
		, SUM(ty_bop_total_c) as ty_bop_total_c
		, SUM(ly_bop_total_c) as ly_bop_total_c
		, SUM(cp_bop_total_u) as cp_bop_total_u
		, SUM(op_bop_total_u) as op_bop_total_u
		, SUM(sp_bop_total_u) as sp_bop_total_u
		, SUM(ty_bop_total_u) as ty_bop_total_u
		, SUM(ly_bop_total_u) as ly_bop_total_u
		, SUM(cp_eop_active_c) as cp_eop_active_c
		, SUM(op_eop_active_c) as op_eop_active_c
		, SUM(sp_eop_active_c) as sp_eop_active_c
		, SUM(ty_eop_active_c) as ty_eop_active_c
		, SUM(ly_eop_active_c) as ly_eop_active_c
		, SUM(cp_eop_active_u) as cp_eop_active_u
		, SUM(op_eop_active_u) as op_eop_active_u
		, SUM(sp_eop_active_u) as sp_eop_active_u
		, SUM(ty_eop_active_u) as ty_eop_active_u
		, SUM(ly_eop_active_u) as ly_eop_active_u
		, SUM(cp_eop_total_c) as cp_eop_total_c
		, SUM(op_eop_total_c) as op_eop_total_c
		, SUM(sp_eop_total_c) as sp_eop_total_c
		, SUM(ty_eop_total_c) as ty_eop_total_c
		, SUM(ly_eop_total_c) as ly_eop_total_c
		, SUM(cp_eop_total_u) as cp_eop_total_u
		, SUM(op_eop_total_u) as op_eop_total_u
		, SUM(sp_eop_total_u) as sp_eop_total_u
		, SUM(ty_eop_total_u) as ty_eop_total_u
		, SUM(ly_eop_total_u) as ly_eop_total_u
		, SUM(cp_gross_margin) as cp_gross_margin
		, SUM(op_gross_margin) as op_gross_margin
		, SUM(sp_gross_margin) as sp_gross_margin
		, SUM(ty_gross_margin) as ty_gross_margin
		, SUM(ly_gross_margin) as ly_gross_margin
		, SUM(cp_merch_margin) as cp_merch_margin
		, SUM(op_merch_margin) as op_merch_margin
		, SUM(sp_merch_margin) as sp_merch_margin
		, SUM(ty_merch_margin) as ty_merch_margin
		, SUM(ly_merch_margin) as ly_merch_margin
		, SUM(cp_net_sales_c) as cp_net_sales_c
		, SUM(op_net_sales_c) as op_net_sales_c
		, SUM(sp_net_sales_c) as sp_net_sales_c
		, SUM(ty_net_sales_c) as ty_net_sales_c
		, SUM(ly_net_sales_c) as ly_net_sales_c
		, SUM(cp_net_sales_r) as cp_net_sales_r
		, SUM(op_net_sales_r) as op_net_sales_r
		, SUM(sp_net_sales_r) as sp_net_sales_r
		, SUM(ty_net_sales_r) as ty_net_sales_r
		, SUM(ly_net_sales_r) as ly_net_sales_r
		, SUM(cp_net_sales_u) as cp_net_sales_u
		, SUM(op_net_sales_u) as op_net_sales_u
		, SUM(sp_net_sales_u) as sp_net_sales_u
		, SUM(ty_net_sales_u) as ty_net_sales_u
		, SUM(ly_net_sales_u) as ly_net_sales_u
		, SUM(cp_rcpts_total_c) as cp_rcpts_total_c
		, SUM(op_rcpts_total_c) as op_rcpts_total_c
		, SUM(sp_rcpts_total_c) as sp_rcpts_total_c
		, SUM(ty_rcpts_total_c) as ty_rcpts_total_c
		, SUM(ly_rcpts_total_c) as ly_rcpts_total_c
		, SUM(cp_rcpts_total_u) as cp_rcpts_total_u
		, SUM(op_rcpts_total_u) as op_rcpts_total_u
		, SUM(sp_rcpts_total_u) as sp_rcpts_total_u
		, SUM(ty_rcpts_total_u) as ty_rcpts_total_u
		, SUM(ly_rcpts_total_u) as ly_rcpts_total_u
		, SUM(ty_rcpts_active_cost_amt) AS ty_rcpts_active_cost_amt
	    , SUM(sp_rcpts_active_cost_amt) AS sp_rcpts_active_cost_amt
	    , SUM(op_rcpts_active_cost_amt) AS op_rcpts_active_cost_amt
	    , SUM(ly_rcpts_active_cost_amt) AS ly_rcpts_active_cost_amt
	    , SUM(ty_rcpts_active_u) AS ty_rcpts_active_u
	    , SUM(sp_rcpts_active_u) AS sp_rcpts_active_u
	    , SUM(op_rcpts_active_u) AS op_rcpts_active_u
	    , SUM(ly_rcpts_active_u) AS ly_rcpts_active_u
	    , SUM(TY_DEMAND_TOTAL_RETAIL_AMT_ONLINE) as ty_demand_total_retail_amt_online
	    , SUM(SP_DEMAND_TOTAL_RETAIL_AMT_ONLINE) as sp_demand_total_retail_amt_online
	    , sum(OP_DEMAND_TOTAL_RETAIL_AMT_ONLINE) as op_demand_total_retail_amt_online
	    , sum(LY_DEMAND_TOTAL_RETAIL_AMT_ONLINE) as ly_demand_total_retail_amt_online
	    , sum(TY_DEMAND_TOTAL_UNITS_ONLINE) as ty_demand_total_units_online
	    , sum(SP_DEMAND_TOTAL_UNITS_ONLINE) as sp_demand_total_units_online
	    , sum(OP_DEMAND_TOTAL_UNITS_ONLINE) as op_demand_total_units_online
	    , sum(LY_DEMAND_TOTAL_UNITS_ONLINE) as ly_demand_total_units_online
	    , sum(TY_RETURNS_RETAIL_AMT) as ty_returns_retail_amt
	    , sum(SP_RETURNS_RETAIL_AMT) as sp_returns_retail_amt
	    , sum(OP_RETURNS_RETAIL_AMT) as op_returns_retail_amt
	    , sum(LY_RETURNS_RETAIL_AMT) as ly_returns_retail_amt
	    , sum(TY_RETURNS_UNITS) as ty_returns_units
	    , sum(SP_RETURNS_UNITS) as sp_returns_units
	    , sum(OP_RETURNS_UNITS) as op_returns_units
	    , sum(LY_RETURNS_UNITS) as ly_returns_units
	  		FROM
			(SELECT
				data_source
					, ty_ly_ind
					, week_idnt
					, month_idnt
					, banner
					, country
					, channel
					, division
					, subdivision
					, department
					, dept_num
					, price_type
					, category
					, supplier_group
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
					, wbr_sales_units
					, wbr_eoh_units
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
					FROM MFP
			UNION ALL
			SELECT
			data_source
				, ty_ly_ind
				, week_idnt
				, month_idnt
				, banner
				, country
				, channel
				, division
				, subdivision
				, department
				, dept_num
				, price_type
				, category
				, supplier_group
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
				, wbr_sales_units
				, wbr_eoh_units
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
			FROM WBR
			UNION ALL
			SELECT
			data_source
				, ty_ly_ind
				, week_idnt
				, month_idnt
				, banner
				, country
				, channel
				, division
				, subdivision
				, department
				, dept_num
				, price_type
				, category
				, supplier_group
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
				, wbr_sales_units
				, wbr_eoh_units
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
			FROM APT) Temp
			LEFT JOIN
				(SELECT DISTINCT week_idnt, fiscal_week_num, week_454_label FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM) AS wl1
			ON wl1.week_idnt = Temp.week_idnt
			LEFT JOIN
				(SELECT DISTINCT month_idnt, fiscal_month_num, month_abrv, fiscal_year_num FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM) AS wl2
			ON wl2.month_idnt  = Temp.month_idnt
			GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)WITH DATA
	PRIMARY INDEX (  department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group)
	ON COMMIT PRESERVE ROWS;

	COLLECT STATS
		PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group
		)
	ON union_table;

-- get realigned fiscal week/month on final table
CREATE MULTISET VOLATILE TABLE final_table AS
(
SELECT
ut.data_source
	, ut.week_idnt
	, ut.month_idnt
	, ut.fiscal_week
	, ut.fiscal_month
	, ut.fiscal_year
	, ut.ty_ly_ind
	, date_align.week_454_label AS fiscal_week_realigned
	, TRIM(date_align2.fiscal_year_num) || ' ' || TRIM(date_align2.fiscal_month_num) || ' ' || TRIM(date_align2.month_abrv) AS fiscal_month_realigned
	, ut.banner
	, ut.country
	, ut.channel
	, ut.division
	, ut.subdivision
	, ut.department
	, ut.price_type
	, ut.category
	, ut.supplier_group
	, aor.general_merch_manager_executive_vice_president
	, aor.div_merch_manager_senior_vice_president
	, aor.div_merch_manager_vice_president
	, aor.merch_director
	, aor.buyer
	, aor.merch_planning_executive_vice_president
	, aor.merch_planning_senior_vice_president
	, aor.merch_planning_vice_president
	, aor.merch_planning_director_manager
	, aor.assortment_planner
	, ut.apt_net_sls_r
	, ut.apt_fut_net_sls_r_dollars
	, ut.apt_net_sls_c
	, ut.apt_fut_net_sls_c_dollars
	, ut.apt_net_sls_units
	, ut.apt_fut_net_sls_c_units
	, ut.apt_demand_r_online
	, ut.apt_fut_demand_r_online
	, ut.apt_eop_ttl_units
	, ut.apt_eop_ttl_c
	, ut.apt_fut_eop_ttl_c
	, ut.apt_ttl_porcpt_c
	, ut.apt_fut_rcpt_need_c
	, ut.apt_plan_bop_c
	, ut.apt_plan_bop_u
	, ut.apt_plan_eop_u
	, ut.wbr_sales_dollars
	, ut.wbr_sales_units
	, ut.wbr_eoh_units
	, ut.wbr_eoh_cost
	, ut.wbr_sales_cost
	, ut.wbr_sales_pm
	, ut.cp_bop_total_c
	, ut.op_bop_total_c
	, ut.sp_bop_total_c
	, ut.ty_bop_total_c
	, ut.ly_bop_total_c
	, ut.cp_bop_total_u
	, ut.op_bop_total_u
	, ut.sp_bop_total_u
	, ut.ty_bop_total_u
	, ut.ly_bop_total_u
	, ut.cp_eop_active_c
	, ut.op_eop_active_c
	, ut.sp_eop_active_c
	, ut.ty_eop_active_c
	, ut.ly_eop_active_c
	, ut.cp_eop_active_u
	, ut.op_eop_active_u
	, ut.sp_eop_active_u
	, ut.ty_eop_active_u
	, ut.ly_eop_active_u
	, ut.cp_eop_total_c
	, ut.op_eop_total_c
	, ut.sp_eop_total_c
	, ut.ty_eop_total_c
	, ut.ly_eop_total_c
	, ut.cp_eop_total_u
	, ut.op_eop_total_u
	, ut.sp_eop_total_u
	, ut.ty_eop_total_u
	, ut.ly_eop_total_u
	, ut.cp_gross_margin
	, ut.op_gross_margin
	, ut.sp_gross_margin
	, ut.ty_gross_margin
	, ut.ly_gross_margin
	, ut.cp_merch_margin
	, ut.op_merch_margin
	, ut.sp_merch_margin
	, ut.ty_merch_margin
	, ut.ly_merch_margin
	, ut.cp_net_sales_c
	, ut.op_net_sales_c
	, ut.sp_net_sales_c
	, ut.ty_net_sales_c
	, ut.ly_net_sales_c
	, ut.cp_net_sales_r
	, ut.op_net_sales_r
	, ut.sp_net_sales_r
	, ut.ty_net_sales_r
	, ut.ly_net_sales_r
	, ut.cp_net_sales_u
	, ut.op_net_sales_u
	, ut.sp_net_sales_u
	, ut.ty_net_sales_u
	, ut.ly_net_sales_u
	, ut.cp_rcpts_total_c
	, ut.op_rcpts_total_c
	, ut.sp_rcpts_total_c
	, ut.ty_rcpts_total_c
	, ut.ly_rcpts_total_c
	, ut.cp_rcpts_total_u
	, ut.op_rcpts_total_u
	, ut.sp_rcpts_total_u
	, ut.ty_rcpts_total_u
	, ut.ly_rcpts_total_u
	, ut.ty_rcpts_active_cost_amt
	, ut.sp_rcpts_active_cost_amt
	, ut.op_rcpts_active_cost_amt
	, ut.ly_rcpts_active_cost_amt
	, ut.ty_rcpts_active_u
	, ut.sp_rcpts_active_u
	, ut.op_rcpts_active_u
	, ut.ly_rcpts_active_u
	, ut.ty_demand_total_retail_amt_online
	, ut.sp_demand_total_retail_amt_online
	, ut.op_demand_total_retail_amt_online
	, ut.ly_demand_total_retail_amt_online
	, ut.ty_demand_total_units_online
	, ut.sp_demand_total_units_online
	, ut.op_demand_total_units_online
	, ut.ly_demand_total_units_online
	, ut.ty_returns_retail_amt
	, ut.sp_returns_retail_amt
	, ut.op_returns_retail_amt
	, ut.ly_returns_retail_amt
	, ut.ty_returns_units
	, ut.sp_returns_units
	, ut.op_returns_units
	, ut.ly_returns_units
FROM union_table ut
LEFT JOIN
	(SELECT distinct week_454_label,fiscal_week_num, fiscal_month_num, month_abrv, fiscal_year_num FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE fiscal_year_num =  (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < current_date)) date_align
		ON ut.fiscal_week_num = date_align.fiscal_week_num
LEFT JOIN
	(SELECT distinct fiscal_month_num,month_abrv, fiscal_year_num  FROM PRD_NAP_USR_VWS.DAY_CAL_454_DIM WHERE fiscal_year_num =  (SELECT MAX(fiscal_year_num) FROM prd_nap_usr_vws.day_cal_454_dim WHERE week_end_day_date < current_date)) date_align2
		ON ut.fiscal_month_num = date_align2.fiscal_month_num
LEFT JOIN aor
	ON ut.banner = aor.banner
	AND ut.dept_num = aor.dept_num
)WITH DATA
	PRIMARY INDEX (  department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group)
	ON COMMIT PRESERVE ROWS;

	COLLECT STATS
		PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group
		)
	ON final_table;

------------------------------------------------------------- END TEMPORARY TABLES -----------------------------------------------------------------------


---------------------------------------------------------------- START MAIN QUERY -------------------------------------------------------------------------

DELETE FROM {environment_schema}.merch_executive_weekly ALL;

INSERT INTO {environment_schema}.merch_executive_weekly
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
		, wbr_sales_units
		, wbr_eoh_units
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
		, current_timestamp as process_tmstp
	FROM final_table
;
COLLECT STATISTICS
    PRIMARY INDEX ( department, week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group )
   on {environment_schema}.merch_executive_weekly;

	 ---------------------------------------------------------------- END MAIN QUERY -------------------------------------------------------------------------
