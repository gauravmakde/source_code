/*
Name:Monday Morning Reporting script
Project:Monday Morning Reporting Dash
Purpose:  Combine MFP, WBR and APT data source into a T2 Table that can direclty feed into Monday Morning Reporting for performance improvment
Variable(s):    {{environment_schema}} T2DL_NAP_AIS_BATCH
                {{env_suffix}} '' or '_dev' tablesuffix for prod testing
DAG:
Author(s):Xiao Tong, Manuela Hurtado
Date Created:3/31/23
Date Last Updated:12/29/23
*/


CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{environment_schema}', 'merch_executive_weekly', OUT_RETURN_MSG);
CREATE MULTISET TABLE {environment_schema}.merch_executive_weekly, FALLBACK,
	NO BEFORE JOURNAL,
	NO AFTER JOURNAL,
	CHECKSUM = DEFAULT,
	DEFAULT MERGEBLOCKRATIO,
	MAP = TD_MAP1
(
  data_source VARCHAR(3) COMPRESS('WBR','APT','MFP')
	, week_idnt INTEGER
	, month_idnt INTEGER
	, fiscal_week VARCHAR(50)
	, fiscal_month VARCHAR(50)
	, fiscal_year INTEGER
	, ty_ly_ind VARCHAR(2) COMPRESS('TY','LY','UH')
	, fiscal_week_realigned VARCHAR(50)
	, fiscal_month_realigned VARCHAR(50)
	, banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, country VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, division VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, subdivision VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, department VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
--  WBR granularity
	, price_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
-- APT granularity
	, category VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
	, supplier_group VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
-- AOR
	, general_merch_manager_executive_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, div_merch_manager_senior_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, div_merch_manager_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_director VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, buyer VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_executive_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_senior_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_vice_president VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, merch_planning_director_manager VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	, assortment_planner VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
-- APT metrics
	, apt_net_sls_r DECIMAL(38,2)
	, apt_fut_net_sls_r_dollars DECIMAL(38,2)
	, apt_net_sls_c DECIMAL(38,2)
	, apt_fut_net_sls_c_dollars DECIMAL(38,2)
	, apt_net_sls_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, apt_fut_net_sls_c_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, apt_demand_r_online DECIMAL(38,2)
	, apt_fut_demand_r_online DECIMAL(38,2)
	, apt_eop_ttl_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, apt_eop_ttl_c DECIMAL(38,2)
	, apt_fut_eop_ttl_c DECIMAL(38,2)
	, apt_ttl_porcpt_c DECIMAL(38,2)
	, apt_fut_rcpt_need_c DECIMAL(38,2)
	, apt_plan_bop_c DECIMAL(38,2)
	, apt_plan_bop_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, apt_plan_eop_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	-- WBR metrics
	, wbr_sales_dollars DECIMAL(38,2)
	, wbr_sales_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, wbr_eoh_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, wbr_eoh_cost DECIMAL(38,2)
	, wbr_sales_cost DECIMAL(38,2)
	, wbr_sales_pm DECIMAL(38,2)
	-- MFP metrics
	-- turn c
	, cp_bop_total_c DECIMAL(38,2)
	, op_bop_total_c DECIMAL(38,2)
	, sp_bop_total_c DECIMAL(38,2)
	, ty_bop_total_c DECIMAL(38,2)
	, ly_bop_total_c DECIMAL(38,2)
	-- turn u
	, cp_bop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, op_bop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, sp_bop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ty_bop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ly_bop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	-- EOP ACT C
	, cp_eop_active_c DECIMAL(38,2)
	, op_eop_active_c DECIMAL(38,2)
	, sp_eop_active_c DECIMAL(38,2)
	, ty_eop_active_c DECIMAL(38,2)
	, ly_eop_active_c DECIMAL(38,2)
	-- EOP ACT U
	, cp_eop_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, op_eop_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, sp_eop_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ty_eop_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ly_eop_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	-- EOP Ttl C
	, cp_eop_total_c DECIMAL(38,2)
	, op_eop_total_c DECIMAL(38,2)
	, sp_eop_total_c DECIMAL(38,2)
	, ty_eop_total_c DECIMAL(38,2)
	, ly_eop_total_c DECIMAL(38,2)
	-- EOP Ttl U
	, cp_eop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, op_eop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, sp_eop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ty_eop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ly_eop_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	-- Product Margin
	, cp_gross_margin DECIMAL(38,2)
	, op_gross_margin DECIMAL(38,2)
	, sp_gross_margin DECIMAL(38,2)
	, ty_gross_margin DECIMAL(38,2)
	, ly_gross_margin DECIMAL(38,2)
	-- Merch Margin, ([TY Merch Margin ]) / SUM([TY Net Sales R]) = MM%,mmroi =[TY Merch Margin ] / ([TY Avg Inv Total C]= SUM([TY BOP Total C orig] + (IF [Max Week per Period] = [Week Num] THEN [TY EOP Total C orig] ELSE 0 END)) / (COUNTD([Week Num]) + 1))
	, cp_merch_margin DECIMAL(38,2)
	, op_merch_margin DECIMAL(38,2)
	, sp_merch_margin DECIMAL(38,2)
	, ty_merch_margin DECIMAL(38,2)
	, ly_merch_margin DECIMAL(38,2)
	-- SUM([TY Net Sales C]) / SUM([TY Net Sales U]) = AUC; Sales C, Sales U; TY avg inv total C: SUM([LY Net Sales R]) / SUM([LY Net Sales U]) = ASP; SUM([TY BOP Total C orig] + (IF [Max Week per Period] = [Week Num] THEN [TY EOP Total C orig] ELSE 0 END)) / (COUNTD([Week Num]) + 1)
	, cp_net_sales_c DECIMAL(38,2)
	, op_net_sales_c DECIMAL(38,2)
	, sp_net_sales_c DECIMAL(38,2)
	, ty_net_sales_c DECIMAL(38,2)
	, ly_net_sales_c DECIMAL(38,2)
	, cp_net_sales_r DECIMAL(38,2)
	, op_net_sales_r DECIMAL(38,2)
	, sp_net_sales_r DECIMAL(38,2)
	, ty_net_sales_r DECIMAL(38,2)
	, ly_net_sales_r DECIMAL(38,2)
	, cp_net_sales_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, op_net_sales_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, sp_net_sales_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ty_net_sales_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ly_net_sales_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	-- Ttl Rcpts C; Ttl Rcpts U
	, cp_rcpts_total_c DECIMAL(38,2)
	, op_rcpts_total_c DECIMAL(38,2)
	, sp_rcpts_total_c DECIMAL(38,2)
	, ty_rcpts_total_c DECIMAL(38,2)
	, ly_rcpts_total_c DECIMAL(38,2)
	, cp_rcpts_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, op_rcpts_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, sp_rcpts_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ty_rcpts_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ly_rcpts_total_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ty_rcpts_active_cost_amt DECIMAL(38,2)
	, sp_rcpts_active_cost_amt DECIMAL(38,2)
	, op_rcpts_active_cost_amt DECIMAL(38,2)
	, ly_rcpts_active_cost_amt DECIMAL(38,2)
	, ty_rcpts_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, sp_rcpts_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, op_rcpts_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, ly_rcpts_active_u INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	-- New Demand and Return Metrics added December 2023
	, ty_demand_total_retail_amt_online DECIMAL(38,2)
    , sp_demand_total_retail_amt_online DECIMAL(38,2)
    , op_demand_total_retail_amt_online DECIMAL(38,2)
    , ly_demand_total_retail_amt_online DECIMAL(38,2)
    , ty_demand_total_units_online INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , sp_demand_total_units_online INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , op_demand_total_units_online INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , ly_demand_total_units_online INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , ty_returns_retail_amt DECIMAL(38,2)
    , sp_returns_retail_amt DECIMAL(38,2)
    , op_returns_retail_amt DECIMAL(38,2)
    , ly_returns_retail_amt DECIMAL(38,2)
    , ty_returns_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , sp_returns_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , op_returns_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
    , ly_returns_units INTEGER COMPRESS (0 ,1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12 ,13 ,14 ,15 ,16 ,17 ,18 ,19 ,20 ,21 ,22 ,24 ,25 )
	, process_tmstp TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX(week_idnt, month_idnt, banner, country, channel, subdivision, division, price_type, category, supplier_group, department)
;

Grant SELECT ON {environment_schema}.merch_executive_weekly TO PUBLIC;