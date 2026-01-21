/*
Name:						Merch KPI Scorecard
APPID-Name:					APP09125
Purpose:					Creates table in T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING for Merch KPI Scorecard
Account:					T2DL_NAP_AIS_BATCH
Variable:
	T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING 	t2dl_das_in_season_management_reporting
DAG:						merch_ddl_kpi_scorecard
Author:						Asiyah Fox
Date Created:				2023/12/22
Date Last Updated:			2023/12/22
*/

CALL SYS_MGMT.DROP_IF_EXISTS_SP ('T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING', 'kpi_scorecard', OUT_RETURN_MSG);
CREATE MULTISET TABLE T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.kpi_scorecard
    ,FALLBACK
	,NO BEFORE JOURNAL
	,NO AFTER JOURNAL
	,CHECKSUM = DEFAULT
	,DEFAULT MERGEBLOCKRATIO
	,MAP = TD_MAP1
(
	 week_num                                              INTEGER
	,week_num_of_fiscal_month                              INTEGER
	,week_label                                            VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,week_start_day_date                                   DATE
	,week_end_day_date                                     DATE
	,month_num                                             INTEGER
	,month_label                                           VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_desc                                            VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,month_start_day_date                                  DATE
	,month_end_day_date                                    DATE
	,quarter_num                                           INTEGER
	,quarter_label                                         VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
	,quarter_start_day_date                                DATE
	,quarter_end_day_date                                  DATE
	,fiscal_year_num                                       INTEGER
	,mtd_flag                                              BYTEINT
	,qtd_flag                                              BYTEINT
	,ytd_flag                                              BYTEINT
	,banner                                                VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC
	,fulfill_type_num                                      INTEGER
	,dept_num                                              INTEGER
	,dept_label                                            VARCHAR(160) CHARACTER SET UNICODE NOT CASESPECIFIC
	,division_num                                          INTEGER
	,div_label                                             VARCHAR(160) CHARACTER SET UNICODE NOT CASESPECIFIC
	,subdivision_num                                       INTEGER
	,sdiv_label                                            VARCHAR(160) CHARACTER SET UNICODE NOT CASESPECIFIC
	,active_store_ind                                      CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
	,merch_dept_ind                                        CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC
	,general_merch_manager_executive_vice_president        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,div_merch_manager_senior_vice_president               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,div_merch_manager_vice_president                      VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,merch_director                                        VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,buyer                                                 VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,merch_planning_executive_vice_president               VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,merch_planning_senior_vice_president                  VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,merch_planning_vice_president                         VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,merch_planning_director_manager                       VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,assortment_planner                                    VARCHAR(100) CHARACTER SET UNICODE NOT CASESPECIFIC
	,ty_net_sales_retail_amt                               DECIMAL(20,4)
	,ly_net_sales_retail_amt                               DECIMAL(20,4)
	,op_net_sales_retail_amt                               DECIMAL(20,4)
	,sp_net_sales_retail_amt                               DECIMAL(20,4)
	,cp_net_sales_retail_amt                               DECIMAL(20,4)
	,ty_net_sales_cost_amt                                 DECIMAL(20,4)
	,ly_net_sales_cost_amt                                 DECIMAL(20,4)
	,op_net_sales_cost_amt                                 DECIMAL(20,4)
	,sp_net_sales_cost_amt                                 DECIMAL(20,4)
	,cp_net_sales_cost_amt                                 DECIMAL(20,4)
	,ty_net_sales_units                                    BIGINT
	,ly_net_sales_units                                    BIGINT
	,op_net_sales_units                                    BIGINT
	,sp_net_sales_units                                    BIGINT
	,cp_net_sales_units                                    BIGINT
	,ty_merch_margin_amt                                   DECIMAL(20,4)
	,ly_merch_margin_amt                                   DECIMAL(20,4)
	,op_merch_margin_amt                                   DECIMAL(20,4)
	,sp_merch_margin_amt                                   DECIMAL(20,4)
	,cp_merch_margin_amt                                   DECIMAL(20,4)
	,ty_bop_tot_cost_amt                                   DECIMAL(20,4)
	,ly_bop_tot_cost_amt                                   DECIMAL(20,4)
	,op_bop_tot_cost_amt                                   DECIMAL(20,4)
	,sp_bop_tot_cost_amt                                   DECIMAL(20,4)
	,cp_bop_tot_cost_amt                                   DECIMAL(20,4)
	,ty_bop_tot_units                                      BIGINT
	,ly_bop_tot_units                                      BIGINT
	,op_bop_tot_units                                      BIGINT
	,sp_bop_tot_units                                      BIGINT
	,cp_bop_tot_units                                      BIGINT
	,ty_eop_tot_cost_amt                                   DECIMAL(20,4)
	,ly_eop_tot_cost_amt                                   DECIMAL(20,4)
	,op_eop_tot_cost_amt                                   DECIMAL(20,4)
	,sp_eop_tot_cost_amt                                   DECIMAL(20,4)
	,cp_eop_tot_cost_amt                                   DECIMAL(20,4)
	,ty_eop_tot_units                                      BIGINT
	,ly_eop_tot_units                                      BIGINT
	,op_eop_tot_units                                      BIGINT
	,sp_eop_tot_units                                      BIGINT
	,cp_eop_tot_units                                      BIGINT
	,ty_rcpts_tot_cost_amt                                 DECIMAL(20,4)
	,ly_rcpts_tot_cost_amt                                 DECIMAL(20,4)
	,op_rcpts_tot_cost_amt                                 DECIMAL(20,4)
	,sp_rcpts_tot_cost_amt                                 DECIMAL(20,4)
	,cp_rcpts_tot_cost_amt                                 DECIMAL(20,4)
	,ty_rcpts_tot_units                                    BIGINT
	,ly_rcpts_tot_units                                    BIGINT
	,op_rcpts_tot_units                                    BIGINT
	,sp_rcpts_tot_units                                    BIGINT
	,cp_rcpts_tot_units                                    BIGINT
	,updated_timestamp                                     TIMESTAMP(6) WITH TIME ZONE
)
PRIMARY INDEX (week_num, banner, dept_num, fulfill_type_num)
PARTITION BY RANGE_N(week_start_day_date BETWEEN DATE '2011-01-01' AND DATE '2031-12-31' EACH INTERVAL '1' DAY , NO RANGE)
;
GRANT SELECT ON T2DL_DAS_IN_SEASON_MANAGEMENT_REPORTING.kpi_scorecard TO PUBLIC
;