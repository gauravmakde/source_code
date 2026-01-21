
SET QUERY_BAND = 'App_ID=APP07324;
     DAG_ID=ddl_shoe_size_performance_monthly_rollup_11521_ACE_ENG;
     Task_Name=ddl_shoe_size_performance_monthly_rollup;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_ccs_categories.shoe_size_performance_monthly_rollup
Team/Owner: Merchandising Insights
Date Created/Modified: 2/13/2024

Note:
-- Purpose: Normalized shoe sizes with monthly performance metrics
-- Cadence/lookback window: Runs weekly on Monday at 02:30 

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'shoe_size_performance_monthly_rollup', OUT_RETURN_MSG);

create multiset table {shoe_categories_t2_schema}.shoe_size_performance_monthly_rollup
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    ty_ly_ind                           varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
	,month_end_date						date
	,quarter_abrv						varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,quarter_label						varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,month_idnt							integer
    ,month_abrv							varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,month_label						varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,year_idnt							integer
    ,rp_idnt                            varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,division                           varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,dept_label                         varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,class_label                        varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,subdivision                        varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,banner                             varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,chnl_idnt                          integer compress 
    ,channel                            varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,loc_idnt                           integer  
    ,loc_label                          varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,store_address_county               varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,region_label                       varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,ccs_subcategory                    varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,quantrix_category                  varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,merch_themes                       varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,supplier                           varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,brand                              varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,supplier_attribute                 varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,npg_ind                            integer compress
    ,price_type                         varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,dropship_ind                       varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,size_1_clean                       varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,left_core_right                    varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,overs_unders                       varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,widths                             varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,wide_calf                          varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,sales_dollars                      decimal(18,2) compress  
    ,sales_cost							decimal(18,2) compress 
	,sales_units                        decimal(18,0) compress 
    ,return_dollars                     decimal(18,2) compress 
	,return_cost						decimal(18,2) compress 
    ,return_units                       decimal(18,0) compress
    ,demand_dollars                     decimal(18,2) compress 
    ,demand_units                       decimal(18,0) compress
    ,eoh_dollars                        decimal(18,2) compress 
	,eoh_cost                           decimal(18,2) compress  
    ,eoh_units                          decimal(18,0) compress 
    ,receipt_dollars                    decimal(18,2) compress 
    ,receipt_units                      decimal(18,0) compress 
    ,receipt_cost                       decimal(18,2) compress
    ,transfer_pah_dollars               decimal(18,2) compress 
    ,transfer_pah_units                 decimal(18,0) compress 
    ,transfer_pah_cost                  decimal(18,2) compress 
    ,oo_dollars				            decimal(18,2) compress
    ,oo_units           				decimal(18,0) compress 
    ,oo_cost       						decimal(18,2) compress 
    ,oo_4weeks_dollars				    decimal(18,2) compress
	,oo_4weeks_units				    decimal(18,0) compress
	,oo_4weeks_cost     			    decimal(18,2) compress
	,oo_12weeks_dollars				    decimal(18,2) compress
	,oo_12weeks_units   			    decimal(18,0) compress
	,oo_12weeks_cost				    decimal(18,2) compress 
	,net_sales_tot_retl_with_cost		decimal(18,2) compress 
    ,dw_sys_load_tmstp                  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
PRIMARY INDEX (month_idnt, loc_idnt, dept_label, size_1_clean, quantrix_category, brand);

COMMENT ON  {shoe_categories_t2_schema}.shoe_size_performance_monthly_rollup IS 'Normalized shoe sizes with monthly performance metrics';

SET QUERY_BAND = NONE FOR SESSION;
