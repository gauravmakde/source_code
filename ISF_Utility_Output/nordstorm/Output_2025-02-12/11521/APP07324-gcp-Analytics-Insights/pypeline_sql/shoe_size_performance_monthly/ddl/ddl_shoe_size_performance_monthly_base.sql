
SET QUERY_BAND = 'App_ID=APP07324;
    DAG_ID=shoe_size_performance_monthly_base_11521_ACE_ENG;
    Task_Name=ddl_shoe_size_performance_monthly_base;'
    FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_ccs_categories.shoe_size_performance_monthly_base
Team/Owner: Merchandising Insights
Date Created/Modified: 2/9/2023

Note:
-- Purpose: Normalized shoe sizes with monthly performance metrics
-- Cadence/lookback window: Runs weekly on Sunday at 19:30 and looks back one month to update merch metrics

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
 --CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'shoe_size_performance_monthly_base', OUT_RETURN_MSG);

create multiset table {shoe_categories_t2_schema}.shoe_size_performance_monthly_base
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
    rms_sku_num                         varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC 
    ,store_num                          integer  
	,week_num							integer
	,rp_idnt                            varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
	,ds_ind                       		varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,price_type                         varchar(150) CHARACTER SET UNICODE NOT CASESPECIFIC compress
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
PRIMARY INDEX (rms_sku_num, store_num, week_num);

COMMENT ON  {shoe_categories_t2_schema}.shoe_size_performance_monthly_base IS 'Normalized shoe sizes with monthly performance metrics';

SET QUERY_BAND = NONE FOR SESSION; 

