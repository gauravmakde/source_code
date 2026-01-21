/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=ddl_item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=create_store_sales;'
     FOR SESSION VOLATILE;



/*
T2/Table Name: IDF_STORE_SALE
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table all store sales data by EPM_CHOICE_NUM X WEEK from clairty tables
 
*/
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'IDF_STORE_SALE', OUT_RETURN_MSG);
create multiset table {ip_forecast_t2_schema}.IDF_STORE_SALE
	,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    selling_channel VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC,
		epm_choice_num INTEGER,
		week_num INTEGER,
    week_start_date DATE,
		store_num INTEGER,
    last_updated_utc DATE,
	  demand_quantity DECIMAL(38,6),
		store_take_demand DECIMAL(38,6),
		owned_demand DECIMAL(38,6),
		employee_demand DECIMAL(38,6),
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)
    PRIMARY INDEX ( channel_brand ,selling_channel,epm_choice_num ,week_start_date)
    partition by RANGE_N(week_start_date BETWEEN DATE '2019-01-01' AND DATE '2026-12-31' EACH INTERVAL '1' DAY);
COMMENT ON  {ip_forecast_t2_schema}.IDF_STORE_SALE IS 'item demand forecasting online sales table - CC x week' ;

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
