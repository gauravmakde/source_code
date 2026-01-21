/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=ddl_item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=create_online_sales;'
     FOR SESSION VOLATILE;



/*
T2/Table Name: IDF_ONLINE_SALE
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2024-02

Note:
This table all online sales data by EPM_CHOICE_NUM X WEEK from the clarity table

*/

--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'IDF_ONLINE_SALE', OUT_RETURN_MSG);
create multiset table {ip_forecast_t2_schema}.IDF_ONLINE_SALE
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
    last_updated_utc DATE,
    dma_cd INTEGER,
	  order_line_quantity  DECIMAL(38,6),
		order_line_amount_usd  DECIMAL(38,6),
		dropship_order_line_quantity  DECIMAL(38,6),
		dropship_order_line_amount_usd  DECIMAL(38,6),
		bopus_order_line_quantity DECIMAL(38,6),
		bopus_order_line_amount_usd DECIMAL(38,6),
		fc_fill_order_line_quantity DECIMAL(38,6),
		fc_fill_order_line_amount_usd DECIMAL(38,6),
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL)		
    PRIMARY INDEX ( channel_brand ,selling_channel,epm_choice_num ,week_start_date )
    partition by RANGE_N(week_start_date BETWEEN DATE '2019-01-01' AND DATE '2026-12-31' EACH INTERVAL '1' DAY);

-- Table Comment (STANDARD)
COMMENT ON  {ip_forecast_t2_schema}.IDF_ONLINE_SALE IS 'item demand forecasting online sales table - CC x week' ;
-- Column comments (OPTIONAL)

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;