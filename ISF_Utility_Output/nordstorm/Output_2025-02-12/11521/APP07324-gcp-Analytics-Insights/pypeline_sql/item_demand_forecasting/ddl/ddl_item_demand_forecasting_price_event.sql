/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=ddl_item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=create_price_event;'
     FOR SESSION VOLATILE;




/*
T2/Table Name: IDF_PRICE_EVENT
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all pricing event data by EPM_CHOICE_NUM X WEEK 
Includes anniversary flag

 
*/
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'IDF_PRICE_EVENT', OUT_RETURN_MSG);
create multiset table {ip_forecast_t2_schema}.IDF_PRICE_EVENT
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
	current_price_event VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC,
	event_tags VARCHAR(200) CHARACTER SET UNICODE NOT CASESPECIFIC)
    PRIMARY INDEX ( channel_brand ,selling_channel,epm_choice_num ,week_start_date )
    partition by RANGE_N(week_start_date BETWEEN DATE '2019-01-01' AND DATE '2026-12-31' EACH INTERVAL '1' DAY);

-- Table Comment (STANDARD)
COMMENT ON  {ip_forecast_t2_schema}.IDF_PRICE_EVENT IS 'item demand forecasting price event table - CC x week' ;
-- Column comments (OPTIONAL)


/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;