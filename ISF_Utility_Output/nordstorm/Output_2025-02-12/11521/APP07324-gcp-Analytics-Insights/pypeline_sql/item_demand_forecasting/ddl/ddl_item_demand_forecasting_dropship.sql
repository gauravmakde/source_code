/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=APP08073;
     DAG_ID=ddl_item_demand_forecasting_dataprep_11521_ACE_ENG;
     Task_Name=create_dropship;'
     FOR SESSION VOLATILE;

 
-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.

/*
T2/Table Name: IDF_DROPSHIP_INV
Team/Owner: Item Demand Forecasting Team (SCDS)
Date Created/Modified: 2023-11

Note:
This table stores all dropship inventory data by EPM_CHOICE_NUM X WEEK

*/
--CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'IDF_DROPSHIP', OUT_RETURN_MSG);
create multiset table {ip_forecast_t2_schema}.IDF_DROPSHIP
	,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (channel_brand VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC,
    epm_choice_num INTEGER,
    week_num INTEGER,
    week_start_date DATE,
    last_updated_utc DATE,
    dropship_boh DECIMAL(38,6),
    dropship_sku_ct DECIMAL(38,6),
    dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
    PRIMARY INDEX ( channel_brand ,epm_choice_num ,week_start_date )
    partition by RANGE_N(week_start_date BETWEEN DATE '2019-01-01' AND DATE '2026-12-31' EACH INTERVAL '1' DAY);

-- Table Comment (STANDARD)
COMMENT ON  {ip_forecast_t2_schema}.IDF_DROPSHIP IS 'item demand forecasting dropship inventory table - CC x week' ;
-- Column comments (OPTIONAL)

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;
