/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=APP08818;
     DAG_ID=ddl_date_fact_11521_ACE_ENG;
     Task_Name=ddl_date_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.date_fact
Team/Owner: Customer Analytics/Ian Rasquinha
Date Created/Modified: 06/11/2024

Note:
-- What is the the purpose of the table: Getting fiscal calendar information and realigned calendar information.
-- What is the update cadence/lookback window: weekly refreshment, run every Monday at 8am UTC
*/

-- For testing the table. Comment out before final merge.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'date_fact', OUT_RETURN_MSG);

CREATE SET TABLE {usl_t2_schema}.date_fact ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_MAP1
(
	  date_id DATE
	  , day_num INTEGER
	  , realigned_day_num INTEGER
	  , day_desc VARCHAR(30)
	  , week_num INTEGER
	  , week_desc VARCHAR(30)
	  , realigned_week_num INTEGER
	  , realigned_week_desc VARCHAR(30)
	  , week_454_num INTEGER
	  , month_num INTEGER
	  , month_desc VARCHAR(30)
	  , realigned_month_num INTEGER
	  , realigned_month_desc VARCHAR(30)
	  , quarter_num INTEGER
	  , quarter_desc VARCHAR(30)
	  , realigned_quarter_num INTEGER
	  , halfyear_num INTEGER 
	  , realigned_halfyear_num INTEGER
	  , year_num INTEGER
	  , realigned_year_num INTEGER
	  , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
) PRIMARY INDEX ( date_id );

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/

SET QUERY_BAND = NONE FOR SESSION;