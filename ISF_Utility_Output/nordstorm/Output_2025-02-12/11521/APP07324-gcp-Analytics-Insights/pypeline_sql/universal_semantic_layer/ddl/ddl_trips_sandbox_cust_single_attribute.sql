/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_trips_sandbox_cust_single_attribute_11521_ACE_ENG;
     Task_Name=ddl_trips_sandbox_cust_single_attribute;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: {usl_t2_schema}.ddl_trips_sandbox_cust_single_attribute
Prior Table Layer: {usl_t2_schema}.trips_sandbox_weekly_cust
Team/Owner: Customer Analytics
Date Created/Modified: August 7 2024

Note:
-- Purpose of the table: Table to directly get single attributes per customer week
-- Update Cadence: Weekly

*/
    
-- For testing the table. Comment out before final merge.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'trips_sandbox_cust_single_attribute', OUT_RETURN_MSG);

CREATE MULTISET TABLE {usl_t2_schema}.trips_sandbox_cust_single_attribute
	, FALLBACK
	, NO BEFORE JOURNAL
	, NO AFTER JOURNAL
	, CHECKSUM = DEFAULT 
	, DEFAULT MERGEBLOCKRATIO
	(
		time_granularity VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, week_num_realigned INTEGER
		, month_num_realigned INTEGER
		, quarter_num_realigned INTEGER
		, year_num_realigned INTEGER
		, acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, region VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, aec VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, predicted_segment VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_level VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, new_to_jwn INTEGER
		, dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(acp_id, time_granularity, week_num_realigned, month_num_realigned, quarter_num_realigned, year_num_realigned);

SET QUERY_BAND = NONE FOR SESSION;