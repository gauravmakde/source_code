/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_trips_sandbox_cust_count_11521_ACE_ENG;
     Task_Name=ddl_trips_sandbox_cust_count;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.ddl_trips_sandbox_cust_count
Prior Table Layer: T2DL_DAS_USL.trips_sandbox_weekly_cust
Team/Owner: Customer Analytics
Date Created/Modified: May 8 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, and some customer attributes on a rolling year basis
-- Update Cadence: Weekly

*/

-- For testing the table. Comment out before final merge.
--  CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'trips_sandbox_cust_count', OUT_RETURN_MSG);

CREATE MULTISET TABLE {usl_t2_schema}.trips_sandbox_cust_count
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
		, region VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, AEC VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, predicted_segment VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_level VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, new_to_jwn INTEGER
		, cust_count_fls INTEGER
		, cust_count_ncom INTEGER
		, cust_count_rs INTEGER
		, cust_count_rcom INTEGER
		-- -- BY DIGITAL VS STORE		
		, cust_count_stores INTEGER
		, cust_count_digital INTEGER
		-- -- BY BANNER		
		, cust_count_nord INTEGER
		, cust_count_rack INTEGER
		-- BY JWN
		, cust_count_jwn INTEGER
		, dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(time_granularity, AEC, loyalty_level, loyalty_type);

SET QUERY_BAND = NONE FOR SESSION;
