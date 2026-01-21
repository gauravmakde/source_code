/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_trips_sandbox_yoy_11521_ACE_ENG;
     Task_Name=ddl_trips_sandbox_yoy;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.ddl_trips_sandbox_yoy
Prior Table Layer: T2DL_DAS_USL.trips_sandbox_weekly_cust
Team/Owner: Customer Analytics
Date Created/Modified: May 8 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, and some customer attributes on a rolling year basis
-- Update Cadence: Weekly

*/

-- For testing the table. Comment out before final merge.
--  CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'trips_sandbox_yoy', OUT_RETURN_MSG);

CREATE MULTISET TABLE {usl_t2_schema}.trips_sandbox_yoy
	, FALLBACK
	, NO BEFORE JOURNAL
	, NO AFTER JOURNAL
	, CHECKSUM = DEFAULT 
	, DEFAULT MERGEBLOCKRATIO
	(
		year_id VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC
		, region VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, AEC VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, predicted_segment VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_level VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, new_to_jwn INTEGER
		--
		-- Calculated Metrics THIS YEAR
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, cust_count_fls_ty INTEGER
		, cust_count_ncom_ty INTEGER
		, cust_count_rs_ty INTEGER
		, cust_count_rcom_ty INTEGER
		-- -- BY DIGITAL VS STORE		
		, cust_count_stores_ty INTEGER
		, cust_count_digital_ty INTEGER
		-- -- BY BANNER		
		, cust_count_nord_ty INTEGER
		, cust_count_rack_ty INTEGER
		-- BY JWN
		, cust_count_jwn_ty INTEGER
		--
		-- TRIP BY CHANNEL
		--
		, trips_fls_ty INTEGER
		, trips_ncom_ty INTEGER
		, trips_rs_ty INTEGER
		, trips_rcom_ty INTEGER
		, trips_JWN_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, net_spend_fls_ty DECIMAL(12,2) compress
		, net_spend_ncom_ty DECIMAL(12,2) compress
		, net_spend_rs_ty DECIMAL(12,2) compress
		, net_spend_rcom_ty DECIMAL(12,2) compress
		, net_spend_JWN_ty DECIMAL(12,2) compress
		--
		-- GROSS SALES BY CHANNEL
		--
		, gross_spend_fls_ty DECIMAL(12,2) compress
		, gross_spend_ncom_ty DECIMAL(12,2) compress
		, gross_spend_rs_ty DECIMAL(12,2) compress
		, gross_spend_rcom_ty DECIMAL(12,2) compress
		, gross_spend_JWN_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, net_units_fls_ty DECIMAL(12,2) compress
		, net_units_ncom_ty DECIMAL(12,2) compress
		, net_units_rs_ty DECIMAL(12,2) compress
		, net_units_rcom_ty DECIMAL(12,2) compress
		, net_units_JWN_ty DECIMAL(12,2) compress
		--
		-- GROSS UNITS BY CHANNEL
		--
		, gross_units_fls_ty DECIMAL(12,2) compress
		, gross_units_ncom_ty DECIMAL(12,2) compress
		, gross_units_rs_ty DECIMAL(12,2) compress
		, gross_units_rcom_ty DECIMAL(12,2) compress
		, gross_units_JWN_ty DECIMAL(12,2) compress
		--
		--
		-- LAST YEAR
		--
		-- Calculated Metrics
		-- CUST_COUNTS
		-- -- BY CHANNEL		
		, cust_count_fls_ly INTEGER
		, cust_count_ncom_ly INTEGER
		, cust_count_rs_ly INTEGER
		, cust_count_rcom_ly INTEGER
		-- -- BY DIGITAL VS STORE		
		, cust_count_stores_ly INTEGER
		, cust_count_digital_ly INTEGER
		-- -- BY BANNER		
		, cust_count_nord_ly INTEGER
		, cust_count_rack_ly INTEGER
		-- BY JWN
		, cust_count_jwn_ly INTEGER
		--
		-- TRIP BY CHANNEL
		--
		, trips_fls_ly INTEGER
		, trips_ncom_ly INTEGER
		, trips_rs_ly INTEGER
		, trips_rcom_ly INTEGER
		, trips_JWN_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, net_spend_fls_ly DECIMAL(12,2) compress
		, net_spend_ncom_ly DECIMAL(12,2) compress
		, net_spend_rs_ly DECIMAL(12,2) compress
		, net_spend_rcom_ly DECIMAL(12,2) compress
		, net_spend_JWN_ly DECIMAL(12,2) compress
		--
		-- GROSS SALES BY CHANNEL
		--
		, gross_spend_fls_ly DECIMAL(12,2) compress
		, gross_spend_ncom_ly DECIMAL(12,2) compress
		, gross_spend_rs_ly DECIMAL(12,2) compress
		, gross_spend_rcom_ly DECIMAL(12,2) compress
		, gross_spend_JWN_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, net_units_fls_ly DECIMAL(12,2) compress
		, net_units_ncom_ly DECIMAL(12,2) compress
		, net_units_rs_ly DECIMAL(12,2) compress
		, net_units_rcom_ly DECIMAL(12,2) compress
		, net_units_JWN_ly DECIMAL(12,2) compress
		--
		-- GROSS UNITS BY CHANNEL
		--
		, gross_units_fls_ly DECIMAL(12,2) compress
		, gross_units_ncom_ly DECIMAL(12,2) compress
		, gross_units_rs_ly DECIMAL(12,2) compress
		, gross_units_rcom_ly DECIMAL(12,2) compress
		, gross_units_JWN_ly DECIMAL(12,2) compress
		, dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(year_id, AEC, loyalty_level, loyalty_type);

SET QUERY_BAND = NONE FOR SESSION;