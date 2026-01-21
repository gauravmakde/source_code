/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_trips_sandbox_monthly_yoy_11521_ACE_ENG;
     Task_Name=ddl_trips_sandbox_monthly_yoy;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.ddl_trips_sandbox_monthly_yoy
Prior Table Layer: T2DL_DAS_USL.sales_cust_fact
Team/Owner: Customer Analytics
Date Created/Modified: Mar 26 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, and some customer attributes
-- Update Cadence: Weekly

*/
    
/*
T2/Table Name: T2DL_DAS_USL.ddl_trips_sandbox_monthly_yoy
Team/Owner: Customer Analytics
Date Created/Modified: Mar 26 2024

Note:
-- Purpose of the table: Table to get monthly trips metrics by customer attributed.
-- Update Cadence: Daily

*/


/*
 * Written: Ian Rasquinha
 * Date Created: 3/21/2024
 * Date Last Edited: 3/26/2024
 * 
 * Goal: The goal of this code is to create a table level before joining on any of the customer attributes to correct the trips first. 
 */
/******************************************************************************************************************/

-- For testing the table. Comment out before final merge.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'trips_sandbox_monthly_yoy', OUT_RETURN_MSG);

CREATE MULTISET TABLE {usl_t2_schema}.trips_sandbox_monthly_yoy
	, FALLBACK
	, NO BEFORE JOURNAL
	, NO AFTER JOURNAL
	, CHECKSUM = DEFAULT 
	, DEFAULT MERGEBLOCKRATIO
	(
		month_num INTEGER
		, quarter_num INTEGER
		, year_num INTEGER
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
		-- Calculated Metrics
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, NS_accessories_trips_ty INTEGER
		, NCOM_accessories_trips_ty INTEGER
		, RS_accessories_trips_ty INTEGER
		, RCOM_accessories_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_accessories_net_spend_ty DECIMAL(12,2) compress
		, NCOM_accessories_net_spend_ty DECIMAL(12,2) compress
		, RS_accessories_net_spend_ty DECIMAL(12,2) compress
		, RCOM_accessories_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_accessories_net_units_ty INTEGER 
		, NCOM_accessories_net_units_ty INTEGER
		, RS_accessories_net_units_ty INTEGER
		, RCOM_accessories_net_units_ty INTEGER
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, NS_apparel_trips_ty INTEGER
		, NCOM_apparel_trips_ty INTEGER
		, RS_apparel_trips_ty INTEGER
		, RCOM_apparel_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_apparel_net_spend_ty DECIMAL(12,2) compress
		, NCOM_apparel_net_spend_ty DECIMAL(12,2) compress
		, RS_apparel_net_spend_ty DECIMAL(12,2) compress
		, RCOM_apparel_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_apparel_net_units_ty DECIMAL(12,2) compress
		, NCOM_apparel_net_units_ty DECIMAL(12,2) compress
		, RS_apparel_net_units_ty DECIMAL(12,2) compress
		, RCOM_apparel_net_units_ty DECIMAL(12,2) compress
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, NS_beauty_trips_ty INTEGER
		, NCOM_beauty_trips_ty INTEGER
		, RS_beauty_trips_ty INTEGER
		, RCOM_beauty_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_beauty_net_spend_ty DECIMAL(12,2) compress
		, NCOM_beauty_net_spend_ty DECIMAL(12,2) compress
		, RS_beauty_net_spend_ty DECIMAL(12,2) compress
		, RCOM_beauty_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_beauty_net_units_ty DECIMAL(12,2) compress
		, NCOM_beauty_net_units_ty DECIMAL(12,2) compress
		, RS_beauty_net_units_ty DECIMAL(12,2) compress
		, RCOM_beauty_net_units_ty DECIMAL(12,2) compress
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, NS_designer_trips_ty INTEGER
		, NCOM_designer_trips_ty INTEGER
		, RS_designer_trips_ty INTEGER
		, RCOM_designer_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_designer_net_spend_ty DECIMAL(12,2) compress
		, NCOM_designer_net_spend_ty DECIMAL(12,2) compress
		, RS_designer_net_spend_ty DECIMAL(12,2) compress
		, RCOM_designer_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_designer_net_units_ty DECIMAL(12,2) compress
		, NCOM_designer_net_units_ty DECIMAL(12,2) compress
		, RS_designer_net_units_ty DECIMAL(12,2) compress
		, RCOM_designer_net_units_ty DECIMAL(12,2) compress
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, NS_home_trips_ty INTEGER
		, NCOM_home_trips_ty INTEGER
		, RS_home_trips_ty INTEGER
		, RCOM_home_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_home_net_spend_ty DECIMAL(12,2) compress
		, NCOM_home_net_spend_ty DECIMAL(12,2) compress
		, RS_home_net_spend_ty DECIMAL(12,2) compress
		, RCOM_home_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_home_net_units_ty DECIMAL(12,2) compress
		, NCOM_home_net_units_ty DECIMAL(12,2) compress
		, RS_home_net_units_ty DECIMAL(12,2) compress
		, RCOM_home_net_units_ty DECIMAL(12,2) compress
		--
		-- MERCH
		--
		-- TRIP BY CHANNEL
		--
		, NS_merch_trips_ty INTEGER
		, NCOM_merch_trips_ty INTEGER
		, RS_merch_trips_ty INTEGER
		, RCOM_merch_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_merch_net_spend_ty DECIMAL(12,2) compress
		, NCOM_merch_net_spend_ty DECIMAL(12,2) compress
		, RS_merch_net_spend_ty DECIMAL(12,2) compress
		, RCOM_merch_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_merch_net_units_ty DECIMAL(12,2) compress
		, NCOM_merch_net_units_ty DECIMAL(12,2) compress
		, RS_merch_net_units_ty DECIMAL(12,2) compress
		, RCOM_merch_net_units_ty DECIMAL(12,2) compress
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, NS_shoes_trips_ty INTEGER
		, NCOM_shoes_trips_ty INTEGER
		, RS_shoes_trips_ty INTEGER
		, RCOM_shoes_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_shoes_net_spend_ty DECIMAL(12,2) compress
		, NCOM_shoes_net_spend_ty DECIMAL(12,2) compress
		, RS_shoes_net_spend_ty DECIMAL(12,2) compress
		, RCOM_shoes_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_shoes_net_units_ty DECIMAL(12,2) compress
		, NCOM_shoes_net_units_ty DECIMAL(12,2) compress
		, RS_shoes_net_units_ty DECIMAL(12,2) compress
		, RCOM_shoes_net_units_ty DECIMAL(12,2) compress
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, NS_other_trips_ty INTEGER
		, NCOM_other_trips_ty INTEGER
		, RS_other_trips_ty INTEGER
		, RCOM_other_trips_ty INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_other_net_spend_ty DECIMAL(12,2) compress
		, NCOM_other_net_spend_ty DECIMAL(12,2) compress
		, RS_other_net_spend_ty DECIMAL(12,2) compress
		, RCOM_other_net_spend_ty DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_other_net_units_ty DECIMAL(12,2) compress
		, NCOM_other_net_units_ty DECIMAL(12,2) compress
		, RS_other_net_units_ty DECIMAL(12,2) compress
		, RCOM_other_net_units_ty DECIMAL(12,2) compress
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
		-- Calculated Metrics
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, NS_accessories_trips_ly INTEGER
		, NCOM_accessories_trips_ly INTEGER
		, RS_accessories_trips_ly INTEGER
		, RCOM_accessories_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_accessories_net_spend_ly DECIMAL(12,2) compress
		, NCOM_accessories_net_spend_ly DECIMAL(12,2) compress
		, RS_accessories_net_spend_ly DECIMAL(12,2) compress
		, RCOM_accessories_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_accessories_net_units_ly INTEGER 
		, NCOM_accessories_net_units_ly INTEGER
		, RS_accessories_net_units_ly INTEGER
		, RCOM_accessories_net_units_ly INTEGER
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, NS_apparel_trips_ly INTEGER
		, NCOM_apparel_trips_ly INTEGER
		, RS_apparel_trips_ly INTEGER
		, RCOM_apparel_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_apparel_net_spend_ly DECIMAL(12,2) compress
		, NCOM_apparel_net_spend_ly DECIMAL(12,2) compress
		, RS_apparel_net_spend_ly DECIMAL(12,2) compress
		, RCOM_apparel_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_apparel_net_units_ly DECIMAL(12,2) compress
		, NCOM_apparel_net_units_ly DECIMAL(12,2) compress
		, RS_apparel_net_units_ly DECIMAL(12,2) compress
		, RCOM_apparel_net_units_ly DECIMAL(12,2) compress
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, NS_beauty_trips_ly INTEGER
		, NCOM_beauty_trips_ly INTEGER
		, RS_beauty_trips_ly INTEGER
		, RCOM_beauty_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_beauty_net_spend_ly DECIMAL(12,2) compress
		, NCOM_beauty_net_spend_ly DECIMAL(12,2) compress
		, RS_beauty_net_spend_ly DECIMAL(12,2) compress
		, RCOM_beauty_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_beauty_net_units_ly DECIMAL(12,2) compress
		, NCOM_beauty_net_units_ly DECIMAL(12,2) compress
		, RS_beauty_net_units_ly DECIMAL(12,2) compress
		, RCOM_beauty_net_units_ly DECIMAL(12,2) compress
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, NS_designer_trips_ly INTEGER
		, NCOM_designer_trips_ly INTEGER
		, RS_designer_trips_ly INTEGER
		, RCOM_designer_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_designer_net_spend_ly DECIMAL(12,2) compress
		, NCOM_designer_net_spend_ly DECIMAL(12,2) compress
		, RS_designer_net_spend_ly DECIMAL(12,2) compress
		, RCOM_designer_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_designer_net_units_ly DECIMAL(12,2) compress
		, NCOM_designer_net_units_ly DECIMAL(12,2) compress
		, RS_designer_net_units_ly DECIMAL(12,2) compress
		, RCOM_designer_net_units_ly DECIMAL(12,2) compress
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, NS_home_trips_ly INTEGER
		, NCOM_home_trips_ly INTEGER
		, RS_home_trips_ly INTEGER
		, RCOM_home_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_home_net_spend_ly DECIMAL(12,2) compress
		, NCOM_home_net_spend_ly DECIMAL(12,2) compress
		, RS_home_net_spend_ly DECIMAL(12,2) compress
		, RCOM_home_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_home_net_units_ly DECIMAL(12,2) compress
		, NCOM_home_net_units_ly DECIMAL(12,2) compress
		, RS_home_net_units_ly DECIMAL(12,2) compress
		, RCOM_home_net_units_ly DECIMAL(12,2) compress
		--
		-- MERCH
		--
		-- TRIP BY CHANNEL
		--
		, NS_merch_trips_ly INTEGER
		, NCOM_merch_trips_ly INTEGER
		, RS_merch_trips_ly INTEGER
		, RCOM_merch_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_merch_net_spend_ly DECIMAL(12,2) compress
		, NCOM_merch_net_spend_ly DECIMAL(12,2) compress
		, RS_merch_net_spend_ly DECIMAL(12,2) compress
		, RCOM_merch_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_merch_net_units_ly DECIMAL(12,2) compress
		, NCOM_merch_net_units_ly DECIMAL(12,2) compress
		, RS_merch_net_units_ly DECIMAL(12,2) compress
		, RCOM_merch_net_units_ly DECIMAL(12,2) compress
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, NS_shoes_trips_ly INTEGER
		, NCOM_shoes_trips_ly INTEGER
		, RS_shoes_trips_ly INTEGER
		, RCOM_shoes_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_shoes_net_spend_ly DECIMAL(12,2) compress
		, NCOM_shoes_net_spend_ly DECIMAL(12,2) compress
		, RS_shoes_net_spend_ly DECIMAL(12,2) compress
		, RCOM_shoes_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_shoes_net_units_ly DECIMAL(12,2) compress
		, NCOM_shoes_net_units_ly DECIMAL(12,2) compress
		, RS_shoes_net_units_ly DECIMAL(12,2) compress
		, RCOM_shoes_net_units_ly DECIMAL(12,2) compress
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, NS_other_trips_ly INTEGER
		, NCOM_other_trips_ly INTEGER
		, RS_other_trips_ly INTEGER
		, RCOM_other_trips_ly INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_other_net_spend_ly DECIMAL(12,2) compress
		, NCOM_other_net_spend_ly DECIMAL(12,2) compress
		, RS_other_net_spend_ly DECIMAL(12,2) compress
		, RCOM_other_net_spend_ly DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_other_net_units_ly DECIMAL(12,2) compress
		, NCOM_other_net_units_ly DECIMAL(12,2) compress
		, RS_other_net_units_ly DECIMAL(12,2) compress
		, RCOM_other_net_units_ly DECIMAL(12,2) compress
		, dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(month_num, region, dma, AEC, predicted_segment, loyalty_level, loyalty_type);

SET QUERY_BAND = NONE FOR SESSION;