/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_trips_sandbox_weekly_11521_ACE_ENG;
     Task_Name=ddl_trips_sandbox_weekly;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.ddl_trips_sandbox_weekly
Prior Table Layer: T2DL_DAS_USL.sales_cust_fact
Team/Owner: Customer Analytics
Date Created/Modified: Mar 26 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, and some customer attributes
-- Update Cadence: Weekly

*/
    
/*
T2/Table Name: T2DL_DAS_USL.trips_sandbox_weekly
Team/Owner: Customer Analytics
Date Created/Modified: Mar 26 2024

Note:
-- Purpose of the table: Table to get weekly trips metrics by customer attributed.
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
--
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'trips_sandbox_weekly', OUT_RETURN_MSG);

CREATE MULTISET TABLE {usl_t2_schema}.trips_sandbox_weekly
	, FALLBACK
	, NO BEFORE JOURNAL
	, NO AFTER JOURNAL
	, CHECKSUM = DEFAULT 
	, DEFAULT MERGEBLOCKRATIO
	(
		week_num INTEGER
		, month_num INTEGER
		, quarter_num INTEGER
		, year_num INTEGER
		, week_num_realigned INTEGER
		, month_num_realigned INTEGER
		, quarter_num_realigned INTEGER
		, year_num_realigned INTEGER
		, year_id VARCHAR(5) CHARACTER SET UNICODE NOT CASESPECIFIC
		, region VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, AEC VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, predicted_segment VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_level VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, new_to_jwn INTEGER
		--
		-- Calculated Metrics
		--
		-- TRIP BY CHANNEL
		--
		, trips_fls INTEGER
		, trips_ncom INTEGER
		, trips_rs INTEGER
		, trips_rcom INTEGER
		, trips_JWN INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, net_spend_fls DECIMAL(12,2) compress
		, net_spend_ncom DECIMAL(12,2) compress
		, net_spend_rs DECIMAL(12,2) compress
		, net_spend_rcom DECIMAL(12,2) compress
		, net_spend_JWN DECIMAL(12,2) compress
		--
		-- GROSS SALES BY CHANNEL
		--
		, gross_spend_fls DECIMAL(12,2) compress
		, gross_spend_ncom DECIMAL(12,2) compress
		, gross_spend_rs DECIMAL(12,2) compress
		, gross_spend_rcom DECIMAL(12,2) compress
		, gross_spend_JWN DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, net_units_fls DECIMAL(12,2) compress
		, net_units_ncom DECIMAL(12,2) compress
		, net_units_rs DECIMAL(12,2) compress
		, net_units_rcom DECIMAL(12,2) compress
		, net_units_JWN DECIMAL(12,2) compress
		--
		-- GROSS UNITS BY CHANNEL
		--
		, gross_units_fls DECIMAL(12,2) compress
		, gross_units_ncom DECIMAL(12,2) compress
		, gross_units_rs DECIMAL(12,2) compress
		, gross_units_rcom DECIMAL(12,2) compress
		, gross_units_JWN DECIMAL(12,2) compress
		-- Calculated Metrics
		--
		-- ACCESSORIES
		--
		-- TRIP BY CHANNEL
		--
		, NS_accessories_weekly_trips INTEGER
		, NCOM_accessories_weekly_trips INTEGER
		, RS_accessories_weekly_trips INTEGER
		, RCOM_accessories_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_accessories_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_accessories_weekly_net_spend DECIMAL(12,2) compress
		, RS_accessories_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_accessories_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_accessories_weekly_net_units INTEGER 
		, NCOM_accessories_weekly_net_units INTEGER
		, RS_accessories_weekly_net_units INTEGER
		, RCOM_accessories_weekly_net_units INTEGER
		--
		-- APPAREL
		--
		-- TRIP BY CHANNEL
		--
		, NS_apparel_weekly_trips INTEGER
		, NCOM_apparel_weekly_trips INTEGER
		, RS_apparel_weekly_trips INTEGER
		, RCOM_apparel_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_apparel_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_apparel_weekly_net_spend DECIMAL(12,2) compress
		, RS_apparel_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_apparel_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_apparel_weekly_net_units DECIMAL(12,2) compress
		, NCOM_apparel_weekly_net_units DECIMAL(12,2) compress
		, RS_apparel_weekly_net_units DECIMAL(12,2) compress
		, RCOM_apparel_weekly_net_units DECIMAL(12,2) compress
		--
		-- BEAUTY
		--
		-- TRIP BY CHANNEL
		--
		, NS_beauty_weekly_trips INTEGER
		, NCOM_beauty_weekly_trips INTEGER
		, RS_beauty_weekly_trips INTEGER
		, RCOM_beauty_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_beauty_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_beauty_weekly_net_spend DECIMAL(12,2) compress
		, RS_beauty_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_beauty_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_beauty_weekly_net_units DECIMAL(12,2) compress
		, NCOM_beauty_weekly_net_units DECIMAL(12,2) compress
		, RS_beauty_weekly_net_units DECIMAL(12,2) compress
		, RCOM_beauty_weekly_net_units DECIMAL(12,2) compress
		--
		-- DESIGNER
		--
		-- TRIP BY CHANNEL
		--
		, NS_designer_weekly_trips INTEGER
		, NCOM_designer_weekly_trips INTEGER
		, RS_designer_weekly_trips INTEGER
		, RCOM_designer_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_designer_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_designer_weekly_net_spend DECIMAL(12,2) compress
		, RS_designer_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_designer_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_designer_weekly_net_units DECIMAL(12,2) compress
		, NCOM_designer_weekly_net_units DECIMAL(12,2) compress
		, RS_designer_weekly_net_units DECIMAL(12,2) compress
		, RCOM_designer_weekly_net_units DECIMAL(12,2) compress
		--
		-- HOME
		--
		-- TRIP BY CHANNEL
		--
		, NS_home_weekly_trips INTEGER
		, NCOM_home_weekly_trips INTEGER
		, RS_home_weekly_trips INTEGER
		, RCOM_home_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_home_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_home_weekly_net_spend DECIMAL(12,2) compress
		, RS_home_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_home_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_home_weekly_net_units DECIMAL(12,2) compress
		, NCOM_home_weekly_net_units DECIMAL(12,2) compress
		, RS_home_weekly_net_units DECIMAL(12,2) compress
		, RCOM_home_weekly_net_units DECIMAL(12,2) compress
		--
		-- MERCH
		--
		-- TRIP BY CHANNEL
		--
		, NS_merch_weekly_trips INTEGER
		, NCOM_merch_weekly_trips INTEGER
		, RS_merch_weekly_trips INTEGER
		, RCOM_merch_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_merch_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_merch_weekly_net_spend DECIMAL(12,2) compress
		, RS_merch_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_merch_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_merch_weekly_net_units DECIMAL(12,2) compress
		, NCOM_merch_weekly_net_units DECIMAL(12,2) compress
		, RS_merch_weekly_net_units DECIMAL(12,2) compress
		, RCOM_merch_weekly_net_units DECIMAL(12,2) compress
		--
		-- SHOES
		--
		-- TRIP BY CHANNEL
		--
		, NS_shoes_weekly_trips INTEGER
		, NCOM_shoes_weekly_trips INTEGER
		, RS_shoes_weekly_trips INTEGER
		, RCOM_shoes_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_shoes_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_shoes_weekly_net_spend DECIMAL(12,2) compress
		, RS_shoes_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_shoes_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_shoes_weekly_net_units DECIMAL(12,2) compress
		, NCOM_shoes_weekly_net_units DECIMAL(12,2) compress
		, RS_shoes_weekly_net_units DECIMAL(12,2) compress
		, RCOM_shoes_weekly_net_units DECIMAL(12,2) compress
		--
		-- OTHER
		--
		-- TRIP BY CHANNEL
		--
		, NS_other_weekly_trips INTEGER
		, NCOM_other_weekly_trips INTEGER
		, RS_other_weekly_trips INTEGER
		, RCOM_other_weekly_trips INTEGER
		--
		-- NET SALES BY CHANNEL
		--
		, NS_other_weekly_net_spend DECIMAL(12,2) compress
		, NCOM_other_weekly_net_spend DECIMAL(12,2) compress
		, RS_other_weekly_net_spend DECIMAL(12,2) compress
		, RCOM_other_weekly_net_spend DECIMAL(12,2) compress
		--
		-- NET UNITS BY CHANNEL
		--
		, NS_other_weekly_net_units DECIMAL(12,2) compress
		, NCOM_other_weekly_net_units DECIMAL(12,2) compress
		, RS_other_weekly_net_units DECIMAL(12,2) compress
		, RCOM_other_weekly_net_units DECIMAL(12,2) compress
		, dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(week_num, region, dma, AEC, predicted_segment, loyalty_level, loyalty_type);

SET QUERY_BAND = NONE FOR SESSION;