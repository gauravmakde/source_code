/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_trips_sandbox_weekly_cust_11521_ACE_ENG;
     Task_Name=ddl_trips_sandbox_weekly_cust;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.ddl_trips_sandbox_weekly_cust
Prior Table Layer: T2DL_DAS_USL.sales_cust_fact
Team/Owner: Customer Analytics
Date Created/Modified: Mar 26 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, and some customer attributes
-- Update Cadence: Weekly

*/
    
/*
T2/Table Name: T2DL_DAS_USL.trips_sandbox_weekly_cust
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
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'trips_sandbox_weekly_cust', OUT_RETURN_MSG);
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('{usl_t2_schema}', 'trips_sandbox_weekly_cust', OUT_RETURN_MSG);

CREATE MULTISET TABLE {usl_t2_schema}.trips_sandbox_weekly_cust
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
		, acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, region VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, AEC VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, predicted_segment VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_level VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, new_to_jwn INTEGER
		, cust_age_bucket VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		-- Calculated Metrics
		, gross_spend DECIMAL(12,2) compress
		, net_spend DECIMAL(12,2) compress
		, trips INTEGER
		, gross_units DECIMAL(12,2) compress
		, net_units DECIMAL(12,2) compress
		--
        -- Divisions Breakout IN ('ACCESSORIES', 'APPAREL', 'BEAUTY', 'DESIGNER', 'HOME', 'MERCH PROJECT', 'SHOES')
        --
        -- ACCESSORIES
        , div_accessories_flag INTEGER
        -- APPAREL
        , div_apparel_flag INTEGER
        -- BEAUTY
        , div_beauty_flag INTEGER
        -- DESIGNER
        , div_designer_flag INTEGER
        -- HOME
        , div_home_flag INTEGER
        -- MERCH
        , div_merch_flag INTEGER
        -- SHOES
        , div_shoes_flag INTEGER
        -- OTHER
        , div_other_flag INTEGER
        , dw_sys_load_tmstp TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(week_num, acp_id, channel, banner, region, dma, AEC, predicted_segment, loyalty_level, loyalty_type);

SET QUERY_BAND = NONE FOR SESSION;