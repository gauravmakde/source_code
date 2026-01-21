/*
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_cust_pop_initiative_exposal_11521_ACE_ENG;
     Task_Name=ddl_cust_pop_initiative_exposal;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.cust_pop_initiative_exposal
Team/Owner: Customer Analytics
Date Created/Modified: Mar 12 2024

Note:
-- Purpose of the table: Table to get the first time that a customer was exposed to the F2DD and/or Beauty5X initiative
-- Update Cadence: Daily

*/

/*
 * I will be adding several metrics to the sales_fact table. The financial metrics being added are:
 * 	country
 * 	region
 * 	dma
 * 	trade_area_type
 * 	engagement_cohort
 * 	predicted_segment
 * 	loyalty_level
 * 	cardmember_flag
 * 	ntn_year
 *
 * Otherwise, the majority of sales_fact table will be pulled to create an additional layer of information.
 */

-- For testing the table. Comment out before final merge.
--  CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'cust_pop_initiative_exposal', OUT_RETURN_MSG);

CREATE MULTISET TABLE T2DL_DAS_USL.cust_pop_initiative_exposal
	, FALLBACK
	, NO BEFORE JOURNAL
	, NO AFTER JOURNAL
	, CHECKSUM = DEFAULT
	, DEFAULT MERGEBLOCKRATIO
	(
		acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, initiative_name VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, first_exposed TIMESTAMP(6)
		, most_recent_initiative_interaction TIMESTAMP(6)
		, dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(acp_id);

SET QUERY_BAND = NONE FOR SESSION;
