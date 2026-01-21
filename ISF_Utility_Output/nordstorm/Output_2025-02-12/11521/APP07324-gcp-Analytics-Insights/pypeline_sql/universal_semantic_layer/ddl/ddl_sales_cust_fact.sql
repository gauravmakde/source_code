/* 
SQL script must begin with QUERY_BAND SETTINGS
*/

SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_sales_fact_cust_11521_ACE_ENG;
     Task_Name=ddl_sales_fact_cust;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: T2DL_DAS_USL.sales_cust_fact
Prior Table Layer: T2DL_DAS_USL.sales_fact
Team/Owner: Customer Analytics
Date Created/Modified: Mar 25 2024

Note:
-- Purpose of the table: Table to directly get transaction related metrics, and some customer attributes
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
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'sales_cust_fact', OUT_RETURN_MSG);

CREATE MULTISET TABLE {usl_t2_schema}.sales_cust_fact
	, FALLBACK
	, NO BEFORE JOURNAL
	, NO AFTER JOURNAL
	, CHECKSUM = DEFAULT 
	, DEFAULT MERGEBLOCKRATIO
	(
		sale_date DATE
		, week_num INTEGER
		, month_num INTEGER
		, quarter_num INTEGER
		, year_num INTEGER
		, global_tran_id BIGINT
		, line_item_seq_num SMALLINT
		, store_num INTEGER
		, acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, sku_num VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
		, upc_num VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
		, trip_id VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC compress
		, employee_discount_flag CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC compress
		, transaction_type_id INTEGER
		, device_id INTEGER
		, ship_method_id INTEGER
		, price_type_id INTEGER
		, line_net_usd_amt DECIMAL(12,2) compress
		, giftcard_flag BYTEINT compress
		, items DECIMAL(8,0) compress
		, returned_sales DECIMAL(12,2) compress
		, returned_items INTEGER compress
		, non_gc_amt DECIMAL(12,2) compress
		, region VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, dma VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, engagement_cohort VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, predicted_segment VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_level VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, loyalty_type VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, new_to_jwn INTEGER
		, channel VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, banner VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, business_unit_desc VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
		, dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
	)
PRIMARY INDEX(acp_id, sale_date);

SET QUERY_BAND = NONE FOR SESSION;