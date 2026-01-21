/* 
SQL script must begin with QUERY_BAND SETTINGS
*/
SET QUERY_BAND = 'App_ID=app08818;
     DAG_ID=ddl_sales_fact_11521_ACE_ENG;
     Task_Name=ddl_sales_fact;'
     FOR SESSION VOLATILE;

/*
T2/Table Name: t2dl_das_usl.sales_fact
Team/Owner: Customer Analytics - Styling & Strategy
Date Created/Modified: Jun. 27th 2023

Note:
-- Purpose of the table: Fact table to directly get transaction related metrics and attributes
-- Update Cadence: Daily

*/

-- Use drop_if_exists for testing DDL changes in development.  Hard code schema to 't2dl_das_bie_dev' to auto drop testing table.
-- Comment out prior to merging to production.
-- CALL SYS_MGMT.DROP_IF_EXISTS_SP ('t2dl_das_bie_dev', 'sales_fact', OUT_RETURN_MSG);

create multiset table {usl_t2_schema}.sales_fact
    ,fallback
    ,no before journal
    ,no after journal
    ,checksum = default
    ,default mergeblockratio
    (
     date_id DATE
    ,global_tran_id BIGINT
    ,line_item_seq_num SMALLINT
    ,store_number INTEGER
    ,acp_id VARCHAR(50) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,sku_id VARCHAR(10) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,upc_num VARCHAR(30) CHARACTER SET UNICODE NOT CASESPECIFIC
    ,sale_date DATE
    ,trip_id VARCHAR(60) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,employee_discount_flag CHAR(4) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,transaction_type_id INTEGER
    ,device_id INTEGER
    ,ship_method_id INTEGER
    ,price_type_id INTEGER
    ,line_item_activity_type_desc VARCHAR(20) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,tran_type_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,reversal_flag CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,line_item_quantity DECIMAL(8,0) compress
    ,line_item_net_amt_currency_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,original_line_item_amt_currency_code CHAR(8) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,original_line_item_amt DECIMAL(12,2) compress
    -- ,line_net_amt DECIMAL(12,2) compress
    ,line_net_usd_amt DECIMAL(12,2) compress
    ,merch_dept_ind CHAR(1) CHARACTER SET UNICODE NOT CASESPECIFIC compress
    ,non_gc_line_net_usd_amt DECIMAL(12,2) compress
    ,line_item_regular_price DECIMAL(12,2) compress
    ,units_returned INTEGER compress
    ,sales_returned DECIMAL(12,2) compress
    ,giftcard_flag BYTEINT compress
    ,merch_flag BYTEINT compress
    ,marketplace_flag BYTEINT compress
    ,dw_sys_load_tmstp  TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) NOT NULL
    )
primary index(global_tran_id ,line_item_seq_num);

-- Table Comment (STANDARD)
COMMENT ON {usl_t2_schema}.sales_fact IS 'Fact table to directly get transaction related metrics and attributes';

/* 
SQL script must end with statement to turn off QUERY_BAND 
*/
SET QUERY_BAND = NONE FOR SESSION;